"""本地演示装配：注册模拟上游、路由规则与后台租约/探测定时器。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import FastAPI

from .api import create_app
from .circuit import CircuitBreaker
from .clock import SystemClock
from .drain import DrainCoordinator
from .health import HealthMonitor, HealthProber
from .models import Instance, RouteRule, new_instance
from .pool import ConnectionPool
from .registry import Registry
from .router import Router
from .sticky import SessionBinder
from .transport import UpstreamTransport
from .upstream import UpstreamCluster, MockUpstream
from .engine import Gateway
from .logging_utils import configure_logging


class _ClusterProber(HealthProber):
    """主动探测：通过本地模拟上游 handle 一个轻量 GET 判断健康。"""

    def __init__(self, cluster: UpstreamCluster) -> None:
        self._cluster = cluster

    async def probe(self, instance: Instance) -> bool:
        from .models import Request

        up = self._cluster.get(instance)
        try:
            resp = await asyncio.wait_for(
                up.handle(Request("HEAD", "/__health")), timeout=1.0,
            )
        except Exception:
            return False
        return resp.ok


def build_components(clock=None):
    clock = clock or SystemClock()
    logger = configure_logging()
    registry = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=3, recovery_threshold=2)
    circuit = CircuitBreaker(clock, failure_threshold=5, open_seconds=5.0)
    pool = ConnectionPool(max_per_upstream=8, max_total=64)
    cluster = UpstreamCluster()
    transport = UpstreamTransport(cluster)
    binder = SessionBinder(clock, ttl_seconds=300.0, logger=logger)
    router = Router(registry, health, circuit, binder=binder)
    drain = DrainCoordinator(
        registry, pool, health=health, circuit=circuit, binder=binder,
        clock=clock, logger=logger,
    )
    # 同地址实例重新注册时清除旧的下线墓碑。
    registry.on_registered = drain.reset_for_registration
    # 连接池门禁：在池锁内、计数提交前拒绝正在退出的实例。
    pool._admission_gate = drain.reject_if_exiting
    # 路由层同步感知下线状态（标记集合），无需 await。
    router._drain = drain
    gateway = Gateway(
        router, transport, health, circuit, pool,
        max_retries=2, upstream_timeout=3.0, binder=binder,
        drain_coordinator=drain, logger=logger,
    )
    return {
        "clock": clock,
        "logger": logger,
        "registry": registry,
        "health": health,
        "circuit": circuit,
        "pool": pool,
        "cluster": cluster,
        "transport": transport,
        "router": router,
        "binder": binder,
        "drain": drain,
        "gateway": gateway,
        "prober": _ClusterProber(cluster),
    }


async def _seed(components: dict) -> None:
    """预置三个模拟实例与两条路由规则，便于本地验证。"""
    reg = components["registry"]
    cluster = components["cluster"]
    clock = components["clock"]
    router = components["router"]

    specs = [
        ("users", "127.0.0.1", 9001, 3, "ok"),
        ("users", "127.0.0.1", 9002, 1, "ok"),
        ("orders", "127.0.0.1", 9101, 1, "ok"),
    ]
    for service, host, port, weight, mode in specs:
        inst = new_instance(
            service, host, port, weight=weight, lease_seconds=15.0, clock=clock,
        )
        await reg.register(inst)
        cluster.add(inst, MockUpstream(name=str(inst), mode=mode))

    router.add_rule(RouteRule(service="users", path_prefix="/proxy/users"))
    router.add_rule(
        RouteRule(
            service="orders",
            path_prefix="/proxy/orders",
            require_headers={"x-canary": "true"},
            name="orders-canary",
        ),
    )


def build_local_gateway() -> FastAPI:
    components = build_components()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        await _seed(components)
        stop = asyncio.Event()
        logger = components["logger"]

        async def lease_loop() -> None:
            while not stop.is_set():
                expired = await components["registry"].expire_due()
                for iid in expired:
                    # 统一走协调器：连接计数、健康/熔断状态、会话绑定一并清理，无残留。
                    await components["drain"].cleanup_exited(iid)
                    logger.info("租约过期摘除 instance=%s", iid)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=1.0)

        async def binding_prune_loop() -> None:
            while not stop.is_set():
                removed = components["binder"].prune()
                if removed:
                    logger.info("会话绑定过期清理 条数=%d", removed)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=2.0)

        async def probe_loop() -> None:
            while not stop.is_set():
                snapshot = await components["registry"].snapshot()
                for iid, inst in snapshot.items():
                    if not components["health"].is_available(iid):
                        state = await components["health"].probe_once(
                            iid, components["prober"], inst,
                        )
                        logger.info("主动探测 instance=%s -> %s 依据=%s",
                                    iid, state, components["health"].explain(iid))
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=1.0)

        tasks = [
            asyncio.create_task(lease_loop()),
            asyncio.create_task(probe_loop()),
            asyncio.create_task(binding_prune_loop()),
        ]
        try:
            yield
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = create_app(
        components["registry"],
        components["router"],
        components["gateway"],
        components["health"],
        components["circuit"],
        components["cluster"],
        clock=components["clock"],
        logger=components["logger"],
        drain=components["drain"],
        binder=components["binder"],
    )
    app.router.lifespan_context = lifespan
    return app
