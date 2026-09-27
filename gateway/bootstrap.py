"""本地演示装配：注册模拟上游、路由规则与后台租约/探测定时器。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import FastAPI

from .api import create_app
from .circuit import CircuitBreaker
from .clock import SystemClock
from .drain import DrainManager
from .health import HealthMonitor, HealthProber
from .models import Instance, RouteRule, new_instance
from .pool import ConnectionPool
from .registry import Registry
from .router import Router
from .session import SessionStore
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
    sessions = SessionStore(clock, ttl_seconds=300.0)
    drain = DrainManager(registry, pool, clock)
    router = Router(registry, health, circuit, session_store=sessions, drain=drain)
    gateway = Gateway(
        router, transport, health, circuit, pool,
        max_retries=2, upstream_timeout=3.0, drain=drain, logger=logger,
    )

    async def finalize_drain(iid) -> None:
        """平滑下线收尾：在途归零后注销实例并联动清理各组件状态。"""
        with contextlib.suppress(Exception):
            await registry.deregister(iid)
        health.remove(iid)
        circuit.remove(iid)
        pool.remove(iid)
        removed = sessions.unbind_instance(iid)
        logger.info(
            "平滑下线完成 instance=%s 清理会话绑定=%d", iid, removed,
        )

    drain.set_finalizer(finalize_drain)
    # 注册表租约保护：下线中的实例不被过期摘除、心跳被明确拒绝。
    registry.set_retirement_guard(drain.is_draining)
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
        "gateway": gateway,
        "sessions": sessions,
        "drain": drain,
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
                    components["health"].remove(iid)
                    components["circuit"].remove(iid)
                    components["pool"].remove(iid)
                    components["drain"].remove(iid)
                    cleared = components["sessions"].unbind_instance(iid)
                    logger.info("租约过期摘除 instance=%s 清理会话绑定=%d", iid, cleared)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=1.0)

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

        tasks = [asyncio.create_task(lease_loop()), asyncio.create_task(probe_loop())]
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
        sessions=components["sessions"],
        drain=components["drain"],
    )
    app.router.lifespan_context = lifespan
    return app
