"""并发专项：下线/摘除/选路同时进行时不选到已退出实例；连接计数与会话绑定无残留。"""

import asyncio
import logging

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.drain import DrainCoordinator
from gateway.engine import Gateway
from gateway.errors import GatewayError
from gateway.health import HealthMonitor
from gateway.models import Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router
from gateway.sticky import SessionBinder
from gateway.transport import UpstreamTransport
from gateway.upstream import UpstreamCluster

LOG = logging.getLogger("test.sticky_drain_concurrency")


@pytest.mark.asyncio
async def test_concurrent_drain_and_select_never_picks_exiting():
    """大量实例注册->带在途下线->在途释放完成，与并发选路交织：绝不选中已退出实例。"""
    clock = FakeClock()
    reg = Registry(clock)
    health = HealthMonitor(clock)
    cb = CircuitBreaker(clock)
    pool = ConnectionPool(max_per_upstream=1000, max_total=100000)
    binder = SessionBinder(clock, ttl_seconds=1000)
    drain = DrainCoordinator(reg, pool, health=health, circuit=cb,
                             binder=binder, clock=clock)
    reg.on_registered = drain.reset_for_registration
    pool._admission_gate = drain.reject_if_exiting
    router = Router(reg, health, cb, binder=binder, drain_coordinator=drain)
    router.add_rule(RouteRule(service="svc"))

    n = 40
    instances = [new_instance("svc", f"h{i}", 8000 + i, clock=clock) for i in range(n)]
    for inst in instances:
        await reg.register(inst)
    # 保留两个常驻实例，确保服务始终有可用候选。
    keep = {instances[0].id, instances[1].id}

    selected_exited = 0
    total = 0

    async def drainer(inst):
        try:
            lease = await pool.acquire(inst.id)
        except Exception:  # noqa: BLE001 - 下线竞争时门禁/背压拒绝都属预期
            return
        result = await drain.begin_drain(inst.id)
        await asyncio.sleep(0)
        lease.release()
        await drain.on_connection_released(inst.id)
        return result

    async def selector():
        nonlocal selected_exited, total
        for _ in range(300):
            try:
                _, _, _, picked = await router.select_with_session(
                    Request("GET", "/", headers={"x-session-id": f"sess-{total % 8}"}),
                )
            except GatewayError:
                continue
            total += 1
            # 选出的实例绝不能处于 draining/drained，也不能已离开注册表。
            if drain.is_exiting(picked.id) or await reg.get(picked.id) is None:
                selected_exited += 1
                LOG.error("并发下选到已退出实例 %s", picked.id)

    drainers = [drainer(inst) for inst in instances if inst.id not in keep]
    await asyncio.gather(*(drainers), *(selector() for _ in range(8)))
    LOG.info("并发选路总命中=%d 选到已退出=%d", total, selected_exited)
    assert selected_exited == 0
    assert total > 0
    # 常驻实例仍可服务；被下线实例已全部离开注册表。
    remaining = {i.id for i in await reg.instances("svc")}
    assert keep <= remaining
    assert all(inst.id not in remaining for inst in instances if inst.id not in keep)


@pytest.mark.asyncio
async def test_concurrent_dispatch_and_drain_counts_and_bindings_clean():
    """经真实 dispatch 的流量与下线交织：连接计数归零，退出实例的绑定被清理。"""
    clock = FakeClock()
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=100)
    cb = CircuitBreaker(clock, failure_threshold=100)
    pool = ConnectionPool(max_per_upstream=1000, max_total=100000)
    cluster = UpstreamCluster()
    binder = SessionBinder(clock, ttl_seconds=1000)
    transport = UpstreamTransport(cluster)
    router = Router(reg, health, cb, binder=binder)
    drain = DrainCoordinator(reg, pool, health=health, circuit=cb,
                             binder=binder, clock=clock)
    reg.on_registered = drain.reset_for_registration
    pool._admission_gate = drain.reject_if_exiting
    router._drain = drain
    gw = Gateway(router, transport, health, cb, pool, max_retries=3,
                 upstream_timeout=5, binder=binder, drain_coordinator=drain)
    router.add_rule(RouteRule(service="svc"))

    # 多个实例，全部正常；额外保留两个常驻。
    hosts = [f"h{i}" for i in range(12)]
    insts = []
    for h in hosts:
        inst = new_instance("svc", h, 1, weight=1, lease_seconds=1000, clock=clock)
        await reg.register(inst)
        cluster.add(inst)
        insts.append(inst)
    keep = {insts[0].id, insts[1].id}

    async def traffic():
        for i in range(400):
            sid = f"sess-{i % 16}"
            try:
                await gw.dispatch(Request("GET", "/", headers={"x-session-id": sid}))
            except GatewayError:
                pass
            if i % 7 == 0:
                await asyncio.sleep(0)

    async def drainer():
        for inst in insts:
            if inst.id in keep:
                continue
            await drain.begin_drain(inst.id)
            await asyncio.sleep(0)

    await asyncio.gather(traffic(), drainer())

    # 全局与每实例连接计数必须归零，无泄漏/负数/残留。
    assert pool.active_total() == 0
    for inst in insts:
        assert pool.active_of(inst.id) == 0
    # 仍有效的绑定只能指向常驻（未退出）实例，不存在指向已退出实例的陈旧绑定。
    for b in binder.bindings_of_service("svc"):
        assert b.instance_id in keep, f"陈旧绑定指向已退出实例 {b.instance_id}"
    # 已退出实例在绑定表中确实查不到（unbind_instance 已清理）。
    for inst in insts:
        if inst.id in keep:
            continue
        for sid in [f"sess-{i}" for i in range(16)]:
            info = binder.describe("svc", sid)
            assert info is None or info["instance"] != str(inst.id)
    LOG.info("并发下线+选路：连接残留=%d，有效绑定=%s",
             pool.active_total(),
             [(b.session_id, str(b.instance_id)) for b in binder.bindings_of_service("svc")])
