"""并发专项：注册与摘除同时发生时不得选到已摘除实例；健康/连接计数不被污染。"""

import asyncio
import itertools
import logging

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.health import HealthMonitor
from gateway.models import InstanceId, Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router

LOG = logging.getLogger("test.concurrency")


@pytest.mark.asyncio
async def test_never_select_deregistered_instance():
    clock = FakeClock()
    reg = Registry(clock)
    health = HealthMonitor(clock)
    router = Router(reg, health, CircuitBreaker(clock))
    router.add_rule(RouteRule(service="svc"))

    counter = itertools.count()
    selected_but_gone = 0
    total_selections = 0

    async def register_and_remove(i):
        iid = InstanceId("svc", f"h{i}", 8000 + i)
        inst = new_instance("svc", f"h{i}", 8000 + i, clock=clock)
        await reg.register(inst)
        await asyncio.sleep(0)
        await reg.deregister(iid)
        health.remove(iid)

    async def selector():
        nonlocal selected_but_gone, total_selections
        for _ in range(200):
            try:
                _, _, picked = await router.select(Request("GET", "/"))
            except Exception:
                continue
            total_selections += 1
            # 选到的实例必须在注册表中仍然存在（快照一致性）。
            still = await reg.get(picked.id)
            if still is None:
                selected_but_gone += 1
                LOG.error("选到已摘除实例 %s", picked.id)

    await asyncio.gather(
        *(register_and_remove(i) for i in range(60)),
        *(selector() for _ in range(8)),
    )
    LOG.info("总选中=%d 选到已摘除=%d", total_selections, selected_but_gone)
    assert selected_but_gone == 0


@pytest.mark.asyncio
async def test_connection_counts_consistent_under_mixed_traffic():
    clock = FakeClock()
    pool = ConnectionPool(max_per_upstream=4, max_total=40)
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=2)
    iids = [new_instance("svc", f"h{i}", 1).id for i in range(10)]

    async def worker(iid, idx):
        for _ in range(30):
            try:
                lease = await pool.acquire(iid)
            except Exception:
                continue
            await asyncio.sleep(0)
            # 一半概率模拟失败反馈
            if idx % 2 == 0:
                health.record_failure(iid)
            else:
                health.record_success(iid)
            lease.release()

    await asyncio.gather(*(worker(iid, i) for i, iid in enumerate(iids)))
    for iid in iids:
        assert pool.active_of(iid) == 0
    assert pool.active_total() == 0
    LOG.info("混合流量后 per 计数全部归零 total=%d", pool.active_total())


@pytest.mark.asyncio
async def test_lease_expiry_concurrent_with_heartbeat():
    """续约与过期摘除竞争：过期后心跳失败，实例不会复活。"""
    clock = FakeClock()
    reg = Registry(clock)
    inst = new_instance("svc", "h", 1, lease_seconds=5, clock=clock)
    await reg.register(inst)
    clock.advance(6)

    async def expire():
        return await reg.expire_due()

    from gateway.errors import HeartbeatUnknown

    async def beat():
        try:
            await reg.heartbeat(inst.id)
            return True
        except HeartbeatUnknown:
            return False

    results = await asyncio.gather(expire(), beat())
    LOG.info("过期与续约竞争结果=%s", results)
    assert await reg.get(inst.id) is None
    assert len(await reg.instances("svc")) == 0
