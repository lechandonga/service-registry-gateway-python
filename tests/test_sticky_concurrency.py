"""并发专项：下线/摘除/选路并发进行时，不选到已退出实例；连接计数与会话绑定无残留。"""

import asyncio
import logging
import random

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.drain import DrainManager
from gateway.engine import Gateway
from gateway.health import HealthMonitor
from gateway.models import InstanceId, Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router
from gateway.session import SessionStore
from gateway.transport import UpstreamTransport
from gateway.upstream import UpstreamCluster

LOG = logging.getLogger("test.sticky_concurrency")


def build(clock):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=3, recovery_threshold=1)
    cb = CircuitBreaker(clock)
    pool = ConnectionPool(max_per_upstream=64, max_total=None)
    cluster = UpstreamCluster()
    sessions = SessionStore(clock)
    drain = DrainManager(reg, pool, clock)

    async def finalize(iid):
        await reg.deregister(iid)
        health.remove(iid)
        cb.remove(iid)
        pool.remove(iid)
        sessions.unbind_instance(iid)

    drain.set_finalizer(finalize)
    reg.set_retirement_guard(drain.is_draining)
    router = Router(reg, health, cb, session_store=sessions, drain=drain)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    gw = Gateway(router, UpstreamTransport(cluster), health, cb, pool,
                 drain=drain, upstream_timeout=5.0)
    return reg, health, pool, sessions, drain, router, gw, cluster


@pytest.mark.asyncio
async def test_dispatch_never_lands_on_drained_instance_and_no_residue():
    """选路与平滑下线并发：已退出实例零命中；结束后计数与绑定无残留。"""
    clock = FakeClock()
    reg, health, pool, sessions, drain, router, gw, cluster = build(clock)
    keep = new_instance("svc", "keep", 8000, weight=1, lease_seconds=60, clock=clock)
    await reg.register(keep)
    cluster.add(keep)
    victims = []
    for i in range(6):
        inst = new_instance("svc", f"v{i}", 8000, weight=10,
                            lease_seconds=60, clock=clock)
        await reg.register(inst)
        cluster.add(inst)
        victims.append(inst)

    landed_on_victim_after_drain = 0
    lock = asyncio.Lock()
    drained_ids: set[InstanceId] = set()

    async def drainer(inst):
        await asyncio.sleep(0)
        await drain.start_drain(inst.id)
        async with lock:
            drained_ids.add(inst.id)

    async def worker(n):
        nonlocal landed_on_victim_after_drain
        for i in range(60):
            session = f"s-{n}-{i % 5}"
            try:
                resp = await gw.dispatch(Request("GET", "/", {"x-session-id": session}))
            except Exception:
                continue
            async with lock:
                gone = set()
                for iid in drained_ids:
                    if resp.upstream == str(iid) and await reg.get(iid) is None:
                        gone.add(iid)
                landed_on_victim_after_drain += len(gone)
            await asyncio.sleep(0)

    await asyncio.gather(
        *(drainer(inst) for inst in victims),
        *(worker(n) for n in range(6)),
    )
    LOG.info("命中已退出实例次数=%d 残留连接=%d 残留绑定=%d",
             landed_on_victim_after_drain, pool.active_total(), len(sessions))
    assert landed_on_victim_after_drain == 0
    # 所有下线实例已退出注册表
    for inst in victims:
        assert await reg.get(inst.id) is None
        assert drain.state_of(inst.id).value == "DRAINED"
    # 连接计数无泄漏、无重复扣减
    assert pool.active_total() == 0
    for inst in victims:
        assert pool.active_of(inst.id) == 0
    # 指向已退出实例的会话绑定全部被清理，无陈旧记录
    for inst in victims:
        assert sessions.unbind_instance(inst.id) == 0
    # 服务仍有可用实例：带会话的请求照常成功
    resp = await gw.dispatch(Request("GET", "/", {"x-session-id": "s-final"}))
    assert resp.ok and resp.upstream == str(keep.id)


@pytest.mark.asyncio
async def test_concurrent_drain_same_instance_single_finalize():
    """并发对同一实例发起下线 + 并发释放：收尾只执行一次。"""
    clock = FakeClock()
    reg, health, pool, sessions, drain, router, gw, cluster = build(clock)
    inst = new_instance("svc", "solo", 8000, lease_seconds=60, clock=clock)
    await reg.register(inst)
    cluster.add(inst)

    leases = [await pool.acquire(inst.id) for _ in range(3)]
    results = await asyncio.gather(*(drain.start_drain(inst.id) for _ in range(8)))
    started = [r for r in results if r.code.value == "DRAIN_STARTED"]
    already = [r for r in results if r.code.value == "ALREADY_DRAINING"]
    assert len(started) == 1 and len(already) == 7

    finalized = 0

    async def release_and_check(lease):
        nonlocal finalized
        lease.release()
        if await drain.after_release(inst.id):
            finalized += 1

    await asyncio.gather(*(release_and_check(l) for l in leases))
    LOG.info("并发释放触发收尾次数=%d", finalized)
    assert finalized == 1  # 并发下收尾恰好一次
    assert await reg.get(inst.id) is None
    assert pool.active_total() == 0


@pytest.mark.asyncio
async def test_retry_never_repeats_failed_instance_under_churn():
    """实例故障与摘除并发：同一次请求的重试不反复落在已失败实例上。"""
    clock = FakeClock()
    reg, health, pool, sessions, drain, router, gw, cluster = build(clock)
    bad = new_instance("svc", "bad", 8000, weight=100, lease_seconds=60, clock=clock)
    good = new_instance("svc", "good", 8000, weight=1, lease_seconds=60, clock=clock)
    await reg.register(bad)
    await reg.register(good)
    cluster.add(bad)
    cluster.add(good)
    cluster.set_mode(bad, "unreachable")

    async def churn():
        for _ in range(20):
            await asyncio.sleep(0)
            health.record_failure(bad.id)

    async def caller():
        resp = await gw.dispatch(Request("GET", "/", {"x-session-id": "s-r"}))
        return resp

    results = await asyncio.gather(churn(), caller(), caller(), caller())
    resps = results[1:]
    assert all(r.ok for r in resps)
    assert all(r.upstream == str(good.id) for r in resps)
    assert pool.active_total() == 0
    LOG.info("并发抖动下响应落点=%s", {r.upstream for r in resps})
