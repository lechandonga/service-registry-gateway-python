"""平滑下线测试：受理结果可区分、续约被拒、在途跑完才退出、新流量不再选中、资源无残留。"""

import logging

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.drain import DrainCoordinator, DrainStatus
from gateway.errors import (
    HeartbeatDrainingRejected,
    HeartbeatUnknown,
    InstanceExiting,
)
from gateway.health import HealthMonitor
from gateway.models import InstanceId, Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router
from gateway.sticky import SessionBinder

LOG = logging.getLogger("test.drain")


def build(clock, *, max_per=100):
    reg = Registry(clock)
    health = HealthMonitor(clock)
    cb = CircuitBreaker(clock)
    pool = ConnectionPool(max_per_upstream=max_per)
    binder = SessionBinder(clock, ttl_seconds=100)
    drain = DrainCoordinator(reg, pool, health=health, circuit=cb,
                             binder=binder, clock=clock)
    pool._admission_gate = drain.reject_if_exiting
    router = Router(reg, health, cb, binder=binder, drain_coordinator=drain)
    router.add_rule(RouteRule(service="svc"))
    return reg, health, cb, pool, binder, drain, router


@pytest.mark.asyncio
async def test_begin_drain_distinguishable_results():
    clock = FakeClock()
    reg, _, _, _, _, drain, _ = build(clock)
    inst = new_instance("svc", "h1", 1, clock=clock)
    await reg.register(inst)
    ghost = InstanceId("svc", "ghost", 9)

    not_found = await drain.begin_drain(ghost)
    assert not_found.status is DrainStatus.NOT_FOUND
    assert not_found.deregistered is False

    started = await drain.begin_drain(inst.id)
    assert started.status is DrainStatus.DRAINING_STARTED
    assert started.deregistered is True  # 无在途，立即退出

    again = await drain.begin_drain(inst.id)
    assert again.status is DrainStatus.NOT_FOUND  # 已退出
    LOG.info("下线结果区分: not_found/started/again=%s/%s/%s",
             not_found.status, started.status, again.status)


@pytest.mark.asyncio
async def test_draining_instance_rejects_heartbeat_and_lease_expiry():
    clock = FakeClock()
    reg, _, _, pool, _, drain, _ = build(clock)
    inst = new_instance("svc", "h1", 1, lease_seconds=5, clock=clock)
    await reg.register(inst)
    lease = await pool.acquire(inst.id)  # 占一个在途，使其停留在 DRAINING
    result = await drain.begin_drain(inst.id)
    assert result.status is DrainStatus.DRAINING_STARTED and result.already_active == 1

    # 下线过程中续约被明确拒绝，不会复活。
    with pytest.raises(HeartbeatDrainingRejected) as ei:
        await reg.heartbeat(inst.id)
    assert ei.value.code.value == "INSTANCE_DRAINING"

    # 租约到期也不会被过期摘除。
    clock.advance(50)
    assert (await reg.expire_due()) == []
    assert await reg.get(inst.id) is not None
    assert drain.state_of(inst.id) == "DRAINING"

    # 在途完成后才真正退出。
    lease.release()
    assert await drain.on_connection_released(inst.id) is True
    assert await reg.get(inst.id) is None
    # 退出后续约得到“未知”结论，与下线中拒绝续约可区分。
    with pytest.raises(HeartbeatUnknown):
        await reg.heartbeat(inst.id)
    LOG.info("下线中续约拒绝 + 过期豁免 + 在途归零退出 全部成立")


@pytest.mark.asyncio
async def test_draining_instance_not_selected_and_admission_rejected():
    clock = FakeClock()
    reg, _, _, pool, _, drain, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=9999, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, clock=clock)
    await reg.register(a)
    await reg.register(b)
    lease = await pool.acquire(a.id)
    await drain.begin_drain(a.id)

    # 路由候选不再含 a；即使权重极端倾斜也只选 b。
    for _ in range(50):
        _, reasons, picked = await router.select(Request("GET", "/"))
        assert picked.id == b.id
    assert all(
        r["selected_eligible"] is False and "DRAINING" in r["reason"]
        for r in reasons if r["instance"] == str(a.id)
    )
    # 连接池门禁同样拒绝（关闭选路与下线之间的并发窗口）。
    with pytest.raises(InstanceExiting) as ei:
        await pool.acquire(a.id)
    assert ei.value.code.value == "INSTANCE_EXITING"

    lease.release()
    await drain.on_connection_released(a.id)
    LOG.info("下线实例不接新流量，新请求全部落在=%s", b.id)


@pytest.mark.asyncio
async def test_inflight_request_runs_to_completion_then_instance_exits():
    """核心：宣告下线时已在处理的请求必须跑完，计数归零后才注销。"""
    clock = FakeClock()
    reg, _, _, pool, binder, drain, _ = build(clock)
    inst = new_instance("svc", "h1", 1, clock=clock)
    await reg.register(inst)
    binder.bind("svc", "s1", inst.id)

    # 两个在途请求。
    lease1 = await pool.acquire(inst.id)
    lease2 = await pool.acquire(inst.id)
    result = await drain.begin_drain(inst.id)
    assert result.already_active == 2 and result.deregistered is False
    assert await reg.get(inst.id) is not None  # 仍在册，在途可继续

    # 第一个在途完成：仍不退出。
    lease1.release()
    assert await drain.on_connection_released(inst.id) is False
    assert pool.active_of(inst.id) == 1
    assert await reg.get(inst.id) is not None

    # 第二个完成：归零，真正退出；连接计数与会话绑定无残留。
    lease2.release()
    assert await drain.on_connection_released(inst.id) is True
    assert await reg.get(inst.id) is None
    assert pool.active_of(inst.id) == 0
    assert pool.active_total() == 0
    assert binder.describe("svc", "s1") is None
    LOG.info("在途全部跑完才退出，连接残留=%d 绑定残留=%s",
             pool.active_total(), binder.describe("svc", "s1"))


@pytest.mark.asyncio
async def test_repeat_drain_while_draining_is_distinguishable_and_idempotent():
    clock = FakeClock()
    reg, _, _, pool, _, drain, _ = build(clock)
    inst = new_instance("svc", "h1", 1, clock=clock)
    await reg.register(inst)
    lease = await pool.acquire(inst.id)
    first = await drain.begin_drain(inst.id)
    second = await drain.begin_drain(inst.id)
    assert first.status is DrainStatus.DRAINING_STARTED
    assert second.status is DrainStatus.ALREADY_DRAINING
    assert second.already_active == 1
    # 幂等：没有重复计数/状态扰动，在途仍为 1。
    assert pool.active_of(inst.id) == 1
    lease.release()
    await drain.on_connection_released(inst.id)
    LOG.info("重复宣告可区分且幂恒=%s", second.status)


@pytest.mark.asyncio
async def test_connection_counts_no_leak_through_drain_lifecycle():
    clock = FakeClock()
    reg, _, _, pool, _, drain, _ = build(clock)
    inst = new_instance("svc", "h1", 1, clock=clock)
    await reg.register(inst)
    leases = [await pool.acquire(inst.id) for _ in range(5)]
    await drain.begin_drain(inst.id)
    for l in leases:
        l.release()
    await drain.on_connection_released(inst.id)
    assert pool.active_total() == 0
    assert pool.active_of(inst.id) == 0
