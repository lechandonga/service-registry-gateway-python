"""平滑下线：发起结果可区分、新流量被拒、在途请求跑完才退出、续约/租约语义确定。"""

import asyncio
import random
import logging

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.drain import DrainManager, DrainResultCode, DrainState
from gateway.engine import Gateway
from gateway.errors import FailureCode, HeartbeatWhileDraining
from gateway.health import HealthMonitor
from gateway.models import InstanceId, Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router
from gateway.session import SessionStore
from gateway.transport import UpstreamTransport
from gateway.upstream import MockUpstream, UpstreamCluster

LOG = logging.getLogger("test.drain")


def build(clock, *, max_per=100):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=3, recovery_threshold=1)
    cb = CircuitBreaker(clock)
    pool = ConnectionPool(max_per, max_total=None)
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
    transport = UpstreamTransport(cluster)
    router = Router(reg, health, cb, rng=random.Random(1),
                    session_store=sessions, drain=drain)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    gw = Gateway(router, transport, health, cb, pool, drain=drain, upstream_timeout=5.0)
    return reg, health, cb, pool, sessions, drain, router, gw, cluster


async def register(reg, cluster, clock, host, mode="ok", weight=1, lease_seconds=30.0):
    inst = new_instance("svc", host, 8000, weight=weight,
                        lease_seconds=lease_seconds, clock=clock)
    await reg.register(inst)
    cluster.add(inst, MockUpstream(name=str(inst.id), mode=mode))
    return inst


# ---------- 发起结果可区分 ----------

@pytest.mark.asyncio
async def test_drain_start_results_distinguishable():
    clock = FakeClock()
    reg, _, _, _, _, drain, _, _, _ = build(clock)
    inst = await register(reg, UpstreamCluster(), clock, "h1")
    ghost = InstanceId("svc", "ghost", 9999)

    r1 = await drain.start_drain(ghost)          # 不存在
    r2 = await drain.start_drain(inst.id)         # 正常发起（无在途，直接完成）
    r3 = await drain.start_drain(ghost)           # 完成后实例已注销
    assert r1.code is DrainResultCode.NOT_FOUND and r1.accepted is False
    assert r2.code is DrainResultCode.DRAIN_COMPLETED and r2.accepted is True
    assert r3.code is DrainResultCode.NOT_FOUND
    LOG.info("下线结果=%s %s %s", r1.code.value, r2.code.value, r3.code.value)
    assert await reg.get(inst.id) is None  # 无在途即真正退出


@pytest.mark.asyncio
async def test_repeated_drain_while_draining_is_distinguishable():
    clock = FakeClock()
    reg, _, _, pool, _, drain, _, _, _ = build(clock)
    inst = await register(reg, UpstreamCluster(), clock, "h1")
    lease = await pool.acquire(inst.id)  # 模拟一个在途请求
    try:
        r1 = await drain.start_drain(inst.id)
        r2 = await drain.start_drain(inst.id)
        assert r1.code is DrainResultCode.DRAIN_STARTED and r1.in_flight == 1
        assert r2.code is DrainResultCode.ALREADY_DRAINING and r2.accepted is False
        # 下线中但在途未结束：实例仍在册，新流量选择层不可见
        assert await reg.get(inst.id) is not None
    finally:
        lease.release()
        await drain.after_release(inst.id)
    assert drain.state_of(inst.id) is DrainState.DRAINED
    assert await reg.get(inst.id) is None


# ---------- 新请求不再选中，但在途请求跑完 ----------

class _GatedUpstream(MockUpstream):
    """可阻塞的上游：进入 handle 置位 entered，释放 release 后才返回。"""

    def __init__(self, name):
        super().__init__(name=name)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def handle(self, request):
        self.entered.set()
        await self.release.wait()
        return await super().handle(request)


@pytest.mark.asyncio
async def test_inflight_request_completes_after_drain():
    clock = FakeClock()
    reg, _, _, pool, sessions, drain, router, gw, cluster = build(clock)
    draining = await register(reg, cluster, clock, "drain-h", weight=100)
    other = await register(reg, cluster, clock, "other-h", weight=1)

    # 先把一个会话粘到即将下线的实例（此时还是普通 ok 上游）
    resp = await gw.dispatch(Request("GET", "/", {"x-session-id": "s-1"}))
    assert resp.upstream == str(draining.id)

    # 换成可阻塞上游，制造一个"正在处理中"的在途请求
    cluster.add(draining, _GatedUpstream(name=str(draining.id)))
    gated = cluster.get(draining)

    inflight = asyncio.create_task(
        gw.dispatch(Request("GET", "/", {"x-session-id": "s-2"})),
    )
    await gated.entered.wait()  # 在途请求已进入下游、尚未返回
    result = await drain.start_drain(draining.id)
    assert result.code is DrainResultCode.DRAIN_STARTED
    assert pool.active_of(draining.id) == 1

    # 新请求（含原本粘性指向它的会话）不再落到下线实例
    _, _, picked = await router.select(Request("GET", "/"))
    assert picked.id == other.id
    _, _, picked_s = await router.select(Request("GET", "/", {"x-session-id": "s-1"}))
    assert picked_s.id == other.id  # 会话自动切换，不整体失败
    LOG.info("下线后新请求选路=%s", picked_s.id)

    new_resp = await gw.dispatch(Request("GET", "/", {"x-session-id": "s-3"}))
    assert new_resp.ok and new_resp.upstream == str(other.id)

    # 释放在途：它必须正常跑完返回，随后实例才真正退出
    gated.release.set()
    got = await inflight
    assert got.ok and got.upstream == str(draining.id)
    assert pool.active_of(draining.id) == 0
    assert await reg.get(draining.id) is None
    assert sessions.explain("s-2")["bound"] is False  # 旧绑定随实例退出被清理


@pytest.mark.asyncio
async def test_drain_completes_via_dispatch_release_path():
    """走真实 dispatch 的 finally 路径：请求结束即触发下线收尾，无需手动回调。"""
    clock = FakeClock()
    reg, _, _, _, _, drain, _, gw, cluster = build(clock)
    inst = await register(reg, cluster, clock, "h1", weight=100)
    await register(reg, cluster, clock, "h2", weight=1)
    await drain.start_drain(inst.id)  # 无在途 -> 已完成
    resp = await gw.dispatch(Request("GET", "/"))
    assert resp.ok and resp.upstream == str(InstanceId("svc", "h2", 8000))


# ---------- 下线中续约被拒、租约不过期摘除 ----------

@pytest.mark.asyncio
async def test_heartbeat_denied_and_lease_protected_while_draining():
    clock = FakeClock()
    reg, _, _, pool, _, drain, _, _, _ = build(clock)
    inst = await register(reg, UpstreamCluster(), clock, "h1", lease_seconds=5.0)
    lease = await pool.acquire(inst.id)
    try:
        await drain.start_drain(inst.id)
        with pytest.raises(HeartbeatWhileDraining) as ei:
            await reg.heartbeat(inst.id)
        assert ei.value.code is FailureCode.HEARTBEAT_DRAINING
        clock.advance(10)  # 租约已过期
        expired = await reg.expire_due()
        assert inst.id not in expired  # 下线中受保护，不被提前摘除
        # 仍然拒绝续约（不会因过期被续约复活，也不会被悄悄摘除）
        with pytest.raises(HeartbeatWhileDraining):
            await reg.heartbeat(inst.id)
        assert drain.state_of(inst.id) is DrainState.DRAINING
    finally:
        lease.release()
        await drain.after_release(inst.id)
    # 在途归零后由下线流程注销：实例真正退出
    assert drain.state_of(inst.id) is DrainState.DRAINED
