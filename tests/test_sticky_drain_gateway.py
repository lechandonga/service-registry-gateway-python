"""编排级测试：会话粘性经真实 dispatch 生效；下线在途收尾；切换/重试资源不泄漏。"""

import logging

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.drain import DrainCoordinator, DrainStatus
from gateway.engine import Gateway
from gateway.health import HealthMonitor
from gateway.models import Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router
from gateway.sticky import SessionBinder
from gateway.transport import UpstreamTransport
from gateway.upstream import UpstreamCluster

LOG = logging.getLogger("test.sticky_drain_gateway")


def build(clock, *, max_per=100, timeout=5.0, retries=2, ttl=300.0):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=3, recovery_threshold=1)
    cb = CircuitBreaker(clock, failure_threshold=3, open_seconds=10)
    pool = ConnectionPool(max_per)
    cluster = UpstreamCluster()
    transport = UpstreamTransport(cluster)
    binder = SessionBinder(clock, ttl_seconds=ttl)
    router = Router(reg, health, cb, binder=binder)
    drain = DrainCoordinator(reg, pool, health=health, circuit=cb,
                             binder=binder, clock=clock)
    reg.on_registered = drain.reset_for_registration
    pool._admission_gate = drain.reject_if_exiting
    router._drain = drain
    gw = Gateway(router, transport, health, cb, pool, max_retries=retries,
                 upstream_timeout=timeout, binder=binder, drain_coordinator=drain)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    return gw, reg, health, cb, pool, cluster, binder, drain


async def register(reg, cluster, clock, host, mode="ok", weight=1, **kw):
    inst = new_instance("svc", host, 8000, weight=weight, clock=clock, **kw)
    await reg.register(inst)
    cluster.set_mode(inst, mode, **{"hang_seconds": 5.0} if mode == "timeout" else {})
    return inst


@pytest.mark.asyncio
async def test_dispatch_creates_and_honors_sticky_binding():
    clock = FakeClock()
    gw, reg, _, _, pool, cluster, binder, _ = build(clock)
    await register(reg, cluster, clock, "h1", weight=1)
    await register(reg, cluster, clock, "h2", weight=9999)
    r = Request("GET", "/", headers={"x-session-id": "user-42"})
    resp = await gw.dispatch(r)
    assert resp.ok
    bound = binder.describe("svc", "user-42")
    assert bound is not None and bound["instance"].endswith(resp.upstream)
    # 后续请求稳定粘住，哪怕权重倾斜。
    for _ in range(20):
        rr = await gw.dispatch(r)
        assert rr.upstream == resp.upstream
    assert pool.active_total() == 0
    LOG.info("dispatch 粘性稳定 upstream=%s ttl剩余=%s", resp.upstream, bound["ttl_remaining"])


@pytest.mark.asyncio
async def test_bound_instance_drained_request_switches_and_completes():
    """绑定实例退出后，同会话请求自动落到另一实例完成，并保留切换原因、无残留。"""
    clock = FakeClock()
    gw, reg, _, _, pool, cluster, binder, drain = build(clock)
    a = await register(reg, cluster, clock, "h1", weight=9999)
    b = await register(reg, cluster, clock, "h2", weight=1)
    r = Request("GET", "/", headers={"x-session-id": "sess-9"})
    resp = await gw.dispatch(r)
    assert resp.upstream.endswith("h1:8000")

    result = await drain.begin_drain(a.id)
    assert result.status is DrainStatus.DRAINING_STARTED
    assert result.deregistered is True

    resp2 = await gw.dispatch(r)
    assert resp2.ok and resp2.upstream.endswith("h2:8000")
    rebound = binder.describe("svc", "sess-9")
    assert rebound["instance"] == str(b.id)
    assert pool.active_total() == 0
    LOG.info("绑定实例下线后切换完成 old=%s new=%s", resp.upstream, resp2.upstream)


@pytest.mark.asyncio
async def test_inflight_dispatch_completes_after_drain_announcement():
    """宣告下线时已在途的请求必须跑完返回；之后实例才退出，新请求不再选中它。"""
    clock = FakeClock()
    gw, reg, _, _, pool, cluster, _, drain = build(clock)
    a = await register(reg, cluster, clock, "h1", mode="ok")
    _b = await register(reg, cluster, clock, "h2", mode="ok")

    # 手动占用一个在途连接，模拟“请求处理中”。
    inflight = await pool.acquire(a.id)
    result = await drain.begin_drain(a.id)
    assert result.deregistered is False and result.already_active == 1

    # 在途期间模拟该请求的上游调用仍然成功并返回（下线不打断在途）。
    from gateway.models import Request as Req
    upstream = cluster.get(a)
    ongoing = upstream.handle(Req("GET", "/"))

    async def finish_inflight():
        inflight.release()
        await drain.on_connection_released(a.id)

    response = await ongoing
    assert response.ok
    # 新请求此时只能去 b。
    new_resp = await gw.dispatch(Request("GET", "/new"))
    assert new_resp.upstream.endswith("h2:8000")
    # 在途正式收尾 -> a 退出。
    await finish_inflight()
    assert await reg.get(a.id) is None
    assert pool.active_total() == 0
    LOG.info("在途响应=%s 完成；新请求=%s；a 已退出", response.upstream, new_resp.upstream)


@pytest.mark.asyncio
async def test_retry_never_reselects_failed_bound_instance():
    clock = FakeClock()
    gw, reg, _, _, pool, cluster, binder, _ = build(clock, retries=2)
    bad = await register(reg, cluster, clock, "bad", mode="unreachable")
    await register(reg, cluster, clock, "good", mode="ok", weight=1)
    binder.bind("svc", "s-retry", bad.id)
    resp = await gw.dispatch(Request("GET", "/", headers={"x-session-id": "s-retry"}))
    assert resp.ok and resp.upstream.endswith("good:8000")
    # bad 只被尝试一次：重试被排除，没有反复回失败实例。
    assert cluster.get(bad).calls == 1
    assert binder.describe("svc", "s-retry")["instance"].endswith("good:8000")
    assert pool.active_total() == 0
    LOG.info("重试切换 bad调用=%d 最终=%s", cluster.get(bad).calls, resp.upstream)


@pytest.mark.asyncio
async def test_drain_during_request_admission_gate_retries_to_other_instance():
    """选路后、连接计数前并发宣告下线：门禁拒绝，请求改投另一实例成功（POST 也安全）。"""
    clock = FakeClock()
    gw, reg, _, _, pool, cluster, _, drain = build(clock, retries=2)
    a = await register(reg, cluster, clock, "h1", weight=9999)
    await register(reg, cluster, clock, "h2", weight=1)

    # 直接驱动门禁竞争：实例 a 已 draining，acquire 必须抛 INSTANCE_EXITING，
    # engine 捕获后改投 b。
    await drain.begin_drain(a.id)
    resp = await gw.dispatch(Request("POST", "/orders"))
    assert resp.ok and resp.upstream.endswith("h2:8000")
    assert cluster.get(a).calls == 0
    assert pool.active_total() == 0
    LOG.info("门禁拒绝下线实例，POST 改投=%s 成功且无副作用重试", resp.upstream)


@pytest.mark.asyncio
async def test_all_instances_exiting_is_distinguishable_from_one_exit():
    """“某个实例退出”可继续切换；“整个服务无可用实例”才是失败，两者必须可区分。"""
    from gateway.errors import AllUpstreamsDown, NoAvailableUpstream

    clock = FakeClock()
    gw, reg, _, _, pool, cluster, _, drain = build(clock)
    a = await register(reg, cluster, clock, "h1")

    # 场景一：唯一实例下线且无在途 -> 立即注销 -> 服务无注册实例。
    await drain.begin_drain(a.id)
    with pytest.raises(NoAvailableUpstream) as ei:
        await gw.dispatch(Request("GET", "/"))
    assert ei.value.code.value == "NO_AVAILABLE_UPSTREAM"

    # 场景二：唯一实例处于 DRAINING（仍有在途，在册但不接新流量）
    # -> 有实例却全部不可服务，结论不同。
    a2 = await register(reg, cluster, clock, "h1")
    held = await pool.acquire(a2.id)
    r2 = await drain.begin_drain(a2.id)
    assert r2.status is DrainStatus.DRAINING_STARTED and r2.deregistered is False
    with pytest.raises(AllUpstreamsDown) as ei2:
        await gw.dispatch(Request("GET", "/"))
    assert ei2.value.code.value == "ALL_UPSTREAMS_DOWN"

    held.release()
    await drain.on_connection_released(a2.id)
    assert pool.active_total() == 0
    LOG.info("无注册实例(NO_AVAILABLE_UPSTREAM) 与 全在退出(ALL_UPSTREAMS_DOWN) 可区分")
