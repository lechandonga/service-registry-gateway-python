"""会话保持测试：粘性命中、滑动 TTL、非法标识降级、绑定实例退出自动切换并给出原因。"""

import logging
import random

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.health import HealthMonitor
from gateway.models import Request, RouteRule, new_instance
from gateway.registry import Registry
from gateway.router import Router
from gateway.sticky import SessionBinder

LOG = logging.getLogger("test.sticky")


def build(clock, *, ttl=100.0, failure_threshold=1):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=failure_threshold)
    cb = CircuitBreaker(clock)
    binder = SessionBinder(clock, ttl_seconds=ttl)
    router = Router(reg, health, cb, rng=random.Random(7), binder=binder)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    return reg, health, cb, binder, router


def req(session=None):
    headers = {"x-session-id": session} if session is not None else {}
    return Request("GET", "/", headers=headers)


@pytest.mark.asyncio
async def test_sticky_same_session_stable_despite_weight_skew():
    clock = FakeClock()
    reg, _, _, binder, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=1, clock=clock)
    b = new_instance("svc", "hb", 2, weight=9999, clock=clock)
    await reg.register(a)
    await reg.register(b)

    # 首次把会话绑定到权重极低的 a，之后同会话必须稳定落在 a。
    binder.bind("svc", "s-1", a.id)
    for _ in range(50):
        _, _, decision, picked = await router.select_with_session(req("s-1"))
        assert picked.id == a.id
        assert decision.action == "BOUND_HIT"
    LOG.info("粘性稳定命中=%s 动作=%s", a.id, decision.action)


@pytest.mark.asyncio
async def test_missing_or_invalid_session_falls_back_without_error():
    clock = FakeClock()
    reg, _, _, _, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=1, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, clock=clock)
    await reg.register(a)
    await reg.register(b)

    _, _, d_none, picked_none = await router.select_with_session(req(None))
    assert d_none.action == "NONE" and picked_none is not None
    for bad in ["", "   ", "has space", "中文会话", "a" * 200, "!@#$"]:
        _, _, d_bad, picked_bad = await router.select_with_session(
            Request("GET", "/", headers={"x-session-id": bad}),
        )
        # 非法标识绝不报错、绝不判无可用上游，而是加权选一个可用实例。
        assert d_bad.action == "INVALID_FALLBACK"
        assert d_bad.valid is False
        assert picked_bad is not None
    LOG.info("缺失/非法会话全部安全降级")


@pytest.mark.asyncio
async def test_binding_ttl_sliding_and_expiry_rebind():
    clock = FakeClock()
    reg, _, _, binder, router = build(clock, ttl=10.0)
    a = new_instance("svc", "ha", 1, weight=1, lease_seconds=1000, clock=clock)
    await reg.register(a)
    binder.bind("svc", "s-ttl", a.id)
    clock.advance(6)
    # 命中即滑动续期：再过 6 秒（累计 12s）仍有效。
    _, _, d_hit, _ = await router.select_with_session(req("s-ttl"))
    assert d_hit.action == "BOUND_HIT"
    assert binder.describe("svc", "s-ttl")["ttl_remaining"] == pytest.approx(10.0)
    clock.advance(11)
    _, _, d_exp, picked = await router.select_with_session(req("s-ttl"))
    assert d_exp.action == "EXPIRED_REBIND"
    assert d_exp.switch_reason == "STICKY_TTL_EXPIRED"
    assert picked.id == a.id
    LOG.info("滑动续期与过期重绑动作=%s", d_exp.action)


@pytest.mark.asyncio
async def test_bound_instance_health_down_switches_with_reason():
    clock = FakeClock()
    reg, health, _, binder, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=9999, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, clock=clock)
    await reg.register(a)
    await reg.register(b)
    binder.bind("svc", "s-down", a.id)
    health.record_failure(a.id)  # threshold=1 -> DOWN

    _, reasons, decision, picked = await router.select_with_session(req("s-down"))
    assert picked.id == b.id
    assert decision.action == "SWITCH"
    assert decision.switch_reason == "BOUND_INSTANCE_UNAVAILABLE"
    assert "摘除中" in decision.detail
    row = next(r for r in reasons if r["instance"] == str(a.id))
    assert row["selected_eligible"] is False
    LOG.info("绑定实例健康摘除切换=%s 原因=%s detail=%s",
             picked.id, decision.switch_reason, decision.detail)


@pytest.mark.asyncio
async def test_bound_instance_gone_switches_and_keeps_service_alive():
    clock = FakeClock()
    reg, _, _, binder, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=9999, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, clock=clock)
    await reg.register(a)
    await reg.register(b)
    binder.bind("svc", "s-gone", a.id)
    # 绑定实例物理消失（注销），但未清理绑定，模拟陈旧记录。
    await reg.deregister(a.id)

    _, _, decision, picked = await router.select_with_session(req("s-gone"))
    assert picked.id == b.id
    assert decision.action == "SWITCH"
    assert decision.switch_reason == "BOUND_INSTANCE_GONE"
    LOG.info("绑定实例消失自动切换=%s 原因=%s", picked.id, decision.switch_reason)


@pytest.mark.asyncio
async def test_bound_instance_circuit_open_switches():
    clock = FakeClock()
    reg, _, cb, binder, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=9999, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, clock=clock)
    await reg.register(a)
    await reg.register(b)
    binder.bind("svc", "s-cb", a.id)
    for _ in range(5):
        cb.record_failure(a.id)

    _, reasons, decision, picked = await router.select_with_session(req("s-cb"))
    assert picked.id == b.id
    assert decision.switch_reason == "BOUND_INSTANCE_UNAVAILABLE"
    assert any("熔断=OPEN" in r["reason"] for r in reasons if r["instance"] == str(a.id))
    LOG.info("熔断切换=%s 候选依据=%s", picked.id,
             [r["reason"] for r in reasons if r["instance"] == str(a.id)])


@pytest.mark.asyncio
async def test_bound_instance_lease_expiry_switches():
    clock = FakeClock()
    reg, _, _, binder, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=9999, lease_seconds=5, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, lease_seconds=50, clock=clock)
    await reg.register(a)
    await reg.register(b)
    binder.bind("svc", "s-exp", a.id)
    clock.advance(6)  # a 租约过期被读过滤

    _, _, decision, picked = await router.select_with_session(req("s-exp"))
    assert picked.id == b.id
    assert decision.switch_reason == "BOUND_INSTANCE_GONE"


@pytest.mark.asyncio
async def test_exclude_bound_failed_instance_retry_does_not_reselect_it():
    clock = FakeClock()
    reg, _, _, binder, router = build(clock)
    a = new_instance("svc", "ha", 1, weight=9999, clock=clock)
    b = new_instance("svc", "hb", 2, weight=1, clock=clock)
    await reg.register(a)
    await reg.register(b)
    binder.bind("svc", "s-retry", a.id)
    # a 已在本请求失败过：重试选路必须排除它。
    _, _, decision, picked = await router.select_with_session(
        req("s-retry"), exclude=frozenset({a.id}),
    )
    assert picked.id == b.id
    assert decision.switch_reason == "BOUND_INSTANCE_FAILED"
    LOG.info("重试排除已失败绑定实例 -> %s", picked.id)


@pytest.mark.asyncio
async def test_describe_reports_bound_instance_time_and_remaining_ttl():
    clock = FakeClock(1000.0)
    reg, _, _, binder, _ = build(clock, ttl=30.0)
    a = new_instance("svc", "ha", 1, clock=clock)
    await reg.register(a)
    binder.bind("svc", "s-view", a.id)
    info = binder.describe("svc", "s-view")
    assert info["instance"] == str(a.id)
    assert info["bound_at"] == 1000.0
    assert info["ttl_remaining"] == 30.0
    assert binder.describe("svc", "nope") is None
