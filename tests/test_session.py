"""会话粘性：绑定稳定落点、滑动有效期、切换原因、不合法标识回退加权。"""

import logging
import random

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.errors import AllUpstreamsDown, NoAvailableUpstream
from gateway.health import HealthMonitor
from gateway.models import Request, RouteRule, new_instance
from gateway.registry import Registry
from gateway.router import Router, SelectionContext
from gateway.session import SessionStore, normalize_session_key

LOG = logging.getLogger("test.session")


async def _build(clock, ttl=60.0):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=1)
    cb = CircuitBreaker(clock)
    sessions = SessionStore(clock, ttl_seconds=ttl)
    router = Router(reg, health, cb, rng=random.Random(7), session_store=sessions)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    return reg, health, cb, sessions, router


async def _add(reg, clock, host, weight=1, lease_seconds=10.0):
    inst = new_instance("svc", host, 8000, weight=weight, lease_seconds=lease_seconds, clock=clock)
    await reg.register(inst)
    return inst


def _req(session=None):
    headers = {"x-session-id": session} if session is not None else {}
    return Request("GET", "/", headers)


@pytest.mark.asyncio
async def test_same_session_sticks_to_same_instance():
    clock = FakeClock()
    reg, _, _, sessions, router = await _build(clock)
    await _add(reg, clock, "h1", weight=1)
    await _add(reg, clock, "h2", weight=1)
    await _add(reg, clock, "h3", weight=1)

    ctx = SelectionContext()
    _, _, first = await router.select(_req("s-1"), ctx=ctx)
    for _ in range(30):
        ctx2 = SelectionContext()
        _, _, picked = await router.select(_req("s-1"), ctx=ctx2)
        assert picked.id == first.id
        assert ctx2.sticky is True
    info = sessions.explain("s-1")
    LOG.info("会话绑定=%s", info)
    assert info["bound"] is True
    assert info["instance"] == str(first.id)
    assert info["bound_at"] == 0.0
    assert info["remaining_seconds"] > 0


@pytest.mark.asyncio
async def test_binding_expires_after_ttl_without_touch():
    clock = FakeClock()
    reg, _, _, sessions, router = await _build(clock, ttl=10.0)
    await _add(reg, clock, "h1", lease_seconds=100)
    await _add(reg, clock, "h2", lease_seconds=100)
    _, _, first = await router.select(_req("s-ttl"))
    clock.advance(11)  # 超过粘性有效期
    assert sessions.lookup("s-ttl") is None  # 过期即惰性清除
    # 重新选择：可能换实例，但必然重新建立绑定
    ctx = SelectionContext()
    await router.select(_req("s-ttl"), ctx=ctx)
    assert ctx.sticky is False
    assert sessions.explain("s-ttl")["bound"] is True


@pytest.mark.asyncio
async def test_invalid_or_missing_session_falls_back_to_weighted():
    clock = FakeClock()
    reg, _, _, sessions, router = await _build(clock)
    await _add(reg, clock, "h1", weight=0)  # 权重0必不被选中
    await _add(reg, clock, "h2", weight=5)
    assert normalize_session_key("") is None
    assert normalize_session_key("bad key!") is None
    assert normalize_session_key("x" * 200) is None
    assert normalize_session_key("ok-Key_1.2~3") is not None
    for bad in (None, "", "bad key!", "x" * 200):
        _, _, picked = await router.select(_req(bad))
        assert picked.id.host == "h2"  # 正常加权，不报错、不误判无可用上游
    assert len(sessions) == 0  # 不合法标识不产生绑定


@pytest.mark.asyncio
async def test_switch_when_bound_instance_health_down():
    clock = FakeClock()
    reg, health, _, sessions, router = await _build(clock)
    a = await _add(reg, clock, "h1", weight=100)
    b = await _add(reg, clock, "h2", weight=1)
    _, _, first = await router.select(_req("s-x"))
    assert first.id == a.id  # 高权重必中
    health.record_failure(a.id)  # threshold=1 -> DOWN 摘除
    ctx = SelectionContext()
    _, _, picked = await router.select(_req("s-x"), ctx=ctx)
    LOG.info("切换判定=%s notes=%s", ctx.switch_reason, ctx.notes)
    assert picked.id == b.id
    assert ctx.switched is True
    assert ctx.switch_reason == "bound_instance_unhealthy"
    # 绑定已改写到新实例，后续稳定落 h2
    assert sessions.explain("s-x")["instance"] == str(b.id)


@pytest.mark.asyncio
async def test_switch_when_bound_instance_circuit_open():
    clock = FakeClock()
    reg, _, cb, _, router = await _build(clock)
    a = await _add(reg, clock, "h1", weight=100)
    b = await _add(reg, clock, "h2", weight=1)
    await router.select(_req("s-c"))
    for _ in range(5):
        cb.record_failure(a.id)
    ctx = SelectionContext()
    _, _, picked = await router.select(_req("s-c"), ctx=ctx)
    assert picked.id == b.id
    assert ctx.switch_reason == "bound_instance_circuit_open"


@pytest.mark.asyncio
async def test_switch_when_bound_instance_gone_and_stale_binding_cleaned():
    clock = FakeClock()
    reg, _, _, sessions, router = await _build(clock)
    a = await _add(reg, clock, "h1", weight=100)
    b = await _add(reg, clock, "h2", weight=1)
    await router.select(_req("s-g"))
    await reg.deregister(a.id)  # 实例退出
    cleared = sessions.unbind_instance(a.id)  # 退出路径联动清理
    assert cleared == 1
    assert sessions.explain("s-g")["bound"] is False  # 不留陈旧绑定
    ctx = SelectionContext()
    _, _, picked = await router.select(_req("s-g"), ctx=ctx)
    assert picked.id == b.id  # 自动落到其他可用实例，不整体失败


@pytest.mark.asyncio
async def test_bound_instance_lease_expired_switches_with_reason():
    clock = FakeClock()
    reg, _, _, _, router = await _build(clock)
    a = await _add(reg, clock, "h1", weight=100)
    b = await _add(reg, clock, "h2", weight=1)
    await router.select(_req("s-l"))
    clock.advance(20)  # 两实例租约(默认10s)都过期 -> 重新注册 b，让 a 消失
    expired = await reg.expire_due()
    assert a.id in expired and b.id in expired
    inst_b = new_instance("svc", "h2", 8000, clock=clock)
    await reg.register(inst_b)
    ctx = SelectionContext()
    _, _, picked = await router.select(_req("s-l"), ctx=ctx)
    assert picked.id == inst_b.id
    assert ctx.switched is True
    assert ctx.switch_reason == "bound_instance_gone"


@pytest.mark.asyncio
async def test_no_available_vs_all_down_still_distinguishable_with_session():
    clock = FakeClock()
    reg, health, _, _, router = await _build(clock)
    with pytest.raises(NoAvailableUpstream):
        await router.select(_req("s-1"))
    inst = await _add(reg, clock, "h1")
    health.record_failure(inst.id)
    with pytest.raises(AllUpstreamsDown):
        await router.select(_req("s-1"))
