"""路由测试：规则匹配、三类失败可区分、权重0/下线过滤、加权分布。"""

import logging
from collections import Counter

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.errors import (
    AllUpstreamsDown,
    NoAvailableUpstream,
    NoRouteRule,
)
from gateway.health import HealthMonitor, HealthState
from gateway.models import Request, RouteRule, new_instance
from gateway.registry import Registry
from gateway.router import Router
import random

LOG = logging.getLogger("test.router")


async def _build(clock):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=1)
    cb = CircuitBreaker(clock)
    router = Router(reg, health, cb, rng=random.Random(42))
    return reg, health, cb, router


@pytest.mark.asyncio
async def test_no_rule_distinguishable():
    clock = FakeClock()
    _, _, _, router = await _build(clock)
    with pytest.raises(NoRouteRule) as ei:
        await router.select(Request("GET", "/nope"))
    assert ei.value.code.value == "NO_ROUTE_RULE"


@pytest.mark.asyncio
async def test_service_without_instance_distinguishable():
    clock = FakeClock()
    _, _, _, router = await _build(clock)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    with pytest.raises(NoAvailableUpstream) as ei:
        await router.select(Request("GET", "/"))
    assert ei.value.code.value == "NO_AVAILABLE_UPSTREAM"


@pytest.mark.asyncio
async def test_all_down_distinguishable_with_reasons():
    clock = FakeClock()
    reg, health, cb, router = await _build(clock)
    inst = new_instance("svc", "h", 1, clock=clock)
    await reg.register(inst)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    health.record_failure(inst.id)  # threshold=1 -> DOWN
    with pytest.raises(AllUpstreamsDown) as ei:
        await router.select(Request("GET", "/"))
    assert ei.value.code.value == "ALL_UPSTREAMS_DOWN"
    _, reasons = await router.candidates("svc")
    LOG.info("全不可用判定依据=%s", reasons)
    assert reasons[0]["selected_eligible"] is False
    assert "摘除中" in reasons[0]["reason"]


@pytest.mark.asyncio
async def test_header_and_path_matching():
    clock = FakeClock()
    reg, _, _, router = await _build(clock)
    inst = new("svc", clock)
    await reg.register(inst)
    router.add_rule(RouteRule(service="svc", path_prefix="/v1", require_headers={"x-v": "2"}))
    with pytest.raises(NoRouteRule):
        await router.select(Request("GET", "/v1/x"))
    _, _, picked = await router.select(Request("GET", "/v1/x", {"x-v": "2"}))
    assert picked.id == inst.id


@pytest.mark.asyncio
async def test_zero_weight_and_circuit_filtered():
    clock = FakeClock()
    reg, health, cb, router = await _build(clock)
    a = new_instance("svc", "ha", 1, weight=0, clock=clock)
    b = new_instance("svc", "hb", 2, weight=10, clock=clock)
    c = new_instance("svc", "hc", 3, weight=5, clock=clock)
    for i in (a, b, c):
        await reg.register(i)
    router.add_rule(RouteRule(service="svc"))
    # c 熔断打开 -> 仅剩 b 可选
    for _ in range(10):
        cb.record_failure(c.id)
    _, reasons, picked = await router.select(Request("GET", "/"))
    LOG.info("过滤判定=%s", [r["reason"] for r in reasons])
    assert picked.id == b.id
    eligibility = {r["instance"].split("://")[1]: r["selected_eligible"] for r in reasons}
    assert eligibility["ha:1"] is False
    assert eligibility["hb:2"] is True
    assert eligibility["hc:3"] is False


@pytest.mark.asyncio
async def test_weighted_distribution():
    clock = FakeClock()
    reg, _, _, router = await _build(clock)
    a = new_instance("svc", "ha", 1, weight=90, clock=clock)
    b = new_instance("svc", "hb", 2, weight=10, clock=clock)
    await reg.register(a)
    await reg.register(b)
    router.add_rule(RouteRule(service="svc"), )
    counter = Counter()
    for _ in range(1000):
        _, _, picked = await router.select(Request("GET", "/"))
        counter[picked.id.host] += 1
    LOG.info("加权分布=%s", counter)
    assert counter["ha"] > 800


def new(service, clock):
    return new_instance(service, "h", 1, clock=clock)
