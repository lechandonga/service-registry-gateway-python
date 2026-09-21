"""路径/请求头匹配、加权分配、可区分的失败原因。"""

from __future__ import annotations

from gateway.errors import ErrorType, GatewayError
from gateway.models import Instance, InstanceStatus, Request, RouteRule

from .conftest import log_states


def _req(path, headers=None, method="GET"):
    return Request(path=path, headers=headers or {}, method=method)


async def test_no_rule_is_distinct_from_no_available(catchall):
    b = catchall
    # 有规则但无实例 -> ROUTE_NO_AVAILABLE
    with __import__("pytest").raises(GatewayError) as ei:
        await b["router"].select(_req("/proxy/a"))
    assert ei.value.reason is ErrorType.ROUTE_NO_AVAILABLE
    # 无任何规则匹配 -> ROUTE_NO_MATCH
    b["router"]._rules.clear()
    with __import__("pytest").raises(GatewayError) as ei:
        await b["router"].select(_req("/nope"))
    assert ei.value.reason is ErrorType.ROUTE_NO_MATCH
    assert ei.value.detail["path"] == "/nope"


async def test_priority_longest_prefix_and_header_match(catchall):
    b = catchall
    r = b["router"]
    r.add_rule(RouteRule(name="v2", path_prefix="/proxy/api/v2/", priority=10))
    r.add_rule(
        RouteRule(
            name="canary", path_prefix="/canary/", match_headers={"x-canary": "1"}
        )
    )
    await b["registry"].register(Instance(id="a", host="h", port=1))

    rule, _ = await r.select(_req("/proxy/api/v2/item"))
    assert rule.name == "v2"
    with __import__("pytest").raises(GatewayError) as ei:
        await r.select(_req("/canary/x"))
    assert ei.value.reason is ErrorType.ROUTE_NO_MATCH
    rule, _ = await r.select(_req("/canary/x", headers={"X-Canary": "1"}))
    assert rule.name == "canary"


async def test_zero_weight_and_offline_not_selected(catchall):
    b = catchall
    await b["registry"].register(Instance(id="a", host="h", port=1, weight=4))
    await b["registry"].register(Instance(id="z", host="h", port=2, weight=0))
    for _ in range(30):
        _, inst = await b["router"].select(_req("/proxy/x"))
        assert inst.id == "a"
    z = await b["registry"].get("z")
    assert z.status is InstanceStatus.OFFLINE


async def test_weighted_distribution_proportional(bundle):
    b = bundle
    r = b["router"]
    r.add_rule(RouteRule(name="p", path_prefix="/proxy/"))
    import random

    r._rng = random.Random(42)
    await b["registry"].register(Instance(id="w1", host="h", port=1, weight=1))
    await b["registry"].register(Instance(id="w3", host="h", port=2, weight=3))
    counts = {"w1": 0, "w3": 0}
    for _ in range(400):
        _, inst = await r.select(_req("/proxy/x"))
        counts[inst.id] += 1
    ratio = counts["w3"] / counts["w1"]
    assert 2.2 < ratio < 4.2, ratio  # 期望约 3:1


async def test_no_available_diagnoses_each_candidate(catchall):
    b = catchall
    await b["registry"].register(Instance(id="ok", host="h", port=1, weight=1))
    await b["registry"].register(Instance(id="zero", host="h", port=2, weight=0))
    inst_ok = await b["registry"].get("ok")
    inst_ok.status = InstanceStatus.SICK
    b["health"].state_of("ok").healthy = False
    b["health"].state_of("ok").last_reason = "unreachable"

    with __import__("pytest").raises(GatewayError) as ei:
        await b["router"].select(_req("/proxy/x"))
    assert ei.value.reason is ErrorType.ROUTE_NO_AVAILABLE
    reasons = {c["instance_id"]: c["reason"] for c in ei.value.detail["candidates"]}
    assert reasons["ok"].startswith("health_evicted:unreachable")
    assert reasons["zero"] == "zero_weight"
    await log_states(b, "all candidates ineligible, reasons must differ")
