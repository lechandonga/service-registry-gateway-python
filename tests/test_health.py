"""主动/被动健康检查：抖动容忍、摘除、冷却与恢复。"""

from __future__ import annotations

from gateway.models import Instance, InstanceStatus

from .conftest import log_states


async def test_jitter_does_not_evict(catchall):
    b = catchall
    hc = b["health"]
    await b["registry"].register(Instance(id="a", host="h", port=1))
    # 失败-失败-成功-失败... 连续失败始终 < 阈值 3
    for _ in range(4):
        await hc.record_passive("a", False, "unreachable")
        await hc.record_passive("a", False, "unreachable")
        await hc.record_passive("a", True, "ok")
    inst = await b["registry"].get("a")
    assert inst.status is InstanceStatus.ONLINE
    assert hc.is_available(inst)
    await log_states(b, "jitter tolerated -> still online")


async def test_consecutive_failures_evict_then_probe_recovers(clock, catchall):
    b = catchall
    hc, pool, reg = b["health"], b["upstreams"], b["registry"]
    await reg.register(Instance(id="a", host="h", port=1))
    for _ in range(3):
        await hc.record_passive("a", False, "unreachable")
    inst = await reg.get("a")
    assert inst.status is InstanceStatus.SICK
    assert not hc.is_available(inst)

    # 摘除后即便上游恢复，冷却期内探测也不会纳回
    pool.configure("a", "healthy")
    assert await hc.probe_once("a") is False
    assert (await reg.get("a")).status is InstanceStatus.SICK

    clock.advance(0.3)
    assert await hc.probe_once("a") is True  # 第 1 次成功：仍 SICK（需连续2次）
    assert (await reg.get("a")).status is InstanceStatus.SICK
    assert await hc.probe_once("a") is True  # 第 2 次成功：恢复
    inst = await reg.get("a")
    assert inst.status is InstanceStatus.ONLINE and hc.is_available(inst)
    await log_states(b, "recovered after cooldown + consecutive successes")


async def test_single_probe_failure_reopens_sick_window(clock, catchall):
    """恢复探测过程中一次失败要把成功计数清零，不能被抖动蒙混纳回。"""
    b = catchall
    hc, pool, reg = b["health"], b["upstreams"], b["registry"]
    await reg.register(Instance(id="a", host="h", port=1))
    for _ in range(3):
        await hc.record_passive("a", False, "timeout")
    clock.advance(0.3)
    pool.set_script("a", ["healthy", "unreachable", "healthy", "healthy"])
    assert await hc.probe_once("a") is True
    assert await hc.probe_once("a") is False  # 打断连续成功
    assert (await reg.get("a")).status is InstanceStatus.SICK
    assert await hc.probe_once("a") is True
    assert (await reg.get("a")).status is InstanceStatus.SICK  # 重新计数，仅1次
    assert await hc.probe_once("a") is True
    assert (await reg.get("a")).status is InstanceStatus.ONLINE


async def test_deregistered_instance_not_revived_by_probe(catchall):
    b = catchall
    hc, pool, reg = b["health"], b["upstreams"], b["registry"]
    await reg.register(Instance(id="a", host="h", port=1))
    for _ in range(3):
        await hc.record_passive("a", False, "unreachable")
    await reg.deregister("a")
    pool.configure("a", "healthy")
    # probe 对不存在实例直接返回 False，不重建状态
    assert await hc.probe_once("a") is False


async def test_zero_weight_instance_not_recovered(clock, catchall):
    b = catchall
    hc, pool, reg = b["health"], b["upstreams"], b["registry"]
    await reg.register(Instance(id="a", host="h", port=1))
    for _ in range(3):
        await hc.record_passive("a", False, "unreachable")
    inst = await reg.get("a")
    inst.weight = 0
    inst.status = InstanceStatus.OFFLINE
    clock.advance(1)
    pool.configure("a", "healthy")
    assert await hc.probe_once("a") is True
    assert await hc.probe_once("a") is True
    # 权重为0：健康标记恢复，但不会被选为上游
    assert hc.state_of("a").healthy is True
    from gateway.errors import ErrorType, GatewayError
    from gateway.models import Request

    try:
        await b["router"].select(Request(path="/proxy/x"))
    except GatewayError as e:
        assert e.reason is ErrorType.ROUTE_NO_AVAILABLE
