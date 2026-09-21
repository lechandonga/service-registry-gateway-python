"""注册/注销/续约/租约过期与并发一致性。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.errors import ErrorType, GatewayError
from gateway.models import Instance, InstanceStatus
from gateway.registry import ServiceRegistry

from .conftest import log_states


async def test_register_duplicate_and_deregister_distinguishable():
    reg = ServiceRegistry()
    await reg.register(Instance(id="a", host="h", port=1))

    with pytest.raises(GatewayError) as ei:
        await reg.register(Instance(id="a", host="h", port=2))
    assert ei.value.reason is ErrorType.REGISTRY_DUPLICATE
    assert ei.value.detail["current_status"] == "online"

    assert await reg.deregister("a") is True
    with pytest.raises(GatewayError) as ei:
        await reg.deregister("a")
    assert ei.value.reason is ErrorType.REGISTRY_NOT_FOUND
    # 注销后允许重新注册
    assert await reg.register(Instance(id="a", host="h", port=3)) is True


async def test_heartbeat_unknown_is_not_found():
    reg = ServiceRegistry()
    with pytest.raises(GatewayError) as ei:
        await reg.heartbeat("ghost")
    assert ei.value.reason is ErrorType.REGISTRY_NOT_FOUND
    assert ei.value.detail["op"] == "heartbeat"


async def test_lease_expiry_excludes_from_selection(clock, bundle):
    reg = bundle["registry"]
    from gateway.models import RouteRule

    bundle["router"].add_rule(RouteRule(name="p", path_prefix="/proxy/"))
    await reg.register(Instance(id="a", host="h", port=1, lease_seconds=5))

    snap = await reg.snapshot()
    assert {i.id for i in snap} == {"a"}

    clock.advance(6)
    snap = await reg.snapshot()
    assert snap[0].status is InstanceStatus.EXPIRED
    await log_states(bundle, "lease expired -> must not be selectable")
    with pytest.raises(GatewayError) as ei:
        await bundle["router"].select(_req("/proxy/x"))
    assert ei.value.reason is ErrorType.ROUTE_NO_AVAILABLE
    assert ei.value.detail["candidates"][0]["reason"] == "lease_expired"

    # 过期 id 可以重新注册
    assert (
        await reg.register(Instance(id="a", host="h", port=1, lease_seconds=5)) is True
    )


async def test_heartbeat_renews_lease(clock):
    reg = ServiceRegistry(clock=clock)
    await reg.register(Instance(id="a", host="h", port=1, lease_seconds=5))
    clock.advance(4)
    await reg.heartbeat("a")
    clock.advance(4)  # 距上次心跳仅4秒，未过期
    snap = await reg.snapshot()
    assert snap[0].status is InstanceStatus.ONLINE
    clock.advance(2)  # 累计6秒 -> 过期
    snap = await reg.snapshot()
    assert snap[0].status is InstanceStatus.EXPIRED


async def test_concurrent_register_and_deregister_no_half_state():
    """并发注册/注销：每个 id 最终状态唯一且确定，不出现重复扣减/漏减。"""
    reg = ServiceRegistry()
    n = 50

    async def worker(i):
        iid = f"u{i % 10}"
        try:
            await reg.register(Instance(id=iid, host="h", port=i))
        except GatewayError as e:
            assert e.reason is ErrorType.REGISTRY_DUPLICATE
        await asyncio.sleep(0)
        try:
            await reg.deregister(iid)
        except GatewayError as e:
            assert e.reason is ErrorType.REGISTRY_NOT_FOUND

    await asyncio.gather(*(worker(i) for i in range(n)))
    snap = await reg.snapshot()
    for inst in snap:
        assert inst.status in (InstanceStatus.DEREGISTERED, InstanceStatus.ONLINE)
    # 再来一轮全量注销，每个 id 至多成功一次
    removed = 0
    for i in range(10):
        try:
            await reg.deregister(f"u{i}")
            removed += 1
        except GatewayError as e:
            assert e.reason is ErrorType.REGISTRY_NOT_FOUND
    online = [i for i in await reg.snapshot() if i.status is InstanceStatus.ONLINE]
    assert removed == len(online)


def _req(path, method="GET", headers=None):
    from gateway.models import Request

    return Request(path=path, method=method, headers=headers or {})
