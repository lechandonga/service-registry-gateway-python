"""注册表测试：租约过期、重复注册/注销区分、并发一致。"""

import asyncio

import pytest

from gateway.clock import FakeClock
from gateway.errors import (
    HeartbeatUnknown,
    InstanceNotFound,
    RegistrationConflict,
)
from gateway.models import InstanceId, new_instance
from gateway.registry import Registry


def make_inst(clock, service="svc", host="10.0.0.1", port=8000, **kw):
    return new_instance(service, host, port, clock=clock, **kw)


@pytest.mark.asyncio
async def test_register_and_duplicate_conflict():
    clock = FakeClock()
    reg = Registry(clock)
    inst = make_inst(clock)
    assert await reg.register(inst) is True
    with pytest.raises(RegistrationConflict) as ei:
        await reg.register(make_inst(clock))
    assert ei.value.code.value == "REG_DUPLICATE"
    assert len(await reg.instances("svc")) == 1


@pytest.mark.asyncio
async def test_deregister_and_duplicate_deregister_distinguishable():
    clock = FakeClock()
    reg = Registry(clock)
    inst = make_inst(clock)
    await reg.register(inst)
    removed = await reg.deregister(inst.id)
    assert removed.id == inst.id
    with pytest.raises(InstanceNotFound) as ei:
        await reg.deregister(inst.id)
    assert ei.value.code.value == "UNREG_NOT_FOUND"


@pytest.mark.asyncio
async def test_heartbeat_renews_lease():
    clock = FakeClock()
    reg = Registry(clock)
    inst = make_inst(clock, lease_seconds=10)
    await reg.register(inst)
    clock.advance(8)
    expires = await reg.heartbeat(inst.id)
    assert expires == pytest.approx(18.0)
    assert await reg.get(inst.id) is not None


@pytest.mark.asyncio
async def test_expired_instance_not_selectable_and_heartbeat_rejected():
    clock = FakeClock()
    reg = Registry(clock)
    inst = make_inst(clock, lease_seconds=5)
    await reg.register(inst)
    clock.advance(5)
    assert await reg.instances("svc") == []
    assert await reg.get(inst.id) is None
    with pytest.raises(HeartbeatUnknown) as ei:
        await reg.heartbeat(inst.id)
    assert ei.value.code.value == "HEARTBEAT_UNKNOWN"


@pytest.mark.asyncio
async def test_expire_due_removes_only_due():
    clock = FakeClock()
    reg = Registry(clock)
    a = make_inst(clock, host="h1", port=1, lease_seconds=3)
    b = make_inst(clock, host="h2", port=2, lease_seconds=10)
    await reg.register(a)
    await reg.register(b)
    clock.advance(4)
    removed = await reg.expire_due()
    assert removed == [a.id]
    assert [i.id for i in await reg.instances("svc")] == [b.id]


@pytest.mark.asyncio
async def test_concurrent_register_and_deregister_no_corruption():
    clock = FakeClock()
    reg = Registry(clock)
    inst = make_inst(clock)
    await reg.register(inst)

    async def churn(i):
        iid = InstanceId("svc", f"h{i}", 8000 + i)
        try:
            await reg.register(make_inst(clock, host=f"h{i}", port=8000 + i))
        except RegistrationConflict:
            pass
        await asyncio.sleep(0)
        try:
            await reg.deregister(iid)
        except InstanceNotFound:
            pass

    await asyncio.gather(*(churn(i) for i in range(50)))
    # 初始实例仍在，churn 的实例全部注销干净，无残留半状态。
    assert await reg.get(inst.id) is not None
    assert len(await reg.instances("svc")) == 1


@pytest.mark.asyncio
async def test_draining_instance_exempt_from_expiry_and_heartbeat_rejected():
    from gateway.errors import HeartbeatDrainingRejected, RegistrationConflict

    clock = FakeClock()
    reg = Registry(clock)
    inst = make_inst(clock, lease_seconds=5)
    await reg.register(inst)
    await reg.mark_draining(inst.id)
    # 下线中：续约被明确拒绝，不会被下线复活矛盾。
    with pytest.raises(HeartbeatDrainingRejected) as ei:
        await reg.heartbeat(inst.id)
    assert ei.value.code.value == "INSTANCE_DRAINING"
    # 租约到期也不被过期摘除，仍可读。
    clock.advance(50)
    assert await reg.expire_due() == []
    assert await reg.get(inst.id) is not None
    # 下线中不能重新注册。
    with pytest.raises(RegistrationConflict):
        await reg.register(make_inst(clock, lease_seconds=5))
