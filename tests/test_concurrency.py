"""并发一致性：注册/摘除/续约/选路/调用同时进行。

不变量：
1. 选路返回的实例在返回那一刻必定可用（未注销、未过期、未被摘除）。
2. 摘注销与请求竞争时，不能把请求发到 DEREGISTERED 实例。
3. 连接计数最终归零，过程中不出现负值或超额。
"""

from __future__ import annotations

import asyncio

from gateway.errors import ErrorType, GatewayError
from gateway.models import Instance, InstanceStatus, Request

from .conftest import log_states


async def test_select_never_returns_deregistered(catchall):
    b = catchall
    reg, router = b["registry"], b["router"]
    await reg.register(Instance(id="u1", host="h", port=1, lease_seconds=1000))
    await reg.register(Instance(id="u2", host="h", port=2, lease_seconds=1000))

    stop = asyncio.Event()
    violations = []

    async def churner():
        i = 0
        while not stop.is_set():
            iid = f"u{1 + i % 2}"
            try:
                await reg.deregister(iid)
            except GatewayError:
                pass
            await asyncio.sleep(0)
            try:
                await reg.register(
                    Instance(id=iid, host="h", port=1, lease_seconds=1000)
                )
            except GatewayError:
                pass
            i += 1
            await asyncio.sleep(0)

    async def selector():
        for _ in range(2000):
            try:
                _rule, inst = await router.select(Request(path="/proxy/x"))
                # 复核：拿到的实例此刻绝不能是已注销
                fresh = await reg.get(inst.id)
                if fresh is None or fresh.status == InstanceStatus.DEREGISTERED:
                    violations.append(
                        (inst.id, None if fresh is None else fresh.status)
                    )
            except GatewayError as e:
                assert e.reason in (
                    ErrorType.ROUTE_NO_AVAILABLE,
                    ErrorType.ROUTE_NO_MATCH,
                )
            await asyncio.sleep(0)

    t1 = asyncio.create_task(churner())
    await asyncio.gather(selector())
    stop.set()
    await t1
    assert violations == [], violations
    await log_states(b, "concurrent churn + selection finished")


async def test_requests_during_deregister_do_not_stay_counted(catchall):
    b = catchall
    g, reg, lm = b["gateway"], b["registry"], b["limiter"]
    for i in range(6):
        await reg.register(Instance(id=f"u{i}", host="h", port=i, lease_seconds=1000))

    stop = asyncio.Event()

    async def dereg_loop():
        ids = [f"u{i}" for i in range(6)]
        k = 0
        while not stop.is_set():
            iid = ids[k % len(ids)]
            try:
                await reg.deregister(iid)
            except GatewayError:
                pass
            await asyncio.sleep(0)
            try:
                await reg.register(
                    Instance(id=iid, host="h", port=1, lease_seconds=1000)
                )
            except GatewayError:
                pass
            k += 1

    async def req_loop():
        for _ in range(500):
            try:
                resp = await g.send(Request(path="/proxy/x"))
                assert resp.status == 200
            except GatewayError as e:
                assert e.reason in (
                    ErrorType.ROUTE_NO_AVAILABLE,
                    ErrorType.CIRCUIT_OPEN,
                    ErrorType.UPSTREAM_UNREACHABLE,
                    ErrorType.UPSTREAM_TIMEOUT,
                    ErrorType.DNS_ERROR,
                    ErrorType.RETRIES_EXHAUSTED,
                )
            # 任何时刻全局计数不超上限
            assert lm.active_total() <= 100
            await asyncio.sleep(0)

    t = asyncio.create_task(dereg_loop())
    await req_loop()
    stop.set()
    await t
    assert lm.active_total() == 0
    for i in range(6):
        assert lm.active_for(f"u{i}") == 0


async def test_lease_expiry_under_concurrent_heartbeats(clock):
    """心跳与过期扫描竞争：续约成功的实例必须仍可用。"""
    from gateway.registry import ServiceRegistry

    reg = ServiceRegistry(clock=clock)
    await reg.register(Instance(id="u", host="h", port=1, lease_seconds=5))

    async def renew():
        for _ in range(10):
            clock.advance(1)
            try:
                await reg.heartbeat("u")
            except GatewayError as e:
                assert e.reason is ErrorType.REGISTRY_NOT_FOUND
            await asyncio.sleep(0)

    async def scan():
        for _ in range(10):
            await reg.snapshot()
            await asyncio.sleep(0)

    await asyncio.gather(renew(), scan())
    inst = await reg.get("u")
    assert inst.status is InstanceStatus.ONLINE
    clock.advance(6)  # 停止续约后才允许过期
    await reg.snapshot()
    assert (await reg.get("u")).status is InstanceStatus.EXPIRED
