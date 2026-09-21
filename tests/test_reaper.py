"""后台租约扫描：无流量时主动过期。"""

from __future__ import annotations

import asyncio

from gateway.models import Instance, InstanceStatus


async def test_reaper_expires_without_traffic():
    from gateway.registry import ServiceRegistry

    reg = ServiceRegistry()  # 真实时钟，短租约
    await reg.register(Instance(id="a", host="h", port=1, lease_seconds=0.05))
    await reg.start_reaper(interval=0.02)
    try:
        await asyncio.sleep(0.15)
    finally:
        await reg.stop_reaper()
    inst = await reg.get("a")
    assert inst.status is InstanceStatus.EXPIRED
