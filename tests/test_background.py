"""后台主动健康扫描 + 租约扫描的生命周期与恢复链路（真实时钟）。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.factory import build_gateway
from gateway.models import Instance, InstanceStatus


@pytest.mark.asyncio
async def test_background_health_loop_recovers_evicted_instance():
    b = build_gateway(
        health_fail_threshold=2,
        health_recover_threshold=1,
        health_cooldown=0.05,
        probe_interval=0.02,
        upstream_delay=0,
        start_background=False,
    )
    reg, hc, pool = b["registry"], b["health"], b["upstreams"]
    await reg.register(Instance(id="a", host="h", port=1))

    # 被动摘除
    await hc.record_passive("a", False, "unreachable")
    await hc.record_passive("a", False, "unreachable")
    assert (await reg.get("a")).status is InstanceStatus.SICK

    # 上游恢复后启动后台扫描：冷却 + 一次成功探测即纳回
    pool.configure("a", "healthy")
    await hc.start()
    try:
        for _ in range(50):
            await asyncio.sleep(0.02)
            if (await reg.get("a")).status is InstanceStatus.ONLINE:
                break
        assert (await reg.get("a")).status is InstanceStatus.ONLINE
    finally:
        await hc.stop()


@pytest.mark.asyncio
async def test_app_lifespan_starts_and_stops_background_tasks():
    import httpx

    b = build_gateway(probe_interval=0.02, start_background=True)
    transport = httpx.ASGITransport(app=b["app"])
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        # 进入 lifespan 后后台任务应已在运行
        await client.post("/admin/instances", json={"id": "a", "host": "h", "port": 1})
        r = await client.get("/admin/status")
        assert r.status_code == 200
    # 退出 lifespan 后任务被回收，无残留 task
    assert b["registry"]._reaper is None
    assert b["health"]._task is None
