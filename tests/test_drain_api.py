"""HTTP 接口测试：平滑下线受理/重复/不存在可区分、续约拒绝、会话查询、状态观测。"""

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.api import create_app
from gateway.bootstrap import build_components

LOG = logging.getLogger("test.drain_api")


@pytest.fixture()
def client():
    comp = build_components()
    app = create_app(
        comp["registry"], comp["router"], comp["gateway"], comp["health"],
        comp["circuit"], comp["cluster"], clock=comp["clock"], logger=comp["logger"],
        drain=comp["drain"], binder=comp["binder"],
    )
    with TestClient(app) as c:
        yield c, comp


def _register(c, host, weight=1, lease=300):
    r = c.post("/admin/register", json={
        "service": "d", "host": host, "port": 1,
        "weight": weight, "lease_seconds": lease,
    })
    assert r.status_code == 200, r.text
    c.post("/admin/rules", json={"service": "d", "path_prefix": "/proxy/d"})


def test_drain_not_found_is_distinguishable(client):
    c, _ = client
    r = c.post("/admin/instances/d/nope/1/drain")
    assert r.status_code == 404
    assert r.json()["code"] == "INSTANCE_DRAIN_NOT_FOUND"
    LOG.info("下线不存在实例响应=%s", r.json())


def test_drain_success_then_drain_again_not_found(client):
    c, _ = client
    _register(c, "h1")
    r = c.post("/admin/instances/d/h1/1/drain")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "DRAINING_STARTED"
    assert body["deregistered"] is True and body["in_flight"] == 0
    # 已完成下线 -> 再次下线得到与成功可区分的 NOT_FOUND。
    r2 = c.post("/admin/instances/d/h1/1/drain")
    assert r2.status_code == 404
    assert r2.json()["code"] == "INSTANCE_DRAIN_NOT_FOUND"


def test_heartbeat_rejected_after_drain(client):
    c, _ = client
    _register(c, "h1")
    c.post("/admin/instances/d/h1/1/drain")
    r = c.post("/admin/instances/d/h1/1/heartbeat", json={})
    # 立即注销后实例未知 -> HEARTBEAT_UNKNOWN；两种拒绝结论都与成功续约可区分。
    assert r.status_code == 404
    assert r.json()["code"] == "HEARTBEAT_UNKNOWN"


def test_repeat_drain_while_draining_returns_distinguishable_409(client):
    """实例仍有在途、停留在 DRAINING 时重复宣告 -> ALREADY_DRAINING；HTTP 映射 409。"""
    import asyncio

    c, comp = client
    _register(c, "h1")

    async def drive():
        from gateway.models import InstanceId
        iid = InstanceId("d", "h1", 1)
        lease = await comp["pool"].acquire(iid)  # 占用一个在途，使其停留在 DRAINING
        try:
            first = await comp["drain"].begin_drain(iid)
            second = await comp["drain"].begin_drain(iid)
            return first, second
        finally:
            lease.release()
            await comp["drain"].on_connection_released(iid)

    first, second = asyncio.new_event_loop().run_until_complete(drive())
    assert first.status.value == "DRAINING_STARTED" and first.deregistered is False
    assert second.status.value == "ALREADY_DRAINING"
    # 与 HTTP handler 同一 begin_drain 实现；HTTP 层 409 映射单独断言。
    from gateway.api import _http_status
    from gateway.errors import InstanceAlreadyDraining
    assert _http_status(InstanceAlreadyDraining("x")) == 409
    LOG.info("下线中重复宣告可区分 INSTANCE_ALREADY_DRAINING -> 409")


def test_session_query_and_status_endpoint(client):
    c, _ = client
    _register(c, "h1", weight=9999)
    _register(c, "h2", weight=1)
    # 未绑定 -> bound=False。
    assert c.get("/admin/sessions/d/ghost").json()["bound"] is False
    # 形成粘性。
    r = c.get("/proxy/d/x", headers={"x-session-id": "abc-123"})
    assert r.status_code == 200
    info = c.get("/admin/sessions/d/abc-123").json()
    assert info["bound"] is True
    assert info["binding"]["instance"].startswith("d://")
    assert info["binding"]["ttl_remaining"] > 290
    # 服务状态中含会话列表与 drain 字段。
    status = c.get("/admin/status/d").json()
    assert "sessions" in status
    assert all("drain" in row for row in status["instances"])
    sessions = {s["session_id"]: s for s in status["sessions"]}
    assert sessions["abc-123"]["instance"] == info["binding"]["instance"]
    LOG.info("会话查询=%s 状态会话列表=%s", info["binding"], sessions)


def test_invalid_session_header_does_not_error(client):
    c, _ = client
    _register(c, "h1")
    r = c.get("/proxy/d/x", headers={"x-session-id": "bad value!!"})
    assert r.status_code == 200
    assert c.get("/admin/sessions/d/bad value!!").json()["bound"] is False


def test_new_traffic_avoids_draining_instance(client):
    c, _comp = client
    _register(c, "h1", weight=9999)
    _register(c, "h2", weight=1)
    # 无在途下线 h1（立即退出），随后新请求只能成功落在 h2，绝不选到已退出实例。
    c.post("/admin/instances/d/h1/1/drain")
    for _ in range(10):
        r = c.get("/proxy/d/x")
        assert r.status_code == 200 and "h2:1" in r.text
    LOG.info("h1 下线后新请求全部落在 h2")


def test_heartbeat_during_active_drain_rejected_over_http(client):
    """实例仍有在途（停留在 DRAINING）时，通过真实 HTTP 续约必须拿到 409 INSTANCE_DRAINING。"""
    c, comp = client
    _register(c, "h9")
    from gateway.models import InstanceId
    iid = InstanceId("d", "h9", 1)
    holder = {}

    async def setup():
        holder["lease"] = await comp["pool"].acquire(iid)

    async def teardown():
        holder["lease"].release()
        await comp["drain"].on_connection_released(iid)

    c.portal.call(setup)
    try:
        r = c.post("/admin/instances/d/h9/1/drain")
        assert r.status_code == 200 and r.json()["status"] == "DRAINING_STARTED"
        hb = c.post("/admin/instances/d/h9/1/heartbeat", json={})
        assert hb.status_code == 409 and hb.json()["code"] == "INSTANCE_DRAINING"
        LOG.info("下线过程中续约 HTTP 拒绝=%s", hb.json())
    finally:
        c.portal.call(teardown)
