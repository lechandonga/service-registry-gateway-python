"""HTTP 接口集成：平滑下线端点、会话绑定查询、下线中续约拒绝、粘性代理。"""

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.api import create_app
from gateway.bootstrap import build_components, _seed

LOG = logging.getLogger("test.api_sticky_drain")


@pytest.fixture()
def client():
    comp = build_components()

    async def _startup():
        await _seed(comp)

    app = create_app(
        comp["registry"], comp["router"], comp["gateway"], comp["health"],
        comp["circuit"], comp["cluster"], clock=comp["clock"], logger=comp["logger"],
        on_startup=_startup, sessions=comp["sessions"], drain=comp["drain"],
    )
    with TestClient(app) as c:
        yield c, comp


def test_sticky_proxy_and_session_observable(client):
    c, _ = client
    r1 = c.get("/proxy/users/1", headers={"x-session-id": "sess-42"})
    assert r1.status_code == 200
    info = c.get("/admin/sessions/sess-42").json()
    LOG.info("会话绑定观测=%s", info)
    assert info["bound"] is True
    assert info["instance"].startswith("users://")
    assert info["remaining_seconds"] > 0
    # 同一会话后续请求稳定落同一实例
    for _ in range(5):
        r = c.get("/proxy/users/1", headers={"x-session-id": "sess-42"})
        assert r.status_code == 200
    assert c.get("/admin/sessions/sess-42").json()["instance"] == info["instance"]
    # 不合法会话标识：正常加权转发，不报错
    r_bad = c.get("/proxy/users/1", headers={"x-session-id": "bad key!"})
    assert r_bad.status_code == 200
    assert c.get("/admin/sessions/bad key!").json()["bound"] is False


def test_drain_endpoint_results_distinguishable(client):
    c, _ = client
    # 不存在的实例 -> 404 NOT_FOUND
    r = c.post("/admin/instances/users/10.9.9.9/1/drain")
    assert r.status_code == 404 and r.json()["code"] == "NOT_FOUND"
    # 正常发起（无在途 -> 直接完成）
    r2 = c.post("/admin/instances/users/127.0.0.1/9001/drain")
    assert r2.status_code == 200
    assert r2.json()["code"] == "DRAIN_COMPLETED"
    LOG.info("下线响应=%s", r2.json())
    # 已完成后再次发起 -> 409，与成功可区分
    r3 = c.post("/admin/instances/users/127.0.0.1/9001/drain")
    assert r3.status_code == 409
    # 下线完成的实例不再被代理选中，但服务仍有其他实例可用
    for _ in range(10):
        resp = c.get("/proxy/users/1")
        assert resp.status_code == 200
        assert "9001" not in resp.text
    # 状态查询能看到另一个实例仍在 SERVING
    status = c.get("/admin/status/users").json()
    states = {row["instance"]: row["drain"]["state"] for row in status["instances"]}
    LOG.info("下线后状态=%s", states)
    assert states.get("users://127.0.0.1:9002") == "SERVING"
    assert "users://127.0.0.1:9001" not in states  # 已退出可服务集合


def test_heartbeat_while_draining_rejected(client):
    c, comp = client
    from anyio.from_thread import start_blocking_portal

    from gateway.models import InstanceId

    iid = InstanceId("users", "127.0.0.1", 9002)

    async def _hold_and_drain():
        # 占一个连接模拟在途，使下线停在 DRAINING 而不是立即完成。
        lease = await comp["pool"].acquire(iid)
        result = await comp["drain"].start_drain(iid)
        assert result.code.value == "DRAIN_STARTED"
        return lease

    async def _release(lease):
        lease.release()
        await comp["drain"].after_release(iid)

    with start_blocking_portal() as portal:
        lease = portal.call(_hold_and_drain)
        try:
            r = c.post("/admin/instances/users/127.0.0.1/9002/heartbeat", json={})
            LOG.info("下线中续约响应=%s", r.json())
            assert r.status_code == 409 and r.json()["code"] == "HEARTBEAT_DRAINING"
            # 状态查询能看到 DRAINING 与在途数
            status = portal.call(comp["drain"].explain, iid)
            assert status["state"] == "DRAINING" and status["in_flight"] == 1
        finally:
            portal.call(_release, lease)
    # 在途归零后实例真正退出
    assert c.get("/admin/instances/users/127.0.0.1/9002/drain").json()["state"] == "DRAINED"
