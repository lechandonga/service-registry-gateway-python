"""HTTP 接口集成测试：注册/续约/注销、代理转发、失败归一化、状态观测。"""

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.api import create_app
from gateway.bootstrap import build_components, _seed

LOG = logging.getLogger("test.api")


@pytest.fixture()
def client():
    comp = build_components()
    async def _startup():
        await _seed(comp)

    app = create_app(
        comp["registry"], comp["router"], comp["gateway"], comp["health"],
        comp["circuit"], comp["cluster"], clock=comp["clock"], logger=comp["logger"],
        on_startup=_startup,
    )
    with TestClient(app) as c:
        yield c, comp


def test_root(client):
    c, _ = client
    assert c.get("/").json()["service"]


def test_register_duplicate_deregister_flow(client):
    c, _ = client
    payload = {"service": "svc", "host": "10.0.0.1", "port": 8000, "weight": 2, "lease_seconds": 30}
    r = c.post("/admin/register", json=payload)
    assert r.status_code == 200 and r.json()["registered"] is True
    r2 = c.post("/admin/register", json=payload)
    assert r2.status_code == 409 and r2.json()["code"] == "REG_DUPLICATE"
    LOG.info("重复注册响应=%s", r2.json())
    r3 = c.delete("/admin/instances/svc/10.0.0.1/8000")
    assert r3.status_code == 200
    r4 = c.delete("/admin/instances/svc/10.0.0.1/8000")
    assert r4.status_code == 404 and r4.json()["code"] == "UNREG_NOT_FOUND"


def test_heartbeat_unknown_is_distinguishable(client):
    c, _ = client
    r = c.post("/admin/instances/svc/nohost/1/heartbeat", json={})
    assert r.status_code == 404 and r.json()["code"] == "HEARTBEAT_UNKNOWN"


def test_proxy_success_and_no_route(client):
    c, comp = client
    r = c.get("/proxy/users/123")
    assert r.status_code == 200
    assert "users" in r.text
    LOG.info("代理成功 body=%s", r.text[:60])
    r2 = c.get("/proxy/unknown/x")
    assert r2.status_code == 404 and r2.json()["code"] == "NO_ROUTE_RULE"


def test_proxy_orders_requires_header(client):
    c, comp = client
    r = c.get("/proxy/orders/1")
    assert r.status_code == 404 and r.json()["code"] == "NO_ROUTE_RULE"
    r2 = c.get("/proxy/orders/1", headers={"x-canary": "true"})
    assert r2.status_code == 200


def test_upstream_failure_normalized_via_mode(client):
    c, comp = client
    payload = {"service": "flaky", "host": "h1", "port": 1, "lease_seconds": 60}
    c.post("/admin/register", json=payload)
    c.post("/admin/rules", json={"service": "flaky", "path_prefix": "/proxy/flaky"})
    r = c.post("/admin/instances/flaky/h1/1/mode", json={"mode": "unreachable"})
    assert r.status_code == 200
    resp = c.get("/proxy/flaky/x")
    assert resp.status_code == 502 and resp.json()["code"] == "UPSTREAM_UNREACHABLE"
    LOG.info("不可达归一化=%s", resp.json())
    assert "Traceback" not in resp.text


def test_status_endpoint_reports_state(client):
    c, comp = client
    r = c.get("/admin/status/users")
    assert r.status_code == 200
    rows = r.json()["instances"]
    assert len(rows) == 2
    LOG.info("users 上游状态=%s", rows)
    assert all(row["health"]["state"] == "HEALTHY" for row in rows)
    assert all(row["circuit"]["state"] == "CLOSED" for row in rows)
