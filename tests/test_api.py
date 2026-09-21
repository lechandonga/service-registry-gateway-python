"""HTTP 层：归一化错误、状态码可区分、端到端代理与状态面。"""

from __future__ import annotations

import httpx
import pytest


@pytest.fixture
async def client(bundle):
    transport = httpx.ASGITransport(app=bundle["app"])
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        yield c


async def test_error_codes_are_distinguishable(client):
    # 无规则
    r = await client.get("/proxy/x")
    assert r.status_code == 404 and r.json()["error"] == "route_no_match"
    # 注册
    r = await client.post("/admin/instances", json={"id": "i", "host": "h", "port": 1})
    assert r.status_code == 201
    # 重复注册
    r = await client.post("/admin/instances", json={"id": "i", "host": "h", "port": 1})
    assert r.status_code == 409 and r.json()["error"] == "registry_duplicate"
    # 注销不存在
    r = await client.delete("/admin/instances/ghost")
    assert r.status_code == 404 and r.json()["error"] == "registry_not_found"


async def test_proxy_happy_path_and_headers(client):
    await client.post(
        "/admin/instances", json={"id": "i1", "host": "10.0.0.1", "port": 80}
    )
    await client.put("/admin/routes", json={"name": "all", "path_prefix": "/proxy/"})
    r = await client.get("/proxy/orders/42")
    assert r.status_code == 200
    assert r.headers["x-upstream-id"] == "i1"
    assert r.headers["x-gateway-attempts"] == "1"


async def test_failures_normalized_over_http(client, bundle):
    bundle["gateway"].max_retries = 0  # 该用例精确控制每次请求的上游结果
    bundle["circuits"]._threshold = 10_000  # 关闭熔断干扰
    bundle["health"]._fail_threshold = 10_000  # 关闭健康摘除干扰
    await client.post("/admin/instances", json={"id": "i", "host": "h", "port": 1})
    await client.put("/admin/routes", json={"name": "all", "path_prefix": "/proxy/"})
    # DNS 失败 -> 502 dns_error
    await client.post("/admin/instances/i/script", json={"results": ["dns_fail"]})
    r = await client.get("/proxy/x")
    assert r.status_code == 502 and r.json()["error"] == "dns_error"
    # 超时 -> 504 upstream_timeout
    await client.post("/admin/instances/i/script", json={"results": ["timeout"]})
    r = await client.get("/proxy/x", params={"timeout": 0.05})
    assert r.status_code == 504 and r.json()["error"] == "upstream_timeout"
    # 5xx 是真实响应 -> 503 响应体（非归一化错误 JSON）
    await client.post("/admin/instances/i/script", json={"results": ["http_503"]})
    r = await client.get("/proxy/x")
    assert r.status_code == 503 and r.text == "http_503"
    # 错误载荷中不含堆栈字段
    await client.post("/admin/instances/i/script", json={"results": ["unreachable"]})
    r = await client.get("/proxy/x")
    body = r.json()
    assert "Traceback" not in str(body)
    assert set(body.keys()) == {"error", "message", "detail"}


async def test_status_reports_and_lease(client, clock, bundle):
    await client.post(
        "/admin/instances",
        json={"id": "i", "host": "h", "port": 1, "lease_seconds": 5, "weight": 2},
    )
    r = await client.get("/admin/status")
    row = r.json()["instances"][0]
    assert row["registry_status"] == "online"
    assert row["lease_remaining"] == pytest.approx(5.0)
    clock.advance(6)
    r = await client.get("/admin/status")
    assert r.json()["instances"][0]["registry_status"] == "expired"


async def test_bad_script_body_is_bad_request(client):
    await client.post("/admin/instances", json={"id": "i", "host": "h", "port": 1})
    r = await client.post("/admin/instances/i/script", json={"results": "not-a-list"})
    assert r.status_code == 400 and r.json()["error"] == "bad_request"
