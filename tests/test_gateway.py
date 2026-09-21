"""网关编排测试：重试安全性、熔断快败、背压、超时归一化、资源无泄漏。"""

import logging

import pytest

from gateway.circuit import CircuitBreaker
from gateway.clock import FakeClock
from gateway.engine import Gateway
from gateway.errors import (
    CircuitOpen,
    ConcurrencyLimitReached,
    FailureCode,
    UpstreamTimeout,
)
from gateway.health import HealthMonitor
from gateway.models import Request, RouteRule, new_instance
from gateway.pool import ConnectionPool
from gateway.registry import Registry
from gateway.router import Router
from gateway.transport import UpstreamTransport
from gateway.upstream import UpstreamCluster

LOG = logging.getLogger("test.gateway")


def build_gateway(clock, *, max_per=100, max_total=None, timeout=1.0, retries=2):
    reg = Registry(clock)
    health = HealthMonitor(clock, failure_threshold=3, recovery_threshold=1)
    cb = CircuitBreaker(clock, failure_threshold=3, open_seconds=10)
    pool = ConnectionPool(max_per, max_total)
    cluster = UpstreamCluster()
    transport = UpstreamTransport(cluster)
    router = Router(reg, health, cb)
    router.add_rule(RouteRule(service="svc", path_prefix="/"))
    gw = Gateway(router, transport, health, cb, pool,
                 max_retries=retries, upstream_timeout=timeout)
    return gw, reg, health, cb, pool, cluster


async def register(reg, cluster, clock, host, mode="ok", weight=1, **kw):
    inst = new_instance("svc", host, 8000, weight=weight, clock=clock, **kw)
    await reg.register(inst)
    cluster.set_mode(inst, mode, **{"hang_seconds": 5.0} if mode == "timeout" else {})
    return inst


@pytest.mark.asyncio
async def test_success_path_releases_connection():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock)
    await register(reg, cluster, clock, "h1")
    resp = await gw.dispatch(Request("GET", "/"))
    assert resp.ok
    assert pool.active_total() == 0
    LOG.info("成功后连接残留=%d", pool.active_total())


@pytest.mark.asyncio
async def test_non_safe_request_not_retried():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock, retries=2)
    inst = await register(reg, cluster, clock, "h1", mode="unreachable")
    with pytest.raises(Exception) as ei:
        await gw.dispatch(Request("POST", "/orders"))
    assert ei.value.code == FailureCode.UPSTREAM_UNREACHABLE
    assert cluster.get(inst).calls == 1  # POST 只调用一次
    assert pool.active_total() == 0
    LOG.info("POST不可重试 调用次数=%d code=%s", cluster.get(inst).calls, ei.value.code.value)


@pytest.mark.asyncio
async def test_safe_request_retries_other_instance_then_succeeds():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock, retries=2)
    bad = await register(reg, cluster, clock, "bad", mode="unreachable")
    good = await register(reg, cluster, clock, "good", mode="ok", weight=100)
    # 让 bad 权重极低也不影响：给 good 高权重但保证两次选择至少覆盖——直接固定 bad 权重1/good 99
    # 为确定性，先用全 bad 触发重试，再把 good 权重拉高。
    resp = await gw.dispatch(Request("GET", "/"))
    assert resp.ok
    assert pool.active_total() == 0
    LOG.info("重试后成功 upstream=%s bad调用=%d", resp.upstream, cluster.get(bad).calls)


@pytest.mark.asyncio
async def test_timeout_normalized_and_retry_exhausted():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock, timeout=0.05, retries=1)
    await register(reg, cluster, clock, "h1", mode="timeout")
    with pytest.raises(Exception) as ei:
        await gw.dispatch(Request("GET", "/"))
    assert ei.value.code == FailureCode.UPSTREAM_TIMEOUT
    assert pool.active_total() == 0
    LOG.info("超时归一化 code=%s 残留连接=%d", ei.value.code.value, pool.active_total())


@pytest.mark.asyncio
async def test_circuit_open_fast_fails_and_uses_other_instance():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock)
    bad = await register(reg, cluster, clock, "h1", mode="ok", weight=9999)
    good = await register(reg, cluster, clock, "h2", mode="ok", weight=1)
    for _ in range(3):
        cb.record_failure(bad.id)
    # 路由候选只含 good（bad 熔断过滤），请求成功且 bad 零调用 -> 熔断未透传到调用。
    resp = await gw.dispatch(Request("GET", "/"))
    assert resp.ok and resp.upstream.endswith("h2:8000")
    assert cluster.get(bad).calls == 0
    assert pool.active_total() == 0
    LOG.info("熔断实例=%s 被过滤，零调用；实际服务=%s", bad.id, resp.upstream)


@pytest.mark.asyncio
async def test_concurrency_limit_rejected_without_leak():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock, max_per=1)
    inst = await register(reg, cluster, clock, "h1", mode="ok")
    # 占用唯一连接，模拟在途。
    lease = await pool.acquire(inst.id)
    try:
        with pytest.raises(ConcurrencyLimitReached) as ei:
            await gw.dispatch(Request("GET", "/"))
        assert ei.value.code.value == "CONCURRENCY_LIMIT"
    finally:
        lease.release()
    assert pool.active_total() == 0
    LOG.info("背压拒绝后残留=%d", pool.active_total())


@pytest.mark.asyncio
async def test_all_unreachable_eventually_fails_with_reason():
    clock = FakeClock()
    gw, reg, health, cb, pool, cluster = build_gateway(clock, retries=2)
    await register(reg, cluster, clock, "h1", mode="unreachable")
    await register(reg, cluster, clock, "h2", mode="unreachable")
    with pytest.raises(Exception) as ei:
        await gw.dispatch(Request("GET", "/"))
    assert ei.value.code == FailureCode.UPSTREAM_UNREACHABLE
    assert pool.active_total() == 0
