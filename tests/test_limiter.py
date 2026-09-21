"""并发/连接上限、背压拒绝与资源计数一致性。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.errors import ErrorType, GatewayError
from gateway.models import Instance, Request


async def test_instance_and_gateway_limits_are_distinct():
    from gateway.limiter import ConcurrencyLimiter

    lm = ConcurrencyLimiter(gateway_max=2, instance_max=1)
    async with lm.acquire("a"):
        with pytest.raises(GatewayError) as ei:
            async with lm.acquire("a"):
                pass
        assert ei.value.reason is ErrorType.LIMIT_CONNECTIONS
        async with lm.acquire("b"):
            with pytest.raises(GatewayError) as ei2:
                async with lm.acquire("c"):
                    pass
            assert ei2.value.reason is ErrorType.LIMIT_CONCURRENCY
    assert lm.active_total() == 0


async def test_exception_path_releases_slots():
    from gateway.limiter import ConcurrencyLimiter

    lm = ConcurrencyLimiter(gateway_max=1, instance_max=1)
    with pytest.raises(RuntimeError):
        async with lm.acquire("a"):
            raise RuntimeError("boom")
    assert lm.active_total() == 0 and lm.active_for("a") == 0
    # 名额可再次获取
    async with lm.acquire("a"):
        assert lm.active_total() == 1


async def test_gateway_timeout_leaves_no_connection(clock):
    # 独立构建：关闭健康摘除，专注验证超时后计数不残留
    from gateway.circuit import CircuitBreaker
    from gateway.gateway import Gateway
    from gateway.health import HealthChecker
    from gateway.limiter import ConcurrencyLimiter
    from gateway.models import RouteRule
    from gateway.registry import ServiceRegistry
    from gateway.router import Router
    from gateway.upstream import LocalUpstreamPool

    from .conftest import FirstRNG

    reg = ServiceRegistry(clock=clock)
    pool = LocalUpstreamPool(delay=0)
    hc = HealthChecker(
        reg, pool, fail_threshold=10_000, recover_threshold=1, recover_cooldown=0
    )
    router = Router(reg, hc, rng=FirstRNG())
    router.add_rule(RouteRule(name="all", path_prefix="/proxy/"))
    cb = CircuitBreaker(failure_threshold=10_000, recovery_timeout=99, clock=clock)
    lm = ConcurrencyLimiter(100, 10)
    g = Gateway(reg, router, hc, cb, lm, pool, max_retries=0)
    await reg.register(Instance(id="slow", host="h", port=1))
    pool.configure("slow", "timeout", timeout_delay=10)
    for _ in range(5):
        try:
            await g.send(Request(path="/proxy/x", timeout=0.02))
        except GatewayError as e:
            assert e.reason is ErrorType.UPSTREAM_TIMEOUT
    assert lm.active_total() == 0
    assert lm.active_for("slow") == 0


async def test_backpressure_under_concurrency():
    from gateway.limiter import ConcurrencyLimiter

    lm = ConcurrencyLimiter(gateway_max=4, instance_max=2)
    rejected_total = 0
    rejected_instance = 0

    async def worker(seq):
        nonlocal rejected_total, rejected_instance
        for i in range(40):
            iid = "a" if (seq + i) % 2 else "b"
            try:
                async with lm.acquire(iid):
                    await asyncio.sleep(0)
            except GatewayError as e:
                if e.reason is ErrorType.LIMIT_CONNECTIONS:
                    rejected_instance += 1
                else:
                    assert e.reason is ErrorType.LIMIT_CONCURRENCY
                    rejected_total += 1

    await asyncio.gather(*(worker(s) for s in range(16)))
    assert lm.active_total() == 0
    assert lm.active_for("a") == 0 and lm.active_for("b") == 0
    assert rejected_total + rejected_instance > 0  # 确实发生了背压
