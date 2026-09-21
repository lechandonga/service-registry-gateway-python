"""熔断器：快速失败、冷却后恢复探测的并发去重。"""

from __future__ import annotations

import asyncio

from gateway.circuit import CircuitBreaker, CircuitState
from gateway.errors import ErrorType, GatewayError
from gateway.gateway import Gateway
from gateway.health import HealthChecker
from gateway.limiter import ConcurrencyLimiter
from gateway.models import Instance, Request, RouteRule
from gateway.router import Router

from .conftest import log_states


async def _gateway_without_health_eviction(clock, upstreams, registry):
    """健康阈值调到极大，使熔断器成为唯一生效的防线。"""
    hc = HealthChecker(
        registry,
        upstreams,
        fail_threshold=10_000,
        recover_threshold=1,
        recover_cooldown=0,
    )
    router = Router(
        registry, hc, rng=__import__("tests.conftest", fromlist=["FirstRNG"]).FirstRNG()
    )
    router.add_rule(RouteRule(name="all", path_prefix="/proxy/"))
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0, clock=clock)
    lm = ConcurrencyLimiter(100, 10)
    return Gateway(registry, router, hc, cb, lm, upstreams, max_retries=0), cb, hc, lm


async def test_circuit_opens_then_single_probe_passes(clock):
    from gateway.registry import ServiceRegistry
    from gateway.upstream import LocalUpstreamPool

    reg = ServiceRegistry(clock=clock)
    pool = LocalUpstreamPool(delay=0)
    g, cb, hc, lm = await _gateway_without_health_eviction(clock, pool, reg)
    b = {
        "registry": reg,
        "upstreams": pool,
        "circuits": cb,
        "gateway": g,
        "health": hc,
        "limiter": lm,
    }
    await reg.register(Instance(id="s", host="h", port=1))
    pool.configure("s", "unreachable")

    for _ in range(3):
        try:
            await g.send(Request(path="/proxy/x", idempotent=True))
        except GatewayError as e:
            assert e.reason is ErrorType.UPSTREAM_UNREACHABLE
    assert cb.current_state("s") is CircuitState.OPEN
    await log_states(b, "circuit OPEN -> fast fail")

    # 即便上游恢复，OPEN 冷却内仍快速失败
    b["upstreams"].configure("s", "healthy")
    with __import__("pytest").raises(GatewayError) as ei:
        await g.send(Request(path="/proxy/x"))
    assert ei.value.reason is ErrorType.CIRCUIT_OPEN

    # 冷却后并发 32 个请求：恰好 1 个作为 HALF_OPEN 探测放行
    clock.advance(6)
    results = await asyncio.gather(*[cb.allow("s") for _ in range(32)])
    assert sum(1 for x in results if x) == 1
    assert cb.snapshot("s").probes_in_flight == 1
    assert await cb.allow("s") is False


async def test_half_open_failure_reopens_and_rearms_cooldown(clock, catchall):
    b = catchall
    cb = b["circuits"]
    for _ in range(3):
        await cb.record_failure("s")
    assert cb.current_state("s") is CircuitState.OPEN
    clock.advance(6)
    assert await cb.allow("s") is True
    await cb.record_failure("s")
    assert cb.current_state("s") is CircuitState.OPEN
    # 冷却被重新计时，立刻再请求仍被拒绝
    assert await cb.allow("s") is False
    clock.advance(6)
    assert await cb.allow("s") is True
    await cb.record_success("s")
    assert cb.current_state("s") is CircuitState.CLOSED
    assert await cb.allow("s") is True


async def test_probe_success_closes_and_succeeds(clock):
    from gateway.registry import ServiceRegistry
    from gateway.upstream import LocalUpstreamPool

    reg = ServiceRegistry(clock=clock)
    pool = LocalUpstreamPool(delay=0)
    g, cb, _hc, _lm = await _gateway_without_health_eviction(clock, pool, reg)
    await reg.register(Instance(id="s", host="h", port=1))
    pool.configure("s", "unreachable")
    for _ in range(3):
        try:
            await g.send(Request(path="/proxy/x"))
        except GatewayError:
            pass
    assert cb.current_state("s") is CircuitState.OPEN
    clock.advance(6)
    assert await cb.allow("s") is True  # 唯一恢复探测被放行 -> HALF_OPEN
    await cb.record_success("s")  # 探测成功 -> CLOSED
    assert cb.current_state("s") is CircuitState.CLOSED
    pool.configure("s", "healthy")
    resp = await g.send(Request(path="/proxy/x"))
    assert resp.status == 200 and resp.upstream_id == "s"
