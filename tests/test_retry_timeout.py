"""安全重试、幂等判定、超时背压、失败类型归一化。"""

from __future__ import annotations

import time

from gateway.errors import ErrorType, GatewayError
from gateway.models import Instance, Request

from .conftest import log_states


async def test_retries_other_instance_then_succeeds(catchall):
    b = catchall
    # 先注册的实例先被确定性选中
    await b["registry"].register(Instance(id="bad", host="h", port=1))
    await b["registry"].register(Instance(id="good", host="h", port=2))
    b["upstreams"].set_script("bad", ["unreachable"])
    resp = await b["gateway"].send(Request(path="/proxy/x"))
    assert resp.status == 200
    assert resp.upstream_id == "good"
    assert resp.attempts == 2
    assert b["limiter"].active_total() == 0
    await log_states(b, "retry landed on healthy instance; conn counters clean")


async def test_post_is_not_retried(catchall):
    b = catchall
    await b["registry"].register(Instance(id="bad", host="h", port=1))
    await b["registry"].register(Instance(id="good", host="h", port=2))
    b["upstreams"].set_script("bad", ["unreachable"])
    try:
        await b["gateway"].send(Request(path="/proxy/x", method="POST", body=b"pay"))
        raise AssertionError("expected failure")
    except GatewayError as e:
        assert e.reason is ErrorType.UPSTREAM_UNREACHABLE
    # 显式标记幂等的 POST 可以重试
    b["upstreams"].set_script("bad", ["unreachable"])
    resp = await b["gateway"].send(
        Request(path="/proxy/x", method="POST", body=b"q", idempotent=True)
    )
    assert resp.upstream_id == "good" and resp.attempts == 2


async def test_timeout_normalized_and_counters_released(catchall):
    b = catchall
    await b["registry"].register(Instance(id="slow", host="h", port=1))
    b["upstreams"].configure("slow", "timeout", timeout_delay=10)
    t0 = time.monotonic()
    try:
        await b["gateway"].send(Request(path="/proxy/x", timeout=0.05))
        raise AssertionError("expected timeout")
    except GatewayError as e:
        assert e.reason is ErrorType.UPSTREAM_TIMEOUT
        assert e.detail["timeout"] == 0.05
    assert time.monotonic() - t0 < 0.5
    assert b["limiter"].active_total() == 0
    assert b["limiter"].active_for("slow") == 0


async def test_dns_and_unreachable_are_distinct(catchall):
    b = catchall
    await b["registry"].register(Instance(id="u", host="h", port=1))
    b["upstreams"].configure("u", "dns_fail")
    try:
        await b["gateway"].send(Request(path="/proxy/x"))
    except GatewayError as e:
        assert e.reason is ErrorType.DNS_ERROR
    b["upstreams"].configure("u", "unreachable")
    try:
        await b["gateway"].send(Request(path="/proxy/x"))
    except GatewayError as e:
        assert e.reason is ErrorType.UPSTREAM_UNREACHABLE


async def test_5xx_response_returned_and_retried_when_idempotent(catchall):
    b = catchall
    await b["registry"].register(Instance(id="bad", host="h", port=1))
    await b["registry"].register(Instance(id="good", host="h", port=2))
    b["upstreams"].configure("bad", "http_5xx")
    resp = await b["gateway"].send(Request(path="/proxy/x"))
    assert resp.upstream_id == "good" and resp.attempts == 2

    # 所有上游都 5xx：预算用尽后把最后一个真实响应返回（状态可区分）
    b["upstreams"].configure("good", "http_5xx")
    resp = await b["gateway"].send(Request(path="/proxy/x"))
    assert resp.status == 503
    assert resp.attempts == 2


async def test_no_retry_candidate_gives_original_error(catchall):
    """首次失败后无其他实例，重试选路抛出的 NO_AVAILABLE 不应覆盖原始错误……

    约定：重试预算存在但没有可换实例时，保留第一次的连接类错误。
    """
    b = catchall
    await b["registry"].register(Instance(id="only", host="h", port=1))
    b["upstreams"].configure("only", "dns_fail")
    try:
        await b["gateway"].send(Request(path="/proxy/x"))
    except GatewayError as e:
        assert e.reason is ErrorType.DNS_ERROR


async def test_retry_blocked_by_open_circuit_keeps_first_connect_error(clock):
    """同一请求重试期间被熔断挡住时，必须保留首次连接错误。"""
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
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=99, clock=clock)
    lm = ConcurrencyLimiter(100, 10)
    g = Gateway(reg, router, hc, cb, lm, pool, max_retries=2)
    await reg.register(Instance(id="only", host="h", port=1))

    # 熔断器预置为差一次失败打开
    for _ in range(2):
        await cb.record_failure("only")
    pool.set_script("only", ["unreachable"])
    # 第 1 次尝试 unreachable（同时顶开熔断）；第 2 次 circuit_open。
    # 对外结论必须保持首次连接错误。
    with __import__("pytest").raises(GatewayError) as ei:
        await g.send(Request(path="/proxy/x"))
    assert ei.value.reason is ErrorType.UPSTREAM_UNREACHABLE
