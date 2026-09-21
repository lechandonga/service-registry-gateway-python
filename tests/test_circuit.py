"""熔断测试：阈值打开、快速失败原因、冷却后半开单飞、并发探测不重复放行。"""

import asyncio
import logging

import pytest

from gateway.circuit import CircuitBreaker, CircuitState
from gateway.clock import FakeClock
from gateway.errors import CircuitOpen
from gateway.models import new_instance

LOG = logging.getLogger("test.circuit")


def _log(cb, iid, phase):
    LOG.info("[%s] 上游=%s 熔断判定=%s", phase, iid, cb.explain(iid))


def test_opens_after_threshold_and_fast_fails():
    clock = FakeClock()
    cb = CircuitBreaker(clock, failure_threshold=3, open_seconds=5)
    iid = new_instance("svc", "h", 1).id
    for _ in range(3):
        cb.record_failure(iid)
    _log(cb, iid, "达到阈值")
    assert cb.state_of(iid) == CircuitState.OPEN
    with pytest.raises(CircuitOpen) as ei:
        cb.allow(iid)
    assert ei.value.code.value == "CIRCUIT_OPEN"
    assert "冷却" in ei.value.message


def test_below_threshold_remains_closed():
    clock = FakeClock()
    cb = CircuitBreaker(clock, failure_threshold=5, open_seconds=5)
    iid = new_instance("svc", "h", 1).id
    for _ in range(4):
        cb.record_failure(iid)
    assert cb.state_of(iid) == CircuitState.CLOSED
    assert cb.allow(iid) == CircuitState.CLOSED
    cb.record_failure(iid)
    assert cb.state_of(iid) == CircuitState.OPEN


def test_half_open_single_probe_then_close():
    clock = FakeClock()
    cb = CircuitBreaker(clock, failure_threshold=1, open_seconds=5)
    iid = new_instance("svc", "h", 1).id
    cb.record_failure(iid)
    clock.advance(5)
    assert cb.allow(iid) == CircuitState.HALF_OPEN
    _log(cb, iid, "冷却结束放行探测")
    # 探测未出结论前再次请求仍被拒绝。
    with pytest.raises(CircuitOpen):
        cb.allow(iid)
    cb.record_success(iid)
    assert cb.allow(iid) == CircuitState.CLOSED
    _log(cb, iid, "探测成功关闭熔断")


def test_half_open_failure_reopens():
    clock = FakeClock()
    cb = CircuitBreaker(clock, failure_threshold=1, open_seconds=5)
    iid = new_instance("svc", "h", 1).id
    cb.record_failure(iid)
    clock.advance(5)
    cb.allow(iid)
    cb.record_failure(iid)
    _log(cb, iid, "半开失败重开")
    assert cb.state_of(iid) == CircuitState.OPEN
    with pytest.raises(CircuitOpen):
        cb.allow(iid)


@pytest.mark.asyncio
async def test_concurrent_recovery_only_one_probe_passes():
    clock = FakeClock()
    cb = CircuitBreaker(clock, failure_threshold=1, open_seconds=5)
    iid = new_instance("svc", "h", 1).id
    cb.record_failure(iid)
    clock.advance(5)

    passes = 0
    rejects = 0

    async def attempt():
        nonlocal passes, rejects
        try:
            cb.allow(iid)
            passes += 1
            await asyncio.sleep(0.01)  # 探测在途
        except CircuitOpen:
            rejects += 1

    await asyncio.gather(*(attempt() for _ in range(50)))
    _log(cb, iid, f"50并发恢复探测 放行{passes} 拒绝{rejects}")
    assert passes == 1
    assert rejects == 49
