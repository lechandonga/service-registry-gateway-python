"""健康检查测试：抖动不摘除、阈值摘除、连续成功恢复、探测异常归一化。"""

import logging

import pytest

from gateway.clock import FakeClock
from gateway.health import HealthMonitor, HealthProber, HealthState
from gateway.models import new_instance

LOG = logging.getLogger("test.health")


class _ScriptedProber(HealthProber):
    def __init__(self, results):
        self._results = list(results)

    async def probe(self, instance) -> bool:
        return self._results.pop(0)


class _RaisingProber(HealthProber):
    async def probe(self, instance) -> bool:
        raise RuntimeError("模拟探测内部崩溃，不应外泄")


def _log_state(mon, iid, phase):
    LOG.info("[%s] 上游=%s 状态与判定=%s", phase, iid, mon.explain(iid))


@pytest.mark.asyncio
async def test_single_jitter_does_not_evict():
    clock = FakeClock()
    mon = HealthMonitor(clock, failure_threshold=3, recovery_threshold=2)
    inst = new_instance("svc", "h", 1, clock=clock)
    assert mon.record_failure(inst.id) == HealthState.SUSPECT
    _log_state(mon, inst.id, "单次失败")
    assert mon.is_available(inst.id) is True
    mon.record_success(inst.id)
    _log_state(mon, inst.id, "抖动后成功")
    assert mon.state_of(inst.id) == HealthState.HEALTHY
    assert mon.explain(inst.id)["failures"] == 0


@pytest.mark.asyncio
async def test_consecutive_failures_evict_then_recover():
    clock = FakeClock()
    mon = HealthMonitor(clock, failure_threshold=3, recovery_threshold=2)
    inst = new_instance("svc", "h", 1, clock=clock)
    prober = _ScriptedProber([True, True])
    for _ in range(2):
        mon.record_failure(inst.id)
    assert mon.is_available(inst.id) is True
    mon.record_failure(inst.id)
    _log_state(mon, inst.id, "连续三次失败")
    assert mon.is_available(inst.id) is False

    # 恢复需要连续两次成功；一次不够。
    await mon.probe_once(inst.id, prober, inst)
    _log_state(mon, inst.id, "恢复探测1/2")
    assert mon.is_available(inst.id) is False
    await mon.probe_once(inst.id, prober, inst)
    _log_state(mon, inst.id, "恢复探测2/2")
    assert mon.is_available(inst.id) is True


@pytest.mark.asyncio
async def test_recovery_interrupted_by_failure_keeps_down():
    clock = FakeClock()
    mon = HealthMonitor(clock, failure_threshold=2, recovery_threshold=3)
    inst = new_instance("svc", "h", 1, clock=clock)
    prober = _ScriptedProber([True, False, True, True, True])
    mon.record_failure(inst.id)
    mon.record_failure(inst.id)
    assert mon.is_available(inst.id) is False
    await mon.probe_once(inst.id, prober, inst)  # 成功1
    await mon.probe_once(inst.id, prober, inst)  # 失败 -> 恢复计数清零
    assert mon.is_available(inst.id) is False
    assert mon.explain(inst.id)["recovery_successes"] == 0
    for _ in range(3):
        await mon.probe_once(inst.id, prober, inst)
    assert mon.is_available(inst.id) is True


@pytest.mark.asyncio
async def test_prober_exception_treated_as_failure():
    clock = FakeClock()
    mon = HealthMonitor(clock, failure_threshold=1, recovery_threshold=1)
    inst = new_instance("svc", "h", 1, clock=clock)
    await mon.probe_once(inst.id, _RaisingProber(), inst)
    assert mon.state_of(inst.id) == HealthState.DOWN
    _log_state(mon, inst.id, "探测抛异常")


def test_unknown_instance_defaults_healthy():
    mon = HealthMonitor(FakeClock())
    iid = new_instance("svc", "x", 9).id
    assert mon.is_available(iid) is True
    assert mon.explain(iid)["available"] is True
