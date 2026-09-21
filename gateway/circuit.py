"""熔断器：CLOSED/OPEN/HALF_OPEN，OPEN 快速失败，HALF_OPEN 单飞探测。"""

from __future__ import annotations

import threading
from enum import Enum

from .clock import Clock, SystemClock
from .models import InstanceId


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class _Stat:
    __slots__ = ("state", "failures", "opened_at", "probe_in_flight", "reason")

    def __init__(self) -> None:
        self.state: CircuitState = CircuitState.CLOSED
        self.failures: int = 0
        self.opened_at: float = 0.0
        self.probe_in_flight: bool = False
        self.reason: str = "初始关闭"


class CircuitBreaker:
    """
    每实例熔断状态（threading.Lock 保护，临界区内无阻塞）。

    - CLOSED：连续失败达 failure_threshold -> OPEN。
    - OPEN：allow 快速失败；经过 open_seconds 后，首个 allow 把状态切到 HALF_OPEN
      并占用唯一探测名额（probe_in_flight），并发的其他 allow 仍被拒绝。
    - HALF_OPEN：探测成功 -> CLOSED；探测失败 -> 重新 OPEN 并重置冷却计时。
    """

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        failure_threshold: int = 5,
        open_seconds: float = 5.0,
    ) -> None:
        self._clock = clock or SystemClock()
        self._failure_threshold = failure_threshold
        self._open_seconds = open_seconds
        self._stats: dict[InstanceId, _Stat] = {}
        self._lock = threading.Lock()

    def _ensure(self, instance_id: InstanceId) -> _Stat:
        st = self._stats.get(instance_id)
        if st is None:
            st = _Stat()
            self._stats[instance_id] = st
        return st

    def allow(self, instance_id: InstanceId) -> CircuitState:
        """返回放行时的状态；拒绝时抛 CircuitOpen。状态转换在锁内完成。"""
        from .errors import CircuitOpen

        with self._lock:
            st = self._ensure(instance_id)
            now = self._clock.now()
            if st.state == CircuitState.CLOSED:
                return CircuitState.CLOSED
            if st.state == CircuitState.OPEN:
                if now - st.opened_at < self._open_seconds:
                    raise CircuitOpen(
                        f"熔断打开中，剩余冷却 {self._open_seconds - (now - st.opened_at):.3f}s",
                    )
                # 冷却结束：仅首个请求转入 HALF_OPEN 做单飞探测。
                st.state = CircuitState.HALF_OPEN
                st.probe_in_flight = True
                st.reason = "冷却结束，放行唯一探测请求"
                return CircuitState.HALF_OPEN
            # HALF_OPEN：已有探测在途则继续快速失败，恢复探测不重复放行。
            if st.probe_in_flight:
                raise CircuitOpen("半开探测进行中，并发探测被拒绝")
            # 理论上前一个探测已出结论，保持当前状态快速失败。
            raise CircuitOpen("半开状态等待探测结论")

    def record_success(self, instance_id: InstanceId) -> None:
        with self._lock:
            st = self._ensure(instance_id)
            st.state = CircuitState.CLOSED
            st.failures = 0
            st.probe_in_flight = False
            st.reason = "探测/请求成功，熔断关闭"

    def record_failure(self, instance_id: InstanceId) -> None:
        with self._lock:
            st = self._ensure(instance_id)
            now = self._clock.now()
            if st.state == CircuitState.HALF_OPEN:
                # 半开探测失败：立即重新打开并重置冷却。
                st.state = CircuitState.OPEN
                st.opened_at = now
                st.probe_in_flight = False
                st.reason = "半开探测失败，重新打开并重置冷却"
                return
            # CLOSED 状态：累计失败，仅达阈值才打开。
            st.failures += 1
            if st.failures >= self._failure_threshold:
                st.state = CircuitState.OPEN
                st.opened_at = now
                st.probe_in_flight = False
                st.reason = f"连续失败 {st.failures}/{self._failure_threshold}，熔断打开"
            else:
                st.reason = f"连续失败 {st.failures}/{self._failure_threshold}"

    def state_of(self, instance_id: InstanceId) -> CircuitState:
        with self._lock:
            st = self._stats.get(instance_id)
            return st.state if st else CircuitState.CLOSED

    def explain(self, instance_id: InstanceId) -> dict[str, object]:
        with self._lock:
            st = self._stats.get(instance_id)
            now = self._clock.now()
            if st is None:
                return {
                    "state": CircuitState.CLOSED.value,
                    "failures": 0,
                    "probe_in_flight": False,
                    "cooldown_remaining": None,
                    "reason": "无失败记录",
                }
            remaining = None
            if st.state == CircuitState.OPEN:
                remaining = max(0.0, self._open_seconds - (now - st.opened_at))
            return {
                "state": st.state.value,
                "failures": st.failures,
                "opened_at": st.opened_at,
                "probe_in_flight": st.probe_in_flight,
                "cooldown_remaining": remaining,
                "reason": st.reason,
            }

    def remove(self, instance_id: InstanceId) -> None:
        with self._lock:
            self._stats.pop(instance_id, None)
