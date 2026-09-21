"""主动+被动健康检查：抖动容忍、摘除与恢复。"""

from __future__ import annotations

import threading

from .clock import Clock, SystemClock
from .models import Instance, InstanceId


class HealthProber:
    """主动探测协议：返回 True 表示健康。"""

    async def probe(self, instance: Instance) -> bool:
        raise NotImplementedError


class HealthState:
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    DOWN = "DOWN"


class _Status:
    __slots__ = ("state", "failures", "successes", "reason", "updated_at")

    def __init__(self, now: float) -> None:
        self.state: str = HealthState.HEALTHY
        self.failures: int = 0
        self.successes: int = 0
        self.reason: str = "初始健康"
        self.updated_at: float = now


class HealthMonitor:
    """
    维护每实例健康状态（threading.Lock 保护，临界区内无 await，同步/异步反馈均安全）。

    - 被动反馈：连续 failure_threshold 次失败 -> DOWN；中途成功立即清零（抖动容忍）。
    - 失败 1..threshold-1 为 SUSPECT，仍可被选中但记录判定依据。
    - 主动探测：DOWN 后需连续 recovery_threshold 次探测成功才恢复 HEALTHY；
      期间任意一次失败即清零恢复计数，避免短暂抖动造成反复摘除/恢复。
    """

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
    ) -> None:
        self._clock = clock or SystemClock()
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold
        self._status: dict[InstanceId, _Status] = {}
        self._lock = threading.Lock()

    def _ensure_locked(self, instance_id: InstanceId) -> _Status:
        st = self._status.get(instance_id)
        if st is None:
            st = _Status(self._clock.now())
            self._status[instance_id] = st
        return st

    def record_success(self, instance_id: InstanceId) -> str:
        with self._lock:
            st = self._ensure_locked(instance_id)
            now = self._clock.now()
            if st.state == HealthState.DOWN:
                st.successes += 1
                st.reason = f"恢复探测成功 {st.successes}/{self._recovery_threshold}"
                if st.successes >= self._recovery_threshold:
                    st.state = HealthState.HEALTHY
                    st.failures = 0
                    st.successes = 0
                    st.reason = "连续探测成功，恢复纳入"
            else:
                st.failures = 0
                st.state = HealthState.HEALTHY
                st.successes = 0
                st.reason = "请求成功，失败计数清零"
            st.updated_at = now
            return st.state

    def record_failure(self, instance_id: InstanceId) -> str:
        with self._lock:
            st = self._ensure_locked(instance_id)
            now = self._clock.now()
            st.successes = 0
            if st.state == HealthState.DOWN:
                st.reason = "DOWN 状态下探测仍失败，保持摘除"
            else:
                st.failures += 1
                if st.failures >= self._failure_threshold:
                    st.state = HealthState.DOWN
                    st.reason = f"连续失败 {st.failures} 次达到阈值，摘除"
                else:
                    st.state = HealthState.SUSPECT
                    st.reason = f"连续失败 {st.failures}/{self._failure_threshold}，可疑但保留"
            st.updated_at = now
            return st.state

    async def arecord_success(self, instance_id: InstanceId) -> str:
        return self.record_success(instance_id)

    async def arecord_failure(self, instance_id: InstanceId) -> str:
        return self.record_failure(instance_id)

    def is_available(self, instance_id: InstanceId) -> bool:
        with self._lock:
            st = self._status.get(instance_id)
            return st is None or st.state != HealthState.DOWN

    def state_of(self, instance_id: InstanceId) -> str:
        with self._lock:
            st = self._status.get(instance_id)
            return st.state if st else HealthState.HEALTHY

    def explain(self, instance_id: InstanceId) -> dict[str, object]:
        with self._lock:
            st = self._status.get(instance_id)
            if st is None:
                return {
                    "state": HealthState.HEALTHY,
                    "failures": 0,
                    "recovery_successes": 0,
                    "reason": "无失败记录，默认健康",
                    "available": True,
                }
            return {
                "state": st.state,
                "failures": st.failures,
                "recovery_successes": st.successes,
                "reason": st.reason,
                "available": st.state != HealthState.DOWN,
                "updated_at": st.updated_at,
            }

    async def probe_once(self, instance_id: InstanceId, prober: HealthProber, instance: Instance) -> str:
        try:
            ok = bool(await prober.probe(instance))
        except Exception:
            # 探测本身抛错按失败处理，异常不外泄。
            ok = False
        return self.record_success(instance_id) if ok else self.record_failure(instance_id)

    def remove(self, instance_id: InstanceId) -> None:
        with self._lock:
            self._status.pop(instance_id, None)
