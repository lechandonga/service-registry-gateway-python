"""熔断器（每实例一个）。

CLOSED -> 连续失败达阈值 -> OPEN
OPEN 期间快速失败；冷却结束后只放一个 HALF_OPEN 探测（并发去重）。
HALF_OPEN 成功 -> CLOSED，失败 -> OPEN（重新计时冷却）。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class _Stat:
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    successes: int = 0
    opened_at: float = 0.0
    probes_in_flight: int = 0


@dataclass
class CircuitSnapshot:
    state: CircuitState
    failures: int
    opened_at: float
    probes_in_flight: int


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 5.0,
        clock=time.monotonic,
    ) -> None:
        self._threshold = failure_threshold
        self._recovery = recovery_timeout
        self._clock = clock
        self._stats: dict[str, _Stat] = {}
        self._lock = asyncio.Lock()

    def _get(self, instance_id: str) -> _Stat:
        st = self._stats.get(instance_id)
        if st is None:
            st = _Stat()
            self._stats[instance_id] = st
        return st

    def current_state(self, instance_id: str, now: float | None = None) -> CircuitState:
        """逻辑态：OPEN 已过冷却期在视角上仍为 OPEN，直到 allow() 放行探测。"""
        st = self._stats.get(instance_id)
        return CircuitState.CLOSED if st is None else st.state

    async def allow(self, instance_id: str) -> bool:
        """是否放行请求。

        CLOSED 放行；OPEN 冷却未到拒绝；OPEN 冷却到 -> 恰好放一个 HALF_OPEN 探测，
        其余并发请求全部拒绝（fast-fail）。
        """
        now = self._clock()
        async with self._lock:
            st = self._get(instance_id)
            if st.state is CircuitState.CLOSED:
                return True
            if st.state is CircuitState.OPEN:
                if now - st.opened_at < self._recovery:
                    return False
                st.state = CircuitState.HALF_OPEN
                st.probes_in_flight = 1
                st.successes = 0
                return True
            # HALF_OPEN：恢复探测未完成，拒绝其他请求
            return False

    async def record_success(self, instance_id: str) -> None:
        async with self._lock:
            st = self._get(instance_id)
            if st.state is CircuitState.HALF_OPEN:
                # 探测成功：恢复纳回
                st.state = CircuitState.CLOSED
                st.failures = 0
                st.successes = 0
                st.probes_in_flight = 0
            else:
                st.failures = 0
                st.successes += 1

    async def record_failure(self, instance_id: str) -> None:
        async with self._lock:
            st = self._get(instance_id)
            st.failures += 1
            if st.state is CircuitState.HALF_OPEN:
                # 探测失败：重新打开并重新计时冷却
                st.state = CircuitState.OPEN
                st.opened_at = self._clock()
                st.probes_in_flight = 0
            elif st.failures >= self._threshold:
                st.state = CircuitState.OPEN
                st.opened_at = self._clock()

    def snapshot(self, instance_id: str) -> CircuitSnapshot:
        st = self._stats.get(instance_id) or _Stat()
        return CircuitSnapshot(st.state, st.failures, st.opened_at, st.probes_in_flight)
