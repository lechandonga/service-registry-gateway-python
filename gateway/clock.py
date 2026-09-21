"""可注入的时间源，便于测试控制租约、抖动与熔断计时。"""

from __future__ import annotations

import time


class Clock:
    def now(self) -> float:
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> float:
        return time.monotonic()


class FakeClock(Clock):
    """测试用：手动推进的时钟，advance 后通过事件唤醒等待者。"""

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds
