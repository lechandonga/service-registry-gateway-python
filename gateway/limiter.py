"""连接/并发上限与背压。

- 全局限号：gateway_max
- 每实例连接计数：instance_max
- 超限立即拒绝（背压），区分 LIMIT_CONCURRENCY / LIMIT_CONNECTIONS
- 名额在 finally 中释放，保证异常路径不残留、不重复扣减。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from .errors import ErrorType, GatewayError


class ConcurrencyLimiter:
    def __init__(self, gateway_max: int = 100, instance_max: int = 10) -> None:
        self._gateway_max = gateway_max
        self._instance_max = instance_max
        self._gateway_active = 0
        self._instance_active: dict[str, int] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, instance_id: str):
        """获取全局+实例名额；拿不到立即抛错。退出时各释放一次。

        获取顺序固定为“先全局、后实例”：实例名额拿不到时回滚全局名额，
        任何路径都不会出现漏减或重复扣减。
        """
        async with self._lock:
            if self._gateway_active >= self._gateway_max:
                raise GatewayError(
                    ErrorType.LIMIT_CONCURRENCY,
                    "gateway concurrency limit reached, rejecting (backpressure)",
                    detail={"active": self._gateway_active, "max": self._gateway_max},
                )
            if self._instance_active.get(instance_id, 0) >= self._instance_max:
                raise GatewayError(
                    ErrorType.LIMIT_CONNECTIONS,
                    f"upstream {instance_id} connection limit reached, rejecting (backpressure)",
                    detail={
                        "instance_id": instance_id,
                        "active": self._instance_active.get(instance_id, 0),
                        "max": self._instance_max,
                    },
                )
            self._gateway_active += 1
            self._instance_active[instance_id] = (
                self._instance_active.get(instance_id, 0) + 1
            )

        acquired = True
        try:
            yield
        finally:
            if acquired:
                async with self._lock:
                    self._gateway_active -= 1
                    left = self._instance_active[instance_id] - 1
                    if left <= 0:
                        self._instance_active.pop(instance_id, None)
                    else:
                        self._instance_active[instance_id] = left

    def active_total(self) -> int:
        return self._gateway_active

    def active_for(self, instance_id: str) -> int:
        return self._instance_active.get(instance_id, 0)
