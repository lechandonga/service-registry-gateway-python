"""连接/并发计数与上限：租约式申请，异常路径保证不漏减、不重复扣减。"""

from __future__ import annotations

import asyncio

from .models import InstanceId


class ConnectionLease:
    """一次连接占用；release 幂等，重复释放不会重复扣减。"""

    def __init__(self, pool: "ConnectionPool", instance_id: InstanceId) -> None:
        self._pool = pool
        self._instance_id = instance_id
        self._released = False

    @property
    def released(self) -> bool:
        return self._released

    @property
    def instance_id(self) -> InstanceId:
        return self._instance_id

    def release(self) -> bool:
        if self._released:
            return False
        self._released = True
        self._pool._release(self._instance_id)
        return True

    async def __aenter__(self) -> "ConnectionLease":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.release()


class ConnectionPool:
    """
    每上游连接数 + 全局连接数，均在同一把 asyncio.Lock 下变更，
    申请失败（超上限）在抛错前不做任何加计数，杜绝半更新状态。
    """

    def __init__(self, max_per_upstream: int, max_total: int | None = None) -> None:
        self._max_per = max_per_upstream
        self._max_total = max_total
        self._per: dict[InstanceId, int] = {}
        self._total = 0
        self._lock = asyncio.Lock()

    async def acquire(self, instance_id: InstanceId) -> ConnectionLease:
        """申请连接；超出上游或全局上限抛 ConcurrencyLimitReached（无副作用）。"""
        from .errors import ConcurrencyLimitReached

        async with self._lock:
            current = self._per.get(instance_id, 0)
            if current >= self._max_per:
                raise ConcurrencyLimitReached(
                    f"上游 {instance_id} 并发 {current} 已达上限 {self._max_per}",
                )
            if self._max_total is not None and self._total >= self._max_total:
                raise ConcurrencyLimitReached(
                    f"网关全局并发 {self._total} 已达上限 {self._max_total}",
                )
            # 只有全部检查通过后才提交计数，保证原子性。
            self._per[instance_id] = current + 1
            self._total += 1
            return ConnectionLease(self, instance_id)

    def _release(self, instance_id: InstanceId) -> None:
        # 由 ConnectionLease.release 幂等保证只进一次；计数不会为负。
        current = self._per.get(instance_id, 0)
        if current <= 0:
            raise RuntimeError(f"连接计数异常：释放未占用的上游 {instance_id}")
        self._per[instance_id] = current - 1
        if self._total <= 0:
            raise RuntimeError("连接计数异常：全局计数为负")
        self._total -= 1

    def active_of(self, instance_id: InstanceId) -> int:
        return self._per.get(instance_id, 0)

    def active_total(self) -> int:
        return self._total

    def remove(self, instance_id: InstanceId) -> int:
        """摘除实例时清退其计数（由注销路径调用），返回清退数量。"""
        count = self._per.pop(instance_id, 0)
        self._total = max(0, self._total - count)
        return count
