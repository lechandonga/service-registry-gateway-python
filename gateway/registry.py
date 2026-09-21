"""服务注册表：注册 / 注销 / 心跳续约 / 租约过期。

并发安全：所有变更在 asyncio.Lock 下进行；快照读取基于一致性视图。
重复注册/重复注销必须给出可区分结果，而不是静默覆盖。
"""

from __future__ import annotations

import asyncio
import logging
import time

from .errors import ErrorType, GatewayError
from .models import Instance, InstanceStatus

logger = logging.getLogger("gateway.registry")


class ServiceRegistry:
    def __init__(self, clock=time.monotonic) -> None:
        self._instances: dict[str, Instance] = {}
        self._lock = asyncio.Lock()
        self._clock = clock
        self._reaper: asyncio.Task | None = None

    async def register(self, inst: Instance) -> bool:
        """注册新实例。重复注册抛 REGISTRY_DUPLICATE（不静默覆盖）。

        判定“重复”：存在同 id 且未被注销/未过期摘除的实例。
        对已显式注销或租约过期的 id，允许重新注册。
        """
        now = self._clock()
        async with self._lock:
            old = self._instances.get(inst.id)
            if old is not None and old.status in (
                InstanceStatus.ONLINE,
                InstanceStatus.SICK,
                InstanceStatus.OFFLINE,
            ):
                raise GatewayError(
                    ErrorType.REGISTRY_DUPLICATE,
                    f"instance {inst.id} already registered",
                    detail={"instance_id": inst.id, "current_status": old.status.value},
                )
            inst.registered_at = now
            inst.last_heartbeat = now
            if inst.weight <= 0:
                inst.status = InstanceStatus.OFFLINE
                inst.weight = 0
            else:
                inst.status = InstanceStatus.ONLINE
            self._instances[inst.id] = inst
            return True

    async def deregister(self, instance_id: str) -> bool:
        """注销。不存在或已注销抛 REGISTRY_NOT_FOUND（可区分）。"""
        async with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.status == InstanceStatus.DEREGISTERED:
                raise GatewayError(
                    ErrorType.REGISTRY_NOT_FOUND,
                    f"instance {instance_id} not registered",
                    detail={"instance_id": instance_id, "op": "deregister"},
                )
            inst.status = InstanceStatus.DEREGISTERED
            return True

    async def heartbeat(
        self, instance_id: str, lease_seconds: float | None = None
    ) -> float:
        """续约，返回新的过期时间点。已注销/不存在抛 REGISTRY_NOT_FOUND。"""
        now = self._clock()
        async with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.status == InstanceStatus.DEREGISTERED:
                raise GatewayError(
                    ErrorType.REGISTRY_NOT_FOUND,
                    f"instance {instance_id} not registered",
                    detail={"instance_id": instance_id, "op": "heartbeat"},
                )
            if lease_seconds is not None:
                inst.lease_seconds = lease_seconds
            inst.last_heartbeat = now
            # 续约不会把 SICK/OFFLINE 自动改回 ONLINE；只刷新租约。
            return now + inst.lease_seconds

    async def get(self, instance_id: str) -> Instance | None:
        async with self._lock:
            return self._instances.get(instance_id)

    async def snapshot(self) -> list[Instance]:
        """返回当前实例的一致性快照（已应用租约过期判定）。

        过期实例被标记为 EXPIRED，不再出现在可路由集合中。
        """
        now = self._clock()
        async with self._lock:
            self._expire_locked(now)
            return list(self._instances.values())

    def expire_now(self) -> list[str]:
        """供测试/后台任务同步触发一次过期扫描。"""
        return self._expire_locked(self._clock())

    async def start_reaper(self, interval: float = 0.05) -> None:
        """启动后台租约扫描，无流量时也能主动摘除过期实例。"""
        self._reaper = asyncio.create_task(self._reap_loop(interval))

    async def stop_reaper(self) -> None:
        task = getattr(self, "_reaper", None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._reaper = None

    async def _reap_loop(self, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                async with self._lock:
                    expired = self._expire_locked(self._clock())
                if expired:
                    logger.info("lease reaper expired instances: %s", expired)
        except asyncio.CancelledError:
            return

    def _expire_locked(self, now: float) -> list[str]:
        """在持锁状态下把过期实例标记为 EXPIRED，返回受影响 id。"""
        expired: list[str] = []
        for inst in self._instances.values():
            if (
                inst.status
                in (InstanceStatus.ONLINE, InstanceStatus.SICK, InstanceStatus.OFFLINE)
                and now - inst.last_heartbeat > inst.lease_seconds
            ):
                inst.status = InstanceStatus.EXPIRED
                expired.append(inst.id)
        return expired
