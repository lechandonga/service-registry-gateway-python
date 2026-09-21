"""注册表：注册、注销、心跳续约、租约过期摘除与并发一致的快照读取。"""

from __future__ import annotations

import asyncio

from .clock import Clock, SystemClock
from .errors import HeartbeatUnknown, InstanceNotFound, RegistrationConflict
from .models import Instance, InstanceId


class Registry:
    """以 InstanceId 为键的注册表，单把 asyncio.Lock 保证多任务一致性。"""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self._instances: dict[InstanceId, Instance] = {}
        self._lock = asyncio.Lock()

    async def register(self, instance: Instance) -> bool:
        """注册新实例；重复注册抛 RegistrationConflict，不静默覆盖。"""
        async with self._lock:
            now = self._clock.now()
            existing = self._instances.get(instance.id)
            if existing is not None and not existing.lease_expired(now):
                raise RegistrationConflict(
                    f"实例已存在且租约有效: {instance.id}",
                )
            # 已过期残留（理论上 expire_due 会清理）也拒绝静默覆盖，显式冲突。
            if existing is not None:
                raise RegistrationConflict(
                    f"实例处于过期残留状态，需先注销: {instance.id}",
                )
            self._instances[instance.id] = instance
            return True

    async def deregister(self, instance_id: InstanceId) -> Instance:
        """注销实例；不存在（含已过期被摘除）抛 InstanceNotFound。"""
        async with self._lock:
            instance = self._instances.pop(instance_id, None)
            if instance is None:
                raise InstanceNotFound(f"实例未注册或已摘除: {instance_id}")
            return instance

    async def heartbeat(self, instance_id: InstanceId, lease_seconds: float | None = None) -> float:
        """续约；实例未知抛 HeartbeatUnknown；租约已过期视为失效（已摘除），抛 HeartbeatUnknown。"""
        async with self._lock:
            instance = self._instances.get(instance_id)
            now = self._clock.now()
            if instance is None:
                raise HeartbeatUnknown(f"续约失败，实例不存在: {instance_id}")
            if instance.lease_expired(now):
                # 过期实例不得复活，摘除后告知调用方需重新注册。
                self._instances.pop(instance_id, None)
                raise HeartbeatUnknown(f"续约失败，租约已过期: {instance_id}")
            if lease_seconds is not None:
                instance.lease_seconds = lease_seconds
            instance.last_heartbeat = now
            return instance.expires_at

    async def expire_due(self) -> list[InstanceId]:
        """摘除所有租约过期实例，返回被摘除的实例 id。"""
        async with self._lock:
            now = self._clock.now()
            expired = [
                iid for iid, inst in self._instances.items() if inst.lease_expired(now)
            ]
            for iid in expired:
                del self._instances[iid]
            return expired

    async def get(self, instance_id: InstanceId) -> Instance | None:
        async with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return None
            # 读取时同样校验过期，杜绝选到过期实例。
            if inst.lease_expired(self._clock.now()):
                return None
            return inst

    async def instances(self, service: str) -> list[Instance]:
        """返回该服务当前（已剔除过期的）实例快照。"""
        async with self._lock:
            now = self._clock.now()
            return [
                inst
                for iid, inst in self._instances.items()
                if iid.service == service and not inst.lease_expired(now)
            ]

    async def all_services(self) -> list[str]:
        async with self._lock:
            now = self._clock.now()
            return sorted(
                {
                    iid.service
                    for iid, inst in self._instances.items()
                    if not inst.lease_expired(now)
                }
            )

    async def snapshot(self) -> dict[InstanceId, Instance]:
        async with self._lock:
            now = self._clock.now()
            return {
                iid: inst
                for iid, inst in self._instances.items()
                if not inst.lease_expired(now)
            }
