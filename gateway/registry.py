"""注册表：注册、注销、心跳续约、租约过期摘除与并发一致的快照读取。"""

from __future__ import annotations

import asyncio

from .clock import Clock, SystemClock
from .errors import (
    HeartbeatDrainingRejected,
    HeartbeatUnknown,
    InstanceNotFound,
    RegistrationConflict,
)
from .models import Instance, InstanceId


def _is_live(inst: Instance, now: float) -> bool:
    """在册且可被各组件看到：下线中实例豁免租约过期，直到被真正注销。"""
    return inst.draining or not inst.lease_expired(now)


class Registry:
    """以 InstanceId 为键的注册表，单把 asyncio.Lock 保证多任务一致性。"""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self._instances: dict[InstanceId, Instance] = {}
        self._lock = asyncio.Lock()
        # 注册成功后的联动钩子（如清除平滑下线墓碑）；由装配层注入。
        self.on_registered = None

    async def register(self, instance: Instance) -> bool:
        """注册新实例；重复注册抛 RegistrationConflict，不静默覆盖。"""
        async with self._lock:
            now = self._clock.now()
            existing = self._instances.get(instance.id)
            if existing is not None and existing.draining:
                raise RegistrationConflict(
                    f"实例正在平滑下线中，不能重新注册: {instance.id}",
                )
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
            if self.on_registered is not None:
                self.on_registered(instance.id)
            return True

    async def deregister(self, instance_id: InstanceId) -> Instance:
        """注销实例；不存在（含已过期被摘除）抛 InstanceNotFound。"""
        async with self._lock:
            instance = self._instances.pop(instance_id, None)
            if instance is None:
                raise InstanceNotFound(f"实例未注册或已摘除: {instance_id}")
            return instance

    async def heartbeat(self, instance_id: InstanceId, lease_seconds: float | None = None) -> float:
        """续约；实例未知抛 HeartbeatUnknown；租约已过期视为失效（已摘除），抛 HeartbeatUnknown。

        下线过程中的实例拒绝续约（抛 HeartbeatDrainingRejected）：
        下线宣告不可被心跳推翻，避免“已下线又复活”的矛盾状态。
        """
        async with self._lock:
            instance = self._instances.get(instance_id)
            now = self._clock.now()
            if instance is None:
                raise HeartbeatUnknown(f"续约失败，实例不存在: {instance_id}")
            if instance.draining:
                raise HeartbeatDrainingRejected(
                    f"续约拒绝，实例正在平滑下线中: {instance_id}",
                )
            if instance.lease_expired(now):
                # 过期实例不得复活，摘除后告知调用方需重新注册。
                self._instances.pop(instance_id, None)
                raise HeartbeatUnknown(f"续约失败，租约已过期: {instance_id}")
            if lease_seconds is not None:
                instance.lease_seconds = lease_seconds
            instance.last_heartbeat = now
            return instance.expires_at

    async def mark_draining(self, instance_id: InstanceId) -> Instance:
        """把在册且租约有效（或尚未开始下线）的实例标记为平滑下线中；返回该实例。"""
        async with self._lock:
            instance = self._instances.get(instance_id)
            now = self._clock.now()
            if instance is None or not _is_live(instance, now):
                raise InstanceNotFound(f"实例未注册或已摘除: {instance_id}")
            instance.draining = True
            return instance

    async def is_draining(self, instance_id: InstanceId) -> bool:
        async with self._lock:
            inst = self._instances.get(instance_id)
            return bool(inst is not None and inst.draining)

    async def expire_due(self) -> list[InstanceId]:
        """摘除所有租约过期实例，返回被摘除的实例 id。

        正在平滑下线的实例即使租约到期也不摘除：其生命周期交由下线流程，
        在在途请求归零后显式注销。
        """
        async with self._lock:
            now = self._clock.now()
            expired = [
                iid
                for iid, inst in self._instances.items()
                if not inst.draining and inst.lease_expired(now)
            ]
            for iid in expired:
                del self._instances[iid]
            return expired

    async def get(self, instance_id: InstanceId) -> Instance | None:
        async with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return None
            # 读取时同样校验过期，杜绝选到过期实例（下线中实例豁免）。
            if not _is_live(inst, self._clock.now()):
                return None
            return inst

    async def instances(self, service: str) -> list[Instance]:
        """返回该服务当前在册实例快照（剔除已过期；保留下线中实例以便观测/在途收尾）。"""
        async with self._lock:
            now = self._clock.now()
            return [
                inst
                for iid, inst in self._instances.items()
                if iid.service == service and _is_live(inst, now)
            ]

    async def all_services(self) -> list[str]:
        async with self._lock:
            now = self._clock.now()
            return sorted(
                {
                    iid.service
                    for iid, inst in self._instances.items()
                    if _is_live(inst, now)
                }
            )

    async def snapshot(self) -> dict[InstanceId, Instance]:
        async with self._lock:
            now = self._clock.now()
            return {
                iid: inst
                for iid, inst in self._instances.items()
                if _is_live(inst, now)
            }
