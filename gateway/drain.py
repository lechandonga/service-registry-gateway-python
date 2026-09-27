"""平滑下线：宣告后不再接新流量，在途请求跑完、计数归零后才真正退出。"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from enum import Enum

from .clock import Clock, SystemClock
from .errors import InstanceExiting, InstanceNotFound
from .models import InstanceId


class DrainStatus(str, Enum):
    """begin_drain 面向调用方的明确结果（与“正常下线成功”可区分）。"""

    DRAINING_STARTED = "DRAINING_STARTED"   # 正常受理：开始平滑下线
    ALREADY_DRAINING = "ALREADY_DRAINING"   # 重复宣告：已在下线过程中
    NOT_FOUND = "NOT_FOUND"                 # 实例不存在 / 已退出 / 已过期


@dataclass
class DrainResult:
    status: DrainStatus
    instance_id: InstanceId
    already_active: int
    message: str
    deregistered: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "instance": str(self.instance_id),
            "in_flight": self.already_active,
            "deregistered": self.deregistered,
            "message": self.message,
        }


class DrainCoordinator:
    """
    协调“注册表标记 -> 连接池门禁 -> 在途归零 -> 真正注销”的平滑下线流程。

    并发约定：
    - DRAINING 集合由 threading.Lock 保护，供同步路径（路由/连接池门禁）快速判定，
      与 begin/最终注销的 asyncio 锁协同保证：begin 提交 draining 标记后，池门禁在
      同一把池锁内复查，关闭“选路后才宣告下线”的窗口；
    - begin 与最终注销由 asyncio.Lock 串行化；
    - drain 期间注册表把实例豁免租约过期且拒绝续约（避免“已宣告下线又复活”）。
    """

    def __init__(
        self,
        registry,
        pool,
        *,
        health=None,
        circuit=None,
        binder=None,
        clock: Clock | None = None,
        max_tombstones: int = 1024,
        logger: logging.Logger | None = None,
    ) -> None:
        self._registry = registry
        self._pool = pool
        self._health = health
        self._circuit = circuit
        self._binder = binder
        self._clock = clock or SystemClock()
        self._max_tombstones = max_tombstones
        self._logger = logger or logging.getLogger("gateway.drain")
        self._draining: set[InstanceId] = set()
        # 已真正退出的实例墓碑：用于续约等场景给出确定结论，不作为选路依据。
        self._drained: set[InstanceId] = set()
        self._lock = threading.Lock()
        self._async_lock = asyncio.Lock()

    # ---- 平滑下线受理 -------------------------------------------------------
    async def begin_drain(self, instance_id: InstanceId) -> DrainResult:
        """宣告平滑下线，返回与“正常下线成功”可区分的明确结果。"""
        async with self._async_lock:
            # 已退出（正常下线完成/此前注销/过期）：明确可区分的 NOT_FOUND。
            with self._lock:
                already_drained = instance_id in self._drained
            if already_drained:
                self._logger.info("平滑下线被拒绝（已退出）instance=%s", instance_id)
                return DrainResult(
                    DrainStatus.NOT_FOUND, instance_id, 0,
                    "实例已退出可服务集合（此前已完成下线或注销）",
                )
            instance = await self._registry.get(instance_id)
            if instance is None:
                self._logger.info("平滑下线被拒绝（不存在/已过期）instance=%s", instance_id)
                return DrainResult(
                    DrainStatus.NOT_FOUND, instance_id, 0,
                    "实例未注册或已过期摘除",
                )
            # 重复宣告：状态幂等且结论可区分，不重复扰动在途计数。
            with self._lock:
                if instance_id in self._draining:
                    active = self._pool.active_of(instance_id)
                    self._logger.info(
                        "平滑下线重复宣告（幂等）instance=%s 在途=%d", instance_id, active,
                    )
                    return DrainResult(
                        DrainStatus.ALREADY_DRAINING, instance_id, active,
                        "实例已在平滑下线过程中",
                    )
            # 先提交标记，再标记注册表（池 acquire 持锁时也会复查门禁）。
            self._draining.add(instance_id)

            try:
                await self._registry.mark_draining(instance_id)
            except InstanceNotFound:
                # 极端竞态：拿到快照后、标记前实例被并发主动注销。回滚标记，
                # 给出与正常受理可区分的 NOT_FOUND，而不是透出 500。
                with self._lock:
                    self._draining.discard(instance_id)
                    self._drained.add(instance_id)
                self._logger.info("下线宣告时实例已被并发注销 instance=%s", instance_id)
                return DrainResult(
                    DrainStatus.NOT_FOUND, instance_id, 0,
                    "宣告下线时实例已被并发注销",
                )
            active = self._pool.active_of(instance_id)
            self._logger.info(
                "平滑下线开始 instance=%s 在途请求=%d：新流量将被拒绝，等待在途收尾",
                instance_id, active,
            )
            deregistered = False
            if active == 0:
                deregistered = await self._finalize_locked(instance_id)
                message = "无在途请求，实例已立即退出"
            else:
                message = f"等待 {active} 个在途请求完成后退出"
            return DrainResult(
                DrainStatus.DRAINING_STARTED, instance_id, active, message, deregistered,
            )

    # ---- 重新注册复位 -------------------------------------------------------
    def reset_for_registration(self, instance_id: InstanceId) -> None:
        """同地址实例重新注册时清除下线墓碑，避免陈旧退出标记误拒新实例。"""
        with self._lock:
            self._draining.discard(instance_id)
            self._drained.discard(instance_id)

    # ---- 退出判定（同步快速路径，供路由与连接池门禁调用） --------------------
    def is_draining(self, instance_id: InstanceId) -> bool:
        with self._lock:
            return instance_id in self._draining

    def is_exiting(self, instance_id: InstanceId) -> bool:
        with self._lock:
            return instance_id in self._draining or instance_id in self._drained

    def reject_if_exiting(self, instance_id: InstanceId) -> None:
        """连接池门禁：实例已宣告退出则拒绝新计数（在池锁内调用，无副作用）。"""
        with self._lock:
            draining = instance_id in self._draining
            drained = instance_id in self._drained
        if draining:
            raise InstanceExiting(f"实例平滑下线中，拒绝新连接: {instance_id}")
        if drained:
            raise InstanceExiting(f"实例已退出，拒绝新连接: {instance_id}")

    # ---- 在途收尾与最终注销 -------------------------------------------------
    async def on_connection_released(self, instance_id: InstanceId) -> bool:
        """连接租约释放后回调：若实例在下线中且在途归零，则完成最终注销。"""
        if not self.is_draining(instance_id):
            return False
        async with self._async_lock:
            if not self.is_draining(instance_id):
                return False
            if self._pool.active_of(instance_id) > 0:
                return False
            return await self._finalize_locked(instance_id)

    async def _finalize_locked(self, instance_id: InstanceId) -> bool:
        """真正退出：注册表注销 + 健康/熔断/连接/会话绑定清理。幂等。"""
        with self._lock:
            if instance_id not in self._draining:
                return False
        try:
            await self._registry.deregister(instance_id)
        except InstanceNotFound:
            # 并发下已被过期/注销路径清理，仍需完成本地状态与绑定清理。
            self._logger.info("最终注销时实例已不在注册表 instance=%s", instance_id)
        removed_conns = self._pool.remove(instance_id)
        if self._health is not None:
            self._health.remove(instance_id)
        if self._circuit is not None:
            self._circuit.remove(instance_id)
        unbound = 0
        if self._binder is not None:
            unbound = self._binder.unbind_instance(instance_id)
        with self._lock:
            self._draining.discard(instance_id)
            self._drained.add(instance_id)
            if len(self._drained) > self._max_tombstones:
                # 简单限制墓碑规模：墓碑仅用于可解释结论，不影响正确性。
                self._drained = set(sorted(self._drained)[-self._max_tombstones:])
        self._logger.info(
            "实例平滑退出完成 instance=%s 清理连接=%d 清理会话绑定=%d",
            instance_id, removed_conns, unbound,
        )
        return True

    async def cleanup_exited(self, instance_id: InstanceId) -> None:
        """非下线路径的实例退出（主动注销/租约过期）联动清理。幂等。"""
        removed_conns = self._pool.remove(instance_id)
        if self._health is not None:
            self._health.remove(instance_id)
        if self._circuit is not None:
            self._circuit.remove(instance_id)
        unbound = 0
        if self._binder is not None:
            unbound = self._binder.unbind_instance(instance_id)
        with self._lock:
            self._draining.discard(instance_id)
            self._drained.add(instance_id)
            if len(self._drained) > self._max_tombstones:
                self._drained = set(sorted(self._drained)[-self._max_tombstones:])
        if removed_conns or unbound:
            self._logger.info(
                "实例退出联动清理 instance=%s 连接=%d 会话绑定=%d",
                instance_id, removed_conns, unbound,
            )

    # ---- 观测 ---------------------------------------------------------------
    def state_of(self, instance_id: InstanceId) -> str:
        with self._lock:
            if instance_id in self._draining:
                return "DRAINING"
            if instance_id in self._drained:
                return "DRAINED"
        return "SERVING"

    def explain(self, instance_id: InstanceId) -> dict[str, object]:
        state = self.state_of(instance_id)
        info: dict[str, object] = {
            "drain_state": state,
            "in_flight": self._pool.active_of(instance_id),
        }
        if state == "DRAINING":
            info["reason"] = "平滑下线中：不接新流量，等待在途请求归零"
        elif state == "DRAINED":
            info["reason"] = "已完成平滑下线并退出可服务集合"
        else:
            info["reason"] = "正常服务"
        return info

    def draining_ids(self) -> list[InstanceId]:
        with self._lock:
            return sorted(self._draining, key=str)
