"""平滑下线：实例先停止接新流量，在途请求归零后才真正退出可服务集合。

状态流转：
    SERVING --start_drain--> DRAINING --在途归零--> DRAINED（终态，从注册表移除）

约定：
- DRAINING 中的实例不再被路由选中（路由层过滤），租约到期与心跳都不会让它
  "复活"或提前消失：注册表对它跳过过期摘除，心跳被明确拒绝（HEARTBEAT_DRAINING）。
- 在途计数来自连接池的真实占用；每次连接释放后调用 ``after_release`` 检查是否归零。
- 归零后执行 finalizer（注销注册表、清理健康/熔断/连接计数/会话绑定），
  finalizer 只执行一次，并发 ``after_release`` 不会重复触发。
"""

from __future__ import annotations

import threading
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable

from .clock import Clock, SystemClock
from .models import InstanceId

if TYPE_CHECKING:
    from .pool import ConnectionPool
    from .registry import Registry


class DrainState(str, Enum):
    SERVING = "SERVING"
    DRAINING = "DRAINING"
    DRAINED = "DRAINED"


class DrainResultCode(str, Enum):
    """平滑下线发起的可区分结果。"""

    DRAIN_STARTED = "DRAIN_STARTED"          # 进入下线流程，等待在途归零
    DRAIN_COMPLETED = "DRAIN_COMPLETED"      # 发起即在途为零，直接完成
    ALREADY_DRAINING = "ALREADY_DRAINING"    # 拒绝：下线进行中
    ALREADY_DRAINED = "ALREADY_DRAINED"      # 拒绝：此前已完成下线
    NOT_FOUND = "NOT_FOUND"                  # 拒绝：实例不存在或已过期


class DrainResult:
    """一次 start_drain 的明确结论，面向调用方可区分。"""

    def __init__(
        self,
        code: DrainResultCode,
        instance_id: InstanceId,
        *,
        in_flight: int = 0,
        message: str = "",
    ) -> None:
        self.code = code
        self.instance_id = instance_id
        self.in_flight = in_flight
        self.message = message

    @property
    def accepted(self) -> bool:
        """是否进入了（或已完成）下线流程；NOT_FOUND/重复发起为 False。"""
        return self.code in (
            DrainResultCode.DRAIN_STARTED,
            DrainResultCode.DRAIN_COMPLETED,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code.value,
            "instance": str(self.instance_id),
            "in_flight": self.in_flight,
            "message": self.message,
        }


class _DrainEntry:
    __slots__ = ("state", "started_at", "finished_at", "finalize_started")

    def __init__(self, now: float) -> None:
        self.state: DrainState = DrainState.DRAINING
        self.started_at: float = now
        self.finished_at: float | None = None
        self.finalize_started: bool = False


# finalizer 签名：async (instance_id) -> None，负责联动清理。
DrainFinalizer = Callable[[InstanceId], Awaitable[None]]


class DrainManager:
    """
    平滑下线协调器（threading.Lock 保护状态，finalizer 在锁外异步执行）。

    - ``start_drain``：幂等；对不存在/已过期实例返回 NOT_FOUND，
      对已在下线的实例返回 ALREADY_DRAINING，均与正常发起可区分。
    - ``after_release``：每次连接释放后调用；在途归零即触发一次性收尾。
    - ``is_draining``：路由过滤与注册表租约保护共用的判定。
    """

    def __init__(
        self,
        registry: "Registry",
        pool: "ConnectionPool",
        clock: Clock | None = None,
        *,
        finalizer: DrainFinalizer | None = None,
    ) -> None:
        self._registry = registry
        self._pool = pool
        self._clock = clock or SystemClock()
        self._finalizer = finalizer
        self._entries: dict[InstanceId, _DrainEntry] = {}
        self._lock = threading.Lock()

    def set_finalizer(self, finalizer: DrainFinalizer) -> None:
        self._finalizer = finalizer

    async def start_drain(self, instance_id: InstanceId) -> DrainResult:
        """发起平滑下线；并发重复发起只有一个会真正进入下线流程。"""
        with self._lock:
            entry = self._entries.get(instance_id)
            if entry is not None:
                if entry.state == DrainState.DRAINING:
                    return DrainResult(
                        DrainResultCode.ALREADY_DRAINING,
                        instance_id,
                        in_flight=self._pool.active_of(instance_id),
                        message="实例已在平滑下线中",
                    )
                return DrainResult(
                    DrainResultCode.ALREADY_DRAINED,
                    instance_id,
                    in_flight=0,
                    message="实例此前已完成下线，重复发起被拒绝",
                )
            entry = _DrainEntry(self._clock.now())
            self._entries[instance_id] = entry
        # 占位后再校验在册：不存在/已过期的实例给出 NOT_FOUND 并撤销占位。
        inst = await self._registry.get(instance_id)
        if inst is None:
            with self._lock:
                self._entries.pop(instance_id, None)
            return DrainResult(
                DrainResultCode.NOT_FOUND,
                instance_id,
                message="实例不存在或已过期，无法发起下线",
            )
        in_flight = self._pool.active_of(instance_id)
        if in_flight > 0:
            return DrainResult(
                DrainResultCode.DRAIN_STARTED,
                instance_id,
                in_flight=in_flight,
                message=f"已进入下线流程，等待 {in_flight} 个在途请求完成",
            )
        # 发起即在途为零：直接完成下线（finalizer 在锁外执行）。
        await self._finalize(instance_id, entry)
        return DrainResult(
            DrainResultCode.DRAIN_COMPLETED,
            instance_id,
            in_flight=0,
            message="无在途请求，实例已直接完成下线",
        )

    async def after_release(self, instance_id: InstanceId) -> bool:
        """连接释放后回调：在途归零则完成下线。返回本次是否触发了收尾。"""
        with self._lock:
            entry = self._entries.get(instance_id)
            if (
                entry is None
                or entry.state != DrainState.DRAINING
                or entry.finalize_started
            ):
                return False
            if self._pool.active_of(instance_id) > 0:
                return False
            entry.finalize_started = True
        await self._finalize(instance_id, entry)
        return True

    async def _finalize(self, instance_id: InstanceId, entry: _DrainEntry) -> None:
        with self._lock:
            if entry.state == DrainState.DRAINED:
                return
            entry.state = DrainState.DRAINED
            entry.finished_at = self._clock.now()
        if self._finalizer is not None:
            await self._finalizer(instance_id)

    def is_draining(self, instance_id: InstanceId) -> bool:
        """是否处于下线流程中（含已完成）；路由与注册表共用的排除判定。"""
        with self._lock:
            return instance_id in self._entries

    def state_of(self, instance_id: InstanceId) -> DrainState:
        with self._lock:
            entry = self._entries.get(instance_id)
            return entry.state if entry is not None else DrainState.SERVING

    def explain(self, instance_id: InstanceId) -> dict[str, object]:
        with self._lock:
            entry = self._entries.get(instance_id)
            if entry is None:
                return {"state": DrainState.SERVING.value, "in_flight": self._pool.active_of(instance_id)}
            return {
                "state": entry.state.value,
                "started_at": entry.started_at,
                "finished_at": entry.finished_at,
                "in_flight": self._pool.active_of(instance_id),
            }

    def remove(self, instance_id: InstanceId) -> None:
        """实例被显式注销/租约摘除时清理下线记录。"""
        with self._lock:
            self._entries.pop(instance_id, None)
