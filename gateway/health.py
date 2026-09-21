"""主动/被动健康检查。

- 被动：真实请求结果被记录，连续失败达阈值摘除；成功复位计数。
- 主动：周期性探测；抖动容忍（状态变更需连续达阈值），
  摘除后有恢复冷却，冷却期内不探测、不纳回，避免反复抖动。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from .errors import GatewayError
from .models import Instance, InstanceStatus

logger = logging.getLogger("gateway.health")


@dataclass
class HealthState:
    instance_id: str
    healthy: bool = True
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_failures: int = 0
    total_checks: int = 0
    last_reason: str = ""
    last_changed_at: float = 0.0
    last_probe_at: float = 0.0


class HealthChecker:
    def __init__(
        self,
        registry,
        upstream,
        fail_threshold: int = 3,
        recover_threshold: int = 2,
        probe_interval: float = 0.05,
        recover_cooldown: float = 0.2,
        check_healthy: bool = False,
        probe_timeout: float = 2.0,
        clock=time.monotonic,
    ) -> None:
        self._registry = registry
        self._upstream = upstream
        self._fail_threshold = fail_threshold
        self._recover_threshold = recover_threshold
        self._probe_interval = probe_interval
        self._cooldown = recover_cooldown
        self._check_healthy = check_healthy
        self._probe_timeout = probe_timeout
        self._clock = clock
        self._states: dict[str, HealthState] = {}
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    # ---- 状态读取 ----------------------------------------------------------

    def state_of(self, instance_id: str) -> HealthState:
        st = self._states.get(instance_id)
        if st is None:
            st = HealthState(instance_id=instance_id)
            self._states[instance_id] = st
        return st

    def is_available(self, inst: Instance) -> bool:
        """综合判定：在线、权重>0、租约未过期、未被健康摘除。"""
        if inst.status != InstanceStatus.ONLINE:
            return False
        if inst.weight <= 0:
            return False
        return self.state_of(inst.id).healthy

    # ---- 被动反馈 ----------------------------------------------------------

    async def record_passive(
        self, instance_id: str, ok: bool, reason: str = ""
    ) -> None:
        async with self._lock:
            inst = await self._registry.get(instance_id)
            if inst is None or inst.status == InstanceStatus.DEREGISTERED:
                return
            st = self.state_of(instance_id)
            st.total_checks += 1
            st.last_reason = reason
            if ok:
                st.consecutive_failures = 0
                st.consecutive_successes += 1
                self._maybe_recover(inst, st)
            else:
                st.total_failures += 1
                st.consecutive_successes = 0
                st.consecutive_failures += 1
                self._maybe_evict(inst, st)

    # ---- 主动探测 ----------------------------------------------------------

    async def probe_once(self, instance_id: str) -> bool:
        inst = await self._registry.get(instance_id)
        if inst is None:
            return False
        st = self.state_of(instance_id)
        now = self._clock()
        # 摘除后的恢复冷却：冷却期内不做探测，防止抖动反复
        if not st.healthy and now - st.last_changed_at < self._cooldown:
            return False

        ok = True
        reason = ""
        try:
            resp = await asyncio.wait_for(
                self._upstream.invoke(inst, _probe_request(inst)),
                timeout=self._probe_timeout,
            )
            ok = 200 <= resp.status < 400
            reason = f"http_{resp.status}" if not ok else "ok"
        except TimeoutError:
            ok, reason = False, "probe_timeout"
        except GatewayError as exc:
            ok, reason = False, exc.reason.value
        except Exception as exc:  # noqa: BLE001 - 探测内部异常归一化，不外泄
            ok, reason = False, f"internal:{type(exc).__name__}"

        async with self._lock:
            st.last_probe_at = self._clock()
            st.total_checks += 1
            st.last_reason = reason
            if ok:
                st.consecutive_failures = 0
                st.consecutive_successes += 1
                self._maybe_recover(inst, st)
            else:
                st.total_failures += 1
                st.consecutive_successes = 0
                st.consecutive_failures += 1
                self._maybe_evict(inst, st)
        logger.info(
            "probe instance=%s ok=%s reason=%s status=%s healthy=%s fails=%d succ=%d",
            instance_id,
            ok,
            reason,
            inst.status.value,
            st.healthy,
            st.consecutive_failures,
            st.consecutive_successes,
        )
        return ok

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._scan()
            except Exception as exc:  # noqa: BLE001 - 后台扫描必须存活
                logger.warning("health scan error: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self._probe_interval
                )
            except TimeoutError:
                pass

    async def _scan(self) -> None:
        for inst in await self._registry.snapshot():
            if inst.status == InstanceStatus.DEREGISTERED:
                continue
            st = self.state_of(inst.id)
            if st.healthy and not self._check_healthy:
                continue
            now = self._clock()
            if not st.healthy and now - st.last_changed_at < self._cooldown:
                continue
            await self.probe_once(inst.id)

    async def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    # ---- 持锁内部变更 ------------------------------------------------------

    def _maybe_evict(self, inst: Instance, st: HealthState) -> None:
        if st.healthy and st.consecutive_failures >= self._fail_threshold:
            st.healthy = False
            st.last_changed_at = self._clock()
            if inst.status == InstanceStatus.ONLINE:
                inst.status = InstanceStatus.SICK
            logger.info(
                "evict instance=%s reason=%s consecutive_failures=%d",
                inst.id,
                st.last_reason,
                st.consecutive_failures,
            )

    def _maybe_recover(self, inst: Instance, st: HealthState) -> None:
        if not st.healthy and st.consecutive_successes >= self._recover_threshold:
            # 只有实例本身仍在线且权重>0才恢复纳回
            if inst.status == InstanceStatus.SICK and inst.weight > 0:
                inst.status = InstanceStatus.ONLINE
            st.healthy = True
            st.consecutive_failures = 0
            st.last_changed_at = self._clock()
            logger.info(
                "recover instance=%s after %d successful checks",
                inst.id,
                st.consecutive_successes,
            )


def _probe_request(inst: Instance):
    from .models import Request

    return Request(path="/__healthprobe", method="GET", timeout=2.0)
