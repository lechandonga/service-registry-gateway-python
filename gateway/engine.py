"""网关编排：选路（含会话粘性）-> 熔断 -> 连接上限 -> 超时调用 -> 健康/熔断反馈 -> 重试。"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .circuit import CircuitBreaker
from .errors import (
    CircuitOpen,
    ConcurrencyLimitReached,
    GatewayError,
    InstanceExiting,
    RetriesExhausted,
)
from .health import HealthMonitor
from .models import InstanceId, Request, Response
from .pool import ConnectionPool
from .retry import retry_allowed
from .router import Router
from .sticky import StickyDecision
from .transport import UpstreamTransport


@dataclass
class DispatchResult:
    response: Response | None
    attempts: list[dict[str, object]] = field(default_factory=list)
    error_code: str | None = None


class Gateway:
    """
    单次 dispatch 中每一次尝试：
      选实例（粘性/过滤依据，排除本请求已失败的实例） -> 熔断 allow（OPEN 快速失败）
      -> 连接池 acquire（退出门禁 + 超限背压拒绝，无副作用）
      -> transport.invoke（超时控制，异常归一化）
      -> 成功：粘性绑定提交 + 健康/熔断记成功；失败：健康/熔断记失败
      -> 连接租约在 finally 释放一次；释放后通知下线协调器判定在途归零。
    仅可重试失败才换“未失败过”的实例重试；重试不会再落在同一个失败实例上。
    """

    def __init__(
        self,
        router: Router,
        transport: UpstreamTransport,
        health: HealthMonitor,
        circuit: CircuitBreaker,
        pool: ConnectionPool,
        *,
        max_retries: int = 2,
        upstream_timeout: float = 5.0,
        binder=None,
        drain_coordinator=None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._router = router
        self._transport = transport
        self._health = health
        self._circuit = circuit
        self._pool = pool
        self._binder = binder
        self._drain = drain_coordinator
        self._max_retries = max_retries
        self._upstream_timeout = upstream_timeout
        self._logger = logger or logging.getLogger("gateway.engine")

    async def dispatch(self, request: Request) -> Response:
        attempts: list[dict[str, object]] = []
        last_error: GatewayError | None = None
        total_attempts = self._max_retries + 1
        # 已尝试失败的实例：重试时一律排除，杜绝反复落在同一失败实例上。
        excluded: set[InstanceId] = set()

        for attempt_no in range(1, total_attempts + 1):
            record: dict[str, object] = {"attempt": attempt_no}
            decision: StickyDecision | None = None
            try:
                rule, reasons, decision, instance = await self._router.select_with_session(
                    request, exclude=frozenset(excluded),
                )
                record["upstream"] = str(instance.id)
                record["rule"] = rule.service
                record["sticky"] = decision.to_dict()
            except GatewayError as exc:
                record["error"] = exc.code.value
                record["message"] = exc.message
                attempts.append(record)
                self._log_attempt(request, record, reasons=None)
                # 重试期间因已摘故障实例而选不到时，保留首次尝试的真实根因，
                # 避免 ALL_UPSTREAMS_DOWN / 全排除 掩盖上游超时/不可达等原因。
                if last_error is not None:
                    record["masked_by"] = exc.code.value
                    raise last_error
                last_error = exc
                if (
                    retry_allowed(request, exc.code)
                    and attempt_no < total_attempts
                ):
                    continue
                raise

            try:
                state = self._circuit.allow(instance.id)
                record["circuit"] = state.value
            except CircuitOpen as exc:
                record["error"] = exc.code.value
                record["message"] = exc.message
                attempts.append(record)
                self._log_attempt(request, record, reasons)
                last_error = exc
                excluded.add(instance.id)
                if retry_allowed(request, exc.code) and attempt_no < total_attempts:
                    continue
                raise

            lease = None
            try:
                try:
                    lease = await self._pool.acquire(instance.id)
                except InstanceExiting as exc:
                    # 平滑下线并发窗口：尚未调用上游，换实例重试（POST 也安全）。
                    record["error"] = exc.code.value
                    record["message"] = exc.message
                    attempts.append(record)
                    self._log_attempt(request, record, reasons)
                    last_error = exc
                    excluded.add(instance.id)
                    if retry_allowed(request, exc.code) and attempt_no < total_attempts:
                        continue
                    raise
                except ConcurrencyLimitReached as exc:
                    record["error"] = exc.code.value
                    record["message"] = exc.message
                    attempts.append(record)
                    self._log_attempt(request, record, reasons)
                    last_error = exc
                    if retry_allowed(request, exc.code) and attempt_no < total_attempts:
                        continue
                    raise

                try:
                    response = await self._transport.invoke(
                        instance, request, timeout=self._upstream_timeout,
                    )
                except GatewayError as exc:
                    # 反馈被动健康与熔断（连接获取后失败，计数将在 finally 释放）。
                    self._health.record_failure(instance.id)
                    self._circuit.record_failure(instance.id)
                    record["error"] = exc.code.value
                    record["message"] = exc.message
                    attempts.append(record)
                    self._log_attempt(request, record, reasons)
                    last_error = exc
                    excluded.add(instance.id)
                    if retry_allowed(request, exc.code) and attempt_no < total_attempts:
                        continue
                    raise
                else:
                    self._health.record_success(instance.id)
                    self._circuit.record_success(instance.id)
                    # 请求成功后才提交粘性：会话稳定粘在“确实服务成功”的实例上；
                    # 命中绑定时同时完成滑动续期。
                    if self._binder is not None and decision is not None and decision.valid:
                        self._binder.bind(
                            instance.id.service, decision.session_id, instance.id,
                        )
                    record["status"] = response.status
                    attempts.append(record)
                    self._log_attempt(request, record, reasons)
                    return response
            finally:
                # 无论成功失败、是否重试，连接计数必须精确释放一次。
                if lease is not None:
                    lease.release()
                    # 释放后判定在途归零：若实例正在下线，此处完成其最终退出。
                    if self._drain is not None:
                        await self._drain.on_connection_released(instance.id)

        # 理论不可达：循环内必 return 或 raise。
        raise RetriesExhausted(
            f"重试 {self._max_retries} 次后仍失败: "
            f"{last_error.code.value if last_error else 'UNKNOWN'}",
        )

    def _log_attempt(
        self,
        request: Request,
        record: dict[str, object],
        reasons: list[dict[str, object]] | None,
    ) -> None:
        self._logger.info(
            "dispatch 判定 method=%s path=%s record=%s candidates=%s",
            request.method,
            request.path,
            record,
            reasons,
        )
