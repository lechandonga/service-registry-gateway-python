"""网关编排：选路(含粘性) -> 熔断 -> 连接上限 -> 超时调用 -> 健康/熔断反馈 -> 重试。"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .circuit import CircuitBreaker, CircuitState
from .errors import (
    CircuitOpen,
    ConcurrencyLimitReached,
    GatewayError,
    RetriesExhausted,
)
from .health import HealthMonitor
from .models import Instance, InstanceId, Request, Response
from .pool import ConnectionPool
from .retry import is_retryable_failure, is_safe_to_retry
from .router import Router, SelectionContext
from .transport import UpstreamTransport


@dataclass
class DispatchResult:
    response: Response | None
    attempts: list[dict[str, object]] = field(default_factory=list)
    error_code: str | None = None


class Gateway:
    """
    单次 dispatch 中每一次尝试：
      选实例（含粘性/排除已失败实例） -> 熔断 allow（OPEN 快速失败）
      -> 连接池 acquire（超限背压拒绝，无副作用）
      -> 下线复核（获取连接后实例进入下线则释放并立即重选）
      -> transport.invoke（超时控制，异常归一化）
      -> 成功：健康/熔断记成功；失败：健康/熔断记失败，实例加入本次排除集
      -> 连接租约在 finally 释放并回调下线检查；仅幂等安全且失败可重试时换实例重试。
    重试次数耗尽后以最后一次失败原因抛出 RetriesExhausted（携带根因编码）。
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
        drain=None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._router = router
        self._transport = transport
        self._health = health
        self._circuit = circuit
        self._pool = pool
        self._max_retries = max_retries
        self._upstream_timeout = upstream_timeout
        self._drain = drain
        self._logger = logger or logging.getLogger("gateway.engine")

    async def dispatch(self, request: Request) -> Response:
        attempts: list[dict[str, object]] = []
        last_error: GatewayError | None = None
        total_attempts = self._max_retries + 1
        safe = is_safe_to_retry(request)
        # 同一次请求内已失败/已排除的实例：重试不再选中，杜绝反复落点。
        excluded: set[InstanceId] = set()
        ctx = SelectionContext()

        attempt_no = 0
        while attempt_no < total_attempts:
            attempt_no += 1
            record: dict[str, object] = {"attempt": attempt_no}
            try:
                rule, reasons, instance = await self._router.select(
                    request, ctx=ctx, excluded=frozenset(excluded),
                )
                record["upstream"] = str(instance.id)
                record["rule"] = rule.service
                if ctx.sticky:
                    record["sticky"] = True
                if ctx.switched:
                    record["switched"] = True
                    record["switch_reason"] = ctx.switch_reason
                # 每次选路的粘性/切换标记只属于本次尝试，读后重置，
                # 避免污染下一次重试的判定记录。
                ctx.sticky = False
                ctx.switched = False
                ctx.switch_reason = None
            except GatewayError as exc:
                # 选路本身失败（无规则/无实例/全不可用）。
                record["error"] = exc.code.value
                record["message"] = exc.message
                attempts.append(record)
                self._log_attempt(request, record, reasons=None, ctx=ctx)
                # 重试期间因已摘故障实例而选不到时，保留首次尝试的真实根因，
                # 避免 ALL_UPSTREAMS_DOWN 掩盖上游超时/不可达等原因。
                if last_error is not None:
                    record["masked_by"] = exc.code.value
                    raise last_error
                last_error = exc
                if safe and is_retryable_failure(exc.code) and attempt_no < total_attempts:
                    continue
                raise

            try:
                state = self._circuit.allow(instance.id)
                record["circuit"] = state.value
            except CircuitOpen as exc:
                record["error"] = exc.code.value
                record["message"] = exc.message
                attempts.append(record)
                self._log_attempt(request, record, reasons, ctx)
                last_error = exc
                excluded.add(instance.id)
                if safe and is_retryable_failure(exc.code) and attempt_no < total_attempts:
                    # 熔断打开通常整个服务不可用，但仍尝试其他实例。
                    continue
                raise

            lease = None
            try:
                try:
                    lease = await self._pool.acquire(instance.id)
                except ConcurrencyLimitReached as exc:
                    record["error"] = exc.code.value
                    record["message"] = exc.message
                    attempts.append(record)
                    self._log_attempt(request, record, reasons, ctx)
                    last_error = exc
                    excluded.add(instance.id)
                    if safe and is_retryable_failure(exc.code) and attempt_no < total_attempts:
                        continue
                    raise

                # 下线复核：选路到获取连接之间实例可能开始平滑下线，
                # 新请求不得落上去——释放连接并立即重选（不计入重试次数）。
                if self._drain is not None and self._drain.is_draining(instance.id):
                    record["note"] = "实例在选路后开始下线，释放连接并重选"
                    attempts.append(record)
                    self._log_attempt(request, record, reasons, ctx)
                    excluded.add(instance.id)
                    attempt_no -= 1  # 本次不算有效尝试
                    continue

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
                    self._log_attempt(request, record, reasons, ctx)
                    last_error = exc
                    excluded.add(instance.id)
                    if (
                        safe
                        and is_retryable_failure(exc.code)
                        and attempt_no < total_attempts
                    ):
                        continue
                    raise
                else:
                    self._health.record_success(instance.id)
                    self._circuit.record_success(instance.id)
                    record["status"] = response.status
                    attempts.append(record)
                    self._log_attempt(request, record, reasons, ctx)
                    return response
            finally:
                # 无论成功失败、是否重试，连接计数必须精确释放一次；
                # 释放后回调下线检查：在途归零即触发实例真正退出。
                if lease is not None:
                    lease.release()
                    if self._drain is not None:
                        await self._drain.after_release(instance.id)

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
        ctx: SelectionContext | None = None,
    ) -> None:
        sticky_info = None
        if ctx is not None and ctx.session_key is not None:
            sticky_info = {
                "session_key": ctx.session_key,
                "sticky": ctx.sticky,
                "switched": ctx.switched,
                "switch_reason": ctx.switch_reason,
                "bound_instance": ctx.bound_instance,
            }
        self._logger.info(
            "dispatch 判定 method=%s path=%s record=%s candidates=%s sticky=%s",
            request.method,
            request.path,
            record,
            reasons,
            sticky_info,
        )
