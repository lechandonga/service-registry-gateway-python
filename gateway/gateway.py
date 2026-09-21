"""编排层：选路 -> 熔断/限流 -> 调用模拟上游 -> 反馈健康/熔断 -> 安全重试。

失败类型在各入口结论一致：内部异常一律归一化为 GatewayError，
非幂等请求不重试（避免重复提交），重试尽量落到其他健康实例。
"""

from __future__ import annotations

import asyncio
import logging

from .circuit import CircuitBreaker
from .errors import ErrorType, GatewayError
from .health import HealthChecker
from .limiter import ConcurrencyLimiter
from .models import Request, Response
from .router import Router
from .upstream import LocalUpstreamPool

logger = logging.getLogger("gateway")

# 触发被动健康计数的失败类型
_HEALTH_FAIL_REASONS = {
    ErrorType.UPSTREAM_UNREACHABLE.value,
    ErrorType.UPSTREAM_TIMEOUT.value,
    ErrorType.DNS_ERROR.value,
    ErrorType.UPSTREAM_ERROR.value,
}


def is_idempotent_request(request: Request) -> bool:
    if request.idempotent is not None:
        return request.idempotent
    return request.method.upper() in ("GET", "HEAD", "OPTIONS", "PUT", "DELETE")


class Gateway:
    def __init__(
        self,
        registry,
        router: Router,
        health: HealthChecker,
        circuits: CircuitBreaker,
        limiter: ConcurrencyLimiter,
        upstreams: LocalUpstreamPool,
        max_retries: int = 2,
        retry_backoff: float = 0.0,
    ) -> None:
        self.registry = registry
        self.router = router
        self.health = health
        self.circuits = circuits
        self.limiter = limiter
        self.upstreams = upstreams
        self.max_retries = max_retries
        self._backoff = retry_backoff

    # ---- 对外入口 ----------------------------------------------------------

    async def send(self, request: Request) -> Response:
        retryable = is_idempotent_request(request)
        attempts = 0
        tried: set[str] = set()
        budget = self.max_retries if retryable else 0
        first_error: GatewayError | None = None
        last_response: Response | None = None

        for attempt in range(budget + 1):
            attempts = attempt + 1
            try:
                resp, instance_id = await self._attempt(request, tried)
            except GatewayError as exc:
                logger.info(
                    "send attempt=%d failed reason=%s detail=%s",
                    attempts,
                    exc.reason.value,
                    exc.detail,
                )
                if first_error is None:
                    first_error = exc
                swap_allowed = self._is_retryable_error(exc)
                can_retry = retryable and swap_allowed and attempt < budget
                blocking = exc.reason in (
                    ErrorType.ROUTE_NO_AVAILABLE,
                    ErrorType.CIRCUIT_OPEN,
                )
                if blocking:
                    # 重试被“无可用实例 / 熔断”挡住：
                    # 优先返回最后一个上游响应；否则保留首个连接类错误，保证结论可解释
                    if last_response is not None:
                        last_response.attempts = max(attempts - 1, 1)
                        return last_response
                    if attempt > 0 and first_error is not None:
                        raise first_error
                if can_retry:
                    if self._backoff:
                        await asyncio.sleep(self._backoff)
                    continue
                raise
            resp.attempts = attempts
            # 成功或不可重试的业务响应：直接返回（5xx 是真实上游响应，透传给调用方）
            if not resp.is_retryable() or attempt >= budget:
                return resp
            last_response = resp
            tried.add(instance_id)
            if self._backoff:
                await asyncio.sleep(self._backoff)

        # 理论不可达：预算用尽时上一轮已返回最后响应
        raise GatewayError(
            ErrorType.RETRIES_EXHAUSTED,
            "retries exhausted without response",
            detail={"attempts": attempts},
        )

    # ---- 单次尝试 ----------------------------------------------------------

    async def _attempt(
        self, request: Request, tried: set[str]
    ) -> tuple[Response, str | None]:
        _rule, inst = await self.router.select(request, exclude=tried)
        instance_id = inst.id

        try:
            return await self._attempt_locked(request, inst, instance_id)
        except GatewayError:
            # 该实例本次请求已失败，重试必须落到其他实例
            tried.add(instance_id)
            raise

    async def _attempt_locked(
        self, request: Request, inst, instance_id: str
    ) -> tuple[Response, str | None]:
        if not await self.circuits.allow(instance_id):
            raise GatewayError(
                ErrorType.CIRCUIT_OPEN,
                f"circuit breaker open for upstream {instance_id}",
                detail={
                    "instance_id": instance_id,
                    "state": self.circuits.snapshot(instance_id).state.value,
                },
            )

        async with self.limiter.acquire(instance_id):
            try:
                resp = await asyncio.wait_for(
                    self.upstreams.invoke(inst, request), timeout=request.timeout
                )
            except TimeoutError:
                await self._note_failure(instance_id, "upstream_timeout")
                raise GatewayError(
                    ErrorType.UPSTREAM_TIMEOUT,
                    f"upstream {instance_id} timed out after {request.timeout}s",
                    detail={"instance_id": instance_id, "timeout": request.timeout},
                ) from None
            except GatewayError as exc:
                # 模拟上游的已归一化错误：不可达 / DNS / 上游内部超时
                await self._note_failure(instance_id, exc.reason.value)
                raise
            except Exception:  # noqa: BLE001 - 兜底归一化，内部异常绝不透出对外
                # 兜底：内部异常归一化，绝不把堆栈透出对外
                await self._note_failure(instance_id, "upstream_error")
                raise GatewayError(
                    ErrorType.UPSTREAM_ERROR,
                    f"upstream {instance_id} request failed",
                    detail={"instance_id": instance_id},
                ) from None

        # 判定结果并反馈熔断器/健康检查（计数由 limiter 上下文保证已释放）
        if resp.is_success():
            await self.circuits.record_success(instance_id)
            await self.health.record_passive(instance_id, True, "ok")
            return resp, instance_id

        reason = f"http_{resp.status}"
        if resp.is_retryable():
            await self.circuits.record_failure(instance_id)
            await self.health.record_passive(instance_id, False, reason)
        return resp, instance_id

    # ---- 失败分类 ----------------------------------------------------------

    async def _note_failure(self, instance_id: str, reason: str) -> None:
        """把一次上游失败同时反馈给熔断器与被动健康检查。"""
        await self.circuits.record_failure(instance_id)
        await self.health.record_passive(instance_id, False, reason)

    @staticmethod
    def _is_retryable_error(exc: GatewayError) -> bool:
        return exc.reason in (
            ErrorType.UPSTREAM_TIMEOUT,
            ErrorType.UPSTREAM_UNREACHABLE,
            ErrorType.DNS_ERROR,
            ErrorType.UPSTREAM_ERROR,
            ErrorType.UPSTREAM_HTTP_ERROR,
            ErrorType.RETRIES_EXHAUSTED,
        )
