"""重试策略：仅对可安全重试的请求与可重试失败重试。"""

from __future__ import annotations

from .errors import FailureCode
from .models import Request

# 仅瞬时、无副作用的失败可重试；业务错误(5xx)默认不重试，避免重复副作用。
RETRYABLE_FAILURES: frozenset[FailureCode] = frozenset(
    {
        FailureCode.UPSTREAM_UNREACHABLE,
        FailureCode.UPSTREAM_TIMEOUT,
        FailureCode.NAME_NOT_RESOLVED,
        FailureCode.NO_AVAILABLE_UPSTREAM,
        FailureCode.ALL_UPSTREAMS_DOWN,
        # 平滑下线并发窗口内选到退出实例：换一个实例重试即可，
        # 尚未发出真实上游调用，对任意方法（含 POST）重试都没有副作用。
        FailureCode.INSTANCE_EXITING,
    },
)

# 幂等/只读方法才可安全重试。
SAFE_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})


def is_safe_to_retry(request: Request) -> bool:
    return request.method.upper() in SAFE_METHODS


def is_retryable_failure(code: FailureCode) -> bool:
    return code in RETRYABLE_FAILURES


def is_side_effect_free(code: FailureCode) -> bool:
    """这类失败发生在真正调用上游之前，对任何 HTTP 方法重试都不会产生重复副作用。"""
    return code == FailureCode.INSTANCE_EXITING


def retry_allowed(request: Request, code: FailureCode) -> bool:
    return is_retryable_failure(code) and (
        is_safe_to_retry(request) or is_side_effect_free(code)
    )
