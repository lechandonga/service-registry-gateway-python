"""归一化的对外失败类型。

所有内部异常都必须映射为 GatewayError，禁止把堆栈直接透出。
"""

from __future__ import annotations

from enum import Enum


class ErrorType(str, Enum):
    """可区分的失败原因。"""

    REGISTRY_DUPLICATE = "registry_duplicate"
    REGISTRY_NOT_FOUND = "registry_not_found"
    ROUTE_NO_MATCH = "route_no_match"
    ROUTE_NO_AVAILABLE = "route_no_available"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_UNREACHABLE = "upstream_unreachable"
    UPSTREAM_HTTP_ERROR = "upstream_http_error"
    DNS_ERROR = "dns_error"
    UPSTREAM_ERROR = "upstream_error"
    CIRCUIT_OPEN = "circuit_open"
    RETRIES_EXHAUSTED = "retries_exhausted"
    LIMIT_CONCURRENCY = "limit_concurrency"
    LIMIT_CONNECTIONS = "limit_connections"
    LIMIT_TIMEOUT = "limit_timeout"
    BAD_REQUEST = "bad_request"


class GatewayError(Exception):
    """对外统一错误。reason 取 ErrorType，detail 只放安全信息。"""

    def __init__(
        self, reason: ErrorType, message: str, *, detail: dict | None = None
    ) -> None:
        self.reason = reason
        self.message = message
        self.detail = detail or {}
        super().__init__(message)

    def to_dict(self) -> dict:
        return {
            "error": self.reason.value,
            "message": self.message,
            "detail": self.detail,
        }
