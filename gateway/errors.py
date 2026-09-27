"""对外归一化的失败类型定义。"""

from __future__ import annotations

from enum import Enum


class FailureCode(str, Enum):
    """所有可区分失败原因的编码。"""

    REG_DUPLICATE = "REG_DUPLICATE"
    UNREG_NOT_FOUND = "UNREG_NOT_FOUND"
    HEARTBEAT_UNKNOWN = "HEARTBEAT_UNKNOWN"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    NO_ROUTE_RULE = "NO_ROUTE_RULE"
    NO_AVAILABLE_UPSTREAM = "NO_AVAILABLE_UPSTREAM"
    ALL_UPSTREAMS_DOWN = "ALL_UPSTREAMS_DOWN"
    UPSTREAM_UNREACHABLE = "UPSTREAM_UNREACHABLE"
    NAME_NOT_RESOLVED = "NAME_NOT_RESOLVED"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
    CONCURRENCY_LIMIT = "CONCURRENCY_LIMIT"
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    RETRY_NOT_SAFE = "RETRY_NOT_SAFE"
    # 平滑下线相关（对调用方可区分）
    INSTANCE_DRAIN_NOT_FOUND = "INSTANCE_DRAIN_NOT_FOUND"
    INSTANCE_ALREADY_DRAINING = "INSTANCE_ALREADY_DRAINING"
    INSTANCE_DRAINING = "INSTANCE_DRAINING"
    INSTANCE_EXITING = "INSTANCE_EXITING"


class GatewayError(Exception):
    """所有网关失败的基类，携带可区分的失败编码与解释信息。"""

    code: FailureCode = FailureCode.UPSTREAM_ERROR

    def __init__(self, message: str = "", *, code: FailureCode | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code

    def to_dict(self) -> dict[str, str]:
        """对外安全的错误结构：仅编码与可读原因，不含堆栈。"""
        return {"code": self.code.value, "message": self.message or self.code.value}


class RegistrationConflict(GatewayError):
    code = FailureCode.REG_DUPLICATE


class InstanceNotFound(GatewayError):
    code = FailureCode.UNREG_NOT_FOUND


class HeartbeatUnknown(GatewayError):
    code = FailureCode.HEARTBEAT_UNKNOWN


class LeaseExpired(GatewayError):
    code = FailureCode.LEASE_EXPIRED


class NoRouteRule(GatewayError):
    code = FailureCode.NO_ROUTE_RULE


class NoAvailableUpstream(GatewayError):
    code = FailureCode.NO_AVAILABLE_UPSTREAM


class AllUpstreamsDown(GatewayError):
    code = FailureCode.ALL_UPSTREAMS_DOWN


class UpstreamUnreachable(GatewayError):
    code = FailureCode.UPSTREAM_UNREACHABLE


class NameNotResolved(GatewayError):
    code = FailureCode.NAME_NOT_RESOLVED


class UpstreamBadStatus(GatewayError):
    code = FailureCode.UPSTREAM_ERROR


class UpstreamTimeout(GatewayError):
    code = FailureCode.UPSTREAM_TIMEOUT


class ConcurrencyLimitReached(GatewayError):
    code = FailureCode.CONCURRENCY_LIMIT


class CircuitOpen(GatewayError):
    code = FailureCode.CIRCUIT_OPEN


class DrainInstanceNotFound(GatewayError):
    """对不存在 / 已退出 / 已过期的实例发起平滑下线。"""

    code = FailureCode.INSTANCE_DRAIN_NOT_FOUND


class InstanceAlreadyDraining(GatewayError):
    """对已处于下线过程中的实例重复发起下线。"""

    code = FailureCode.INSTANCE_ALREADY_DRAINING


class HeartbeatDrainingRejected(GatewayError):
    """下线过程中的续约被明确拒绝（下线不会被续约复活）。"""

    code = FailureCode.INSTANCE_DRAINING


class InstanceExiting(GatewayError):
    """并发窗口内选中/获得到已宣告退出的实例，换实例重试即可，不面向终端失败。"""

    code = FailureCode.INSTANCE_EXITING


class RetriesExhausted(GatewayError):
    code = FailureCode.RETRIES_EXHAUSTED
