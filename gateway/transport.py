"""上游传输层：调用模拟上游并把各类异常归一化为可区分失败类型。"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .errors import (
    NameNotResolved,
    UpstreamBadStatus,
    UpstreamTimeout,
    UpstreamUnreachable,
)
from .models import Instance, Request, Response

if TYPE_CHECKING:
    from .upstream import UpstreamCluster


class TransportError(Exception):
    """模拟底层网络错误的基类（不对外透出）。"""


class ConnectionRefused(TransportError):
    pass


class DnsResolutionError(TransportError):
    pass


class UpstreamInternalCrash(TransportError):
    pass


class UpstreamTransport:
    """
    归一化规则（内部异常与堆栈不透出）：
    - DnsResolutionError      -> NAME_NOT_RESOLVED
    - ConnectionRefused       -> UPSTREAM_UNREACHABLE
    - asyncio.TimeoutError/超时取消 -> UPSTREAM_TIMEOUT
    - UpstreamInternalCrash   -> UPSTREAM_ERROR（500 语义，仅给出可读信息）
    - 非 2xx 响应             -> UPSTREAM_ERROR（携带状态码），但由上层决定是否重试
    """

    def __init__(self, cluster: "UpstreamCluster") -> None:
        self._cluster = cluster

    async def invoke(
        self,
        instance: Instance,
        request: Request,
        *,
        timeout: float = 5.0,
    ) -> Response:
        upstream = self._cluster.get(instance)
        try:
            response = await asyncio.wait_for(upstream.handle(request), timeout=timeout)
        except DnsResolutionError as exc:
            raise NameNotResolved(f"名称无法解析 {instance.id.host}: {exc}") from None
        except ConnectionRefused as exc:
            raise UpstreamUnreachable(f"上游不可达 {instance.id.address}: {exc}") from None
        except TimeoutError:
            raise UpstreamTimeout(f"上游响应超时 {instance.id.address} (>{timeout}s)") from None
        except UpstreamInternalCrash as exc:
            # 吞掉内部堆栈，仅保留可解释的归一化信息。
            raise UpstreamBadStatus(f"上游内部错误 {instance.id.address}: {exc}") from None
        except Exception as exc:  # pragma: no cover - 兜底，绝不外泄未知异常
            raise UpstreamBadStatus(
                f"上游调用异常 {instance.id.address}: {type(exc).__name__}",
            ) from None

        if not response.ok:
            raise UpstreamBadStatus(
                f"上游返回异常状态 {response.status} @ {instance.id.address}",
            )
        return response
