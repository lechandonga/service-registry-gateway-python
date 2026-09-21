"""数据模型：实例、路由规则、请求/响应。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class InstanceStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"  # 主动注销 / 权重归零下线
    EXPIRED = "expired"  # 租约过期
    SICK = "sick"  # 健康检查摘除
    DEREGISTERED = "deregistered"


@dataclass
class Instance:
    id: str
    host: str
    port: int
    weight: int = 1
    status: InstanceStatus = InstanceStatus.ONLINE
    registered_at: float = 0.0
    last_heartbeat: float = 0.0
    lease_seconds: float = 30.0
    # 模拟上游行为（见 upstream.UpstreamBehavior）
    behavior: str = "healthy"
    metadata: dict = field(default_factory=dict)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class RouteRule:
    name: str
    path_prefix: str
    # 需要全部命中的请求头
    match_headers: dict[str, str] = field(default_factory=dict)
    # 允许转发到的实例 id 列表；空列表表示该服务下任意实例
    targets: list[str] = field(default_factory=list)
    priority: int = 0


@dataclass
class Request:
    path: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    # POST/PUT 等带 body 的请求默认不可安全重试
    idempotent: bool | None = None
    timeout: float = 5.0


@dataclass
class Response:
    status: int
    body: bytes = b""
    upstream_id: str | None = None
    attempts: int = 0
    headers: dict[str, str] = field(default_factory=dict)

    def is_success(self) -> bool:
        return 200 <= self.status < 400

    def is_retryable(self) -> bool:
        # 408 / 5xx 可重试，其余业务错误不重试
        return self.status == 408 or self.status >= 500
