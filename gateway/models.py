"""核心领域模型：上游实例、租约、路由规则与请求/响应。"""

from __future__ import annotations

from dataclasses import dataclass, field

from .clock import Clock, SystemClock


@dataclass(frozen=True)
class InstanceId:
    service: str
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.service}://{self.host}:{self.port}"

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class Instance:
    id: InstanceId
    weight: int
    lease_seconds: float
    registered_at: float
    last_heartbeat: float
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def expires_at(self) -> float:
        return self.last_heartbeat + self.lease_seconds

    def lease_expired(self, now: float) -> bool:
        return now >= self.expires_at


@dataclass
class RouteRule:
    """按路径前缀与请求头匹配，路由到服务并按权重选实例。"""

    service: str
    path_prefix: str = "/"
    require_headers: dict[str, str] = field(default_factory=dict)
    name: str = ""

    def matches(self, path: str, headers: dict[str, str]) -> bool:
        if not path.startswith(self.path_prefix):
            return False
        for key, value in self.require_headers.items():
            if headers.get(key.lower()) != value:
                return False
        return True


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    def normalized_headers(self) -> dict[str, str]:
        return {str(k).lower(): str(v) for k, v in self.headers.items()}


@dataclass
class Response:
    status: int
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    upstream: str | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


def new_instance(
    service: str,
    host: str,
    port: int,
    *,
    weight: int = 1,
    lease_seconds: float = 10.0,
    metadata: dict[str, str] | None = None,
    clock: Clock | None = None,
) -> Instance:
    clock = clock or SystemClock()
    now = clock.now()
    return Instance(
        id=InstanceId(service=service, host=host, port=port),
        weight=weight,
        lease_seconds=lease_seconds,
        registered_at=now,
        last_heartbeat=now,
        metadata=dict(metadata or {}),
    )
