"""本地模拟上游：替代真实后端，可编排健康/不可达/解析失败/超时/异常。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from .models import Instance, Request, Response
from .transport import (
    ConnectionRefused,
    DnsResolutionError,
    UpstreamInternalCrash,
)


@dataclass
class MockUpstream:
    """
    行为由 mode 决定：
    - ok:           正常 200
    - http_error:   返回给定异常状态码（默认 500）
    - unreachable:  抛 ConnectionRefused（由 transport 归一化）
    - unresolvable: 抛 DnsResolutionError
    - timeout:      阻塞超过调用方超时（由 transport 归一化为 UPSTREAM_TIMEOUT）
    - crash:        抛 UpstreamInternalCrash（内部错误，堆栈不外泄）

    fail_first_n：前 N 次调用失败（mode 给出的失败形态），之后恢复成功，
    用于模拟短暂抖动与"先失败后恢复"。
    """

    name: str
    mode: str = "ok"
    status: int = 500
    latency: float = 0.0
    fail_first_n: int = 0
    hang_seconds: float = 30.0
    calls: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def handle(self, request: Request) -> Response:
        async with self.lock:
            n = self.calls
            self.calls += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        # fail_first_n>0 时启用"先失败后恢复"；否则始终按 mode 行为执行。
        mode = self.mode
        if self.fail_first_n > 0 and n >= self.fail_first_n:
            mode = "ok"
        if mode == "ok":
            return Response(
                status=200,
                body=f"response-from-{self.name}-#{n}".encode(),
                upstream=self.name,
            )
        if mode == "http_error":
            return Response(status=self.status, body=b"upstream error", upstream=self.name)
        if mode == "unreachable":
            raise ConnectionRefused(f"模拟连接被拒绝: {self.name}")
        if mode == "unresolvable":
            raise DnsResolutionError(f"模拟名称无法解析: {self.name}")
        if mode == "timeout":
            await asyncio.sleep(self.hang_seconds)
            return Response(status=200, upstream=self.name)
        if mode == "crash":
            raise UpstreamInternalCrash(f"模拟上游内部崩溃: {self.name}")
        raise UpstreamInternalCrash(f"未知模拟模式: {mode}")


class UpstreamCluster:
    """按实例地址索引的本地模拟上游集合。"""

    def __init__(self) -> None:
        self._upstreams: dict[str, MockUpstream] = {}

    def add(self, instance: Instance, upstream: MockUpstream | None = None) -> MockUpstream:
        upstream = upstream or MockUpstream(name=str(instance.id))
        self._upstreams[instance.id.address] = upstream
        return upstream

    def get(self, instance: Instance) -> MockUpstream:
        """无注册模拟上游时默认返回一个 ok 上游（已解析的正常实例）。"""
        up = self._upstreams.get(instance.id.address)
        if up is None:
            up = MockUpstream(name=str(instance.id))
            self._upstreams[instance.id.address] = up
        return up

    def set_mode(self, instance: Instance, mode: str, **kwargs: int | float) -> MockUpstream:
        up = self.get(instance)
        up.mode = mode
        for key, value in kwargs.items():
            setattr(up, key, value)
        return up

    def reset_calls(self) -> None:
        for up in self._upstreams.values():
            up.calls = 0
