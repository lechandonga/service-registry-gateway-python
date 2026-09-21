"""本地模拟上游：不发真实网络请求，但把故障归一化为真实失败类型。

支持的行为（UpstreamBehavior）：
- healthy: 正常返回 200
- flaky: 按概率/固定脚本间歇失败
- timeout: 延迟超过调用方超时（由调用方 wait_for 归一化为超时）
- unreachable: 模拟连接被拒（UPSTREAM_UNREACHABLE）
- dns_fail: 主机名无法解析（DNS_ERROR）
- http_5xx: 稳定返回 5xx（以 Response 形式返回，不抛错）
"""

from __future__ import annotations

import asyncio
import random

from .errors import ErrorType, GatewayError
from .models import Request, Response


class UpstreamBehavior:
    HEALTHY = "healthy"
    FLAKY = "flaky"
    TIMEOUT = "timeout"
    UNREACHABLE = "unreachable"
    DNS_FAIL = "dns_fail"
    HTTP_5XX = "http_5xx"


class _Config:
    __slots__ = (
        "behavior",
        "delay",
        "fail_kind",
        "fail_rate",
        "script",
        "timeout_delay",
    )

    def __init__(self, behavior: str, **opts) -> None:
        self.behavior = behavior
        self.delay: float = opts.get("delay", 0.001)
        self.timeout_delay: float = opts.get("timeout_delay", 30.0)
        self.fail_rate: float = opts.get("fail_rate", 0.5)
        self.fail_kind: str = opts.get("fail_kind", "unreachable")
        self.script: list[str] | None = None


class LocalUpstreamPool:
    """按实例 id 保存模拟实例的行为，提供统一 invoke()。"""

    def __init__(self, delay: float = 0.001, rng: random.Random | None = None) -> None:
        self._default_delay = delay
        self._cfgs: dict[str, _Config] = {}
        self._rng = rng or random.Random(0xC0FFEE)

    def configure(self, instance_id: str, behavior: str, **opts) -> None:
        cfg = _Config(behavior, **opts)
        if "delay" not in opts:
            cfg.delay = self._default_delay
        self._cfgs[instance_id] = cfg

    def set_script(self, instance_id: str, results: list[str]) -> None:
        """用固定结果序列驱动确定性测试。

        令牌：healthy / timeout / unreachable / dns_fail / http_5xx / http_<code>。
        序列耗尽后回落到 healthy。
        """
        cfg = self._cfgs.setdefault(instance_id, _Config(UpstreamBehavior.HEALTHY))
        cfg.script = list(results)

    def _next_token(self, cfg: _Config) -> str:
        if cfg.script:
            token = cfg.script.pop(0)
            return token
        if cfg.behavior == UpstreamBehavior.FLAKY:
            return (
                cfg.fail_kind
                if self._rng.random() < cfg.fail_rate
                else UpstreamBehavior.HEALTHY
            )
        return cfg.behavior

    async def invoke(self, inst, request: Request) -> Response:
        """向模拟上游发一次请求，返回 Response 或抛 GatewayError。"""
        cfg = self._cfgs.get(inst.id) or _Config(
            UpstreamBehavior.HEALTHY, delay=self._default_delay
        )
        token = self._next_token(cfg)

        if token == UpstreamBehavior.TIMEOUT:
            # 模拟上游长时间不响应；由调用方 wait_for 截断并归一化。
            await asyncio.sleep(cfg.timeout_delay)
            raise GatewayError(
                ErrorType.UPSTREAM_TIMEOUT,
                f"upstream {inst.id} did not respond in time",
                detail={"instance_id": inst.id},
            )

        await asyncio.sleep(cfg.delay)

        if token == UpstreamBehavior.HEALTHY:
            return Response(
                status=200,
                body=b"ok:" + inst.id.encode(),
                upstream_id=inst.id,
                headers={"x-upstream": inst.id},
            )
        if token == UpstreamBehavior.UNREACHABLE:
            raise GatewayError(
                ErrorType.UPSTREAM_UNREACHABLE,
                f"upstream {inst.id} unreachable (connection refused)",
                detail={"instance_id": inst.id, "address": inst.address},
            )
        if token == UpstreamBehavior.DNS_FAIL:
            raise GatewayError(
                ErrorType.DNS_ERROR,
                f"cannot resolve host of upstream {inst.id}",
                detail={"instance_id": inst.id, "host": inst.host},
            )
        if token == UpstreamBehavior.HTTP_5XX:
            return Response(status=503, body=b"bad gateway", upstream_id=inst.id)
        if token.startswith("http_"):
            code = int(token.split("_", 1)[1])
            return Response(
                status=code, body=f"http_{code}".encode(), upstream_id=inst.id
            )

        raise GatewayError(
            ErrorType.UPSTREAM_ERROR,
            f"upstream {inst.id} returned unknown simulated result: {token}",
            detail={"instance_id": inst.id, "token": token},
        )
