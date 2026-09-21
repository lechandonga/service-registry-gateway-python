"""共享夹具：可控时钟、确定性加权选择、日志输出上游状态与判定依据。"""

from __future__ import annotations

import logging
import random

import pytest

from gateway.factory import build_gateway
from gateway.models import RouteRule


class FakeClock:
    """单调时钟，测试自行推进。"""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FirstRNG(random.Random):
    """让加权随机总是落到总区间起点（候选列表第一个），便于确定性测试。"""

    def uniform(self, a: float, b: float) -> float:  # type: ignore[override]
        return a


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def bundle(clock):
    b = build_gateway(
        clock=clock,
        rng=FirstRNG(),
        health_fail_threshold=3,
        health_recover_threshold=2,
        health_cooldown=0.2,
        circuit_threshold=3,
        circuit_recovery=5.0,
        max_retries=2,
        upstream_delay=0,
        start_background=False,
    )
    b["clock"] = clock
    return b


@pytest.fixture
def catchall(bundle):
    """安装一条 /proxy/ 兜底规则。"""
    bundle["router"].add_rule(RouteRule(name="all", path_prefix="/proxy/"))
    return bundle


async def log_states(bundle, title: str) -> None:
    """打印所有上游的注册/健康/熔断/连接状态与判定依据。"""
    reg, hc, cb, lm = (
        bundle["registry"],
        bundle["health"],
        bundle["circuits"],
        bundle["limiter"],
    )
    lines = [f"=== {title} ==="]
    for inst in await reg.snapshot():
        hs = hc.state_of(inst.id)
        cs = cb.snapshot(inst.id)
        available = hc.is_available(inst)
        lines.append(
            f"  upstream={inst.id} status={inst.status.value} weight={inst.weight} "
            f"healthy={hs.healthy} available={available} circuit={cs.state.value} "
            f"fails={hs.consecutive_failures} succ={hs.consecutive_successes} "
            f"last_reason={hs.last_reason or '-'} conns={lm.active_for(inst.id)}"
        )
    lines.append(f"  active_total={lm.active_total()}")
    logging.getLogger("gateway.test").info("\n".join(lines))


@pytest.fixture(autouse=True)
def _test_logging(caplog):
    caplog.set_level(logging.INFO, logger="gateway")
    caplog.set_level(logging.INFO, logger="gateway.health")
    yield
