"""组装一套本地可运行的网关组件。"""

from __future__ import annotations

import random
import time

from .api import create_app
from .circuit import CircuitBreaker
from .gateway import Gateway
from .health import HealthChecker
from .limiter import ConcurrencyLimiter
from .registry import ServiceRegistry
from .router import Router
from .upstream import LocalUpstreamPool


def build_gateway(
    *,
    clock=time.monotonic,
    rng: random.Random | None = None,
    gateway_max: int = 100,
    instance_max: int = 10,
    health_fail_threshold: int = 3,
    health_recover_threshold: int = 2,
    health_cooldown: float = 0.2,
    probe_interval: float = 0.1,
    circuit_threshold: int = 5,
    circuit_recovery: float = 5.0,
    max_retries: int = 2,
    upstream_delay: float = 0.001,
    start_background: bool = True,
):
    registry = ServiceRegistry(clock=clock)
    upstreams = LocalUpstreamPool(delay=upstream_delay, rng=rng)
    health = HealthChecker(
        registry,
        upstreams,
        fail_threshold=health_fail_threshold,
        recover_threshold=health_recover_threshold,
        recover_cooldown=health_cooldown,
        probe_interval=probe_interval,
        clock=clock,
    )
    router = Router(registry, health, rng=rng)
    circuits = CircuitBreaker(
        failure_threshold=circuit_threshold,
        recovery_timeout=circuit_recovery,
        clock=clock,
    )
    limiter = ConcurrencyLimiter(gateway_max=gateway_max, instance_max=instance_max)
    gateway = Gateway(
        registry, router, health, circuits, limiter, upstreams, max_retries=max_retries
    )
    app = create_app(gateway, health=health, start_background=start_background)
    return {
        "app": app,
        "gateway": gateway,
        "registry": registry,
        "upstreams": upstreams,
        "health": health,
        "router": router,
        "circuits": circuits,
        "limiter": limiter,
    }
