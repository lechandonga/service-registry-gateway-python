"""FastAPI 入口：管理面（注册/注销/续约/规则/状态）与代理面。

所有对外错误统一经 GatewayError 处理器归一化，不透出内部堆栈。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request as FastRequest
from fastapi.responses import JSONResponse
from fastapi.responses import Response as FastResponse
from pydantic import BaseModel, Field

from .errors import ErrorType, GatewayError
from .gateway import Gateway
from .health import HealthChecker
from .models import Instance as InstanceModel
from .models import Request as GatewayRequest
from .models import RouteRule

logger = logging.getLogger("gateway.api")


# ---- 请求/响应模型 --------------------------------------------------------


class RegisterBody(BaseModel):
    id: str
    host: str
    port: int
    weight: int = 1
    lease_seconds: float = 30.0
    behavior: str = "healthy"
    metadata: dict = Field(default_factory=dict)


class HeartbeatBody(BaseModel):
    lease_seconds: float | None = None


class RuleBody(BaseModel):
    name: str
    path_prefix: str
    match_headers: dict[str, str] = Field(default_factory=dict)
    targets: list[str] = Field(default_factory=list)
    priority: int = 0


class ProxyConfig(BaseModel):
    default_timeout: float = 5.0


# ---- 错误映射码 -----------------------------------------------------------

_STATUS_CODES = {
    ErrorType.REGISTRY_DUPLICATE: 409,
    ErrorType.REGISTRY_NOT_FOUND: 404,
    ErrorType.ROUTE_NO_MATCH: 404,
    ErrorType.ROUTE_NO_AVAILABLE: 503,
    ErrorType.CIRCUIT_OPEN: 503,
    ErrorType.RETRIES_EXHAUSTED: 502,
    ErrorType.LIMIT_CONCURRENCY: 503,
    ErrorType.LIMIT_CONNECTIONS: 503,
    ErrorType.LIMIT_TIMEOUT: 504,
    ErrorType.UPSTREAM_TIMEOUT: 504,
    ErrorType.UPSTREAM_UNREACHABLE: 502,
    ErrorType.UPSTREAM_HTTP_ERROR: 502,
    ErrorType.DNS_ERROR: 502,
    ErrorType.UPSTREAM_ERROR: 502,
    ErrorType.BAD_REQUEST: 400,
}


def _http_status(reason: ErrorType) -> int:
    return _STATUS_CODES.get(reason, 500)


def create_app(
    gateway: Gateway,
    health: HealthChecker | None = None,
    start_background: bool = False,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_background:
            if health is not None:
                await health.start()
            await gateway.registry.start_reaper()
        try:
            yield
        finally:
            if start_background:
                if health is not None:
                    await health.stop()
                await gateway.registry.stop_reaper()

    app = FastAPI(title="service-registry-gateway", lifespan=lifespan)

    # ---- 统一错误处理：归一化输出，绝不外泄内部堆栈 ------------------------

    @app.exception_handler(GatewayError)
    async def _gateway_error_handler(_req: FastRequest, exc: GatewayError):
        logger.info(
            "gateway error: reason=%s message=%s", exc.reason.value, exc.message
        )
        return JSONResponse(status_code=_http_status(exc.reason), content=exc.to_dict())

    @app.exception_handler(Exception)
    async def _unexpected_error_handler(_req: FastRequest, exc: Exception):
        # 内部异常仅记录日志，对外只给归一化消息
        logger.exception("unexpected internal error")
        return JSONResponse(
            status_code=500,
            content={
                "error": ErrorType.UPSTREAM_ERROR.value,
                "message": "internal gateway error",
                "detail": {},
            },
        )

    # ---- 管理面 ------------------------------------------------------------

    @app.post("/admin/instances", status_code=201)
    async def register(body: RegisterBody):
        inst = InstanceModel(
            id=body.id,
            host=body.host,
            port=body.port,
            weight=body.weight,
            lease_seconds=body.lease_seconds,
            behavior=body.behavior,
            metadata=body.metadata,
        )
        gateway.upstreams.configure(body.id, body.behavior)
        await gateway.registry.register(inst)
        return {"registered": True, "instance_id": body.id}

    @app.delete("/admin/instances/{instance_id}")
    async def deregister(instance_id: str):
        await gateway.registry.deregister(instance_id)
        return {"deregistered": True, "instance_id": instance_id}

    @app.post("/admin/instances/{instance_id}/heartbeat")
    async def heartbeat(instance_id: str, body: HeartbeatBody):
        expires_at = await gateway.registry.heartbeat(instance_id, body.lease_seconds)
        return {"instance_id": instance_id, "renewed": True, "expires_at": expires_at}

    @app.put("/admin/routes")
    async def put_rule(body: RuleBody):
        gateway.router.add_rule(
            RouteRule(
                name=body.name,
                path_prefix=body.path_prefix,
                match_headers=body.match_headers,
                targets=body.targets,
                priority=body.priority,
            )
        )
        return {"rule": body.name, "installed": True}

    @app.get("/admin/status")
    async def status():
        instances = []
        for inst in await gateway.registry.snapshot():
            hs = gateway.health.state_of(inst.id)
            cs = gateway.circuits.snapshot(inst.id)
            instances.append(
                {
                    "instance_id": inst.id,
                    "address": inst.address,
                    "weight": inst.weight,
                    "registry_status": inst.status.value,
                    "healthy": hs.healthy,
                    "consecutive_failures": hs.consecutive_failures,
                    "last_reason": hs.last_reason,
                    "circuit": cs.state.value,
                    "active_connections": gateway.limiter.active_for(inst.id),
                    "lease_remaining": max(
                        0.0,
                        inst.last_heartbeat
                        + inst.lease_seconds
                        - gateway.registry._clock(),
                    ),
                }
            )
        return {"instances": instances, "active_total": gateway.limiter.active_total()}

    # ---- 模拟上游行为配置（仅本地验证用）-----------------------------------

    @app.post("/admin/instances/{instance_id}/script")
    async def set_script(instance_id: str, payload: dict):
        results = payload.get("results")
        if not isinstance(results, list) or not all(
            isinstance(x, str) for x in results
        ):
            raise GatewayError(
                ErrorType.BAD_REQUEST,
                "results must be a list of behavior tokens",
                detail={"instance_id": instance_id},
            )
        gateway.upstreams.set_script(instance_id, results)
        return {"instance_id": instance_id, "script_set": len(results)}

    # ---- 代理面 ------------------------------------------------------------

    @app.api_route(
        "/proxy/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
    )
    async def proxy(path: str, fast_request: FastRequest):
        raw = await fast_request.body()
        timeout = float(fast_request.query_params.get("timeout", 5.0))
        req = GatewayRequest(
            path=f"/proxy/{path}",
            method=fast_request.method,
            headers={k: v for k, v in fast_request.headers.items()},
            body=raw,
            timeout=timeout,
        )
        resp = await gateway.send(req)
        return FastResponse(
            content=resp.body,
            status_code=resp.status,
            headers=resp.headers
            | {
                "x-gateway-attempts": str(resp.attempts),
                "x-upstream-id": resp.upstream_id or "",
            },
        )

    @app.get("/")
    async def root():
        return {"service": "service-registry-gateway", "status": "ok"}

    return app
