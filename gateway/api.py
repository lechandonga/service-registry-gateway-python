"""HTTP 接口：注册管理 + 代理转发 + 状态观测，统一错误响应。"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import FastAPI, Request as FastAPIRequest
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import GatewayError
from .models import (
    Instance,
    InstanceId,
    Request,
    RouteRule,
    new_instance,
)
from .clock import SystemClock


class RegisterIn(BaseModel):
    service: str
    host: str
    port: int = Field(gt=0, lt=65536)
    weight: int = 1
    lease_seconds: float = 10.0
    metadata: dict[str, str] = Field(default_factory=dict)


class HeartbeatIn(BaseModel):
    lease_seconds: float | None = None


class RuleIn(BaseModel):
    service: str
    path_prefix: str = "/"
    require_headers: dict[str, str] = Field(default_factory=dict)
    name: str = ""


class ModeIn(BaseModel):
    mode: str
    status: int = 500
    hang_seconds: float = 30.0


def create_app(
    registry,
    router,
    gateway,
    health,
    circuit,
    cluster,
    clock=None,
    logger: logging.Logger | None = None,
    on_startup=None,
    on_shutdown=None,
    drain=None,
    binder=None,
) -> FastAPI:
    """用注入的组件创建 FastAPI 应用，便于测试替换。"""
    import contextlib

    clock = clock or SystemClock()
    logger = logger or logging.getLogger("gateway.api")
    app = FastAPI(title="service-registry-gateway", version="0.1.0")

    @contextlib.asynccontextmanager
    async def _lifespan(app: FastAPI):
        if on_startup is not None:
            await on_startup()
        try:
            yield
        finally:
            if on_shutdown is not None:
                await on_shutdown()

    @app.exception_handler(GatewayError)
    async def _on_gateway_error(request: FastAPIRequest, exc: GatewayError) -> JSONResponse:
        return await gateway_error_handler(request, exc, logger)

    @app.exception_handler(Exception)
    async def _on_unexpected(request: FastAPIRequest, exc: Exception) -> JSONResponse:
        # 内部未知异常绝不透出堆栈：记录服务端日志，对外返回归一化结构。
        request_id = str(uuid.uuid4())
        logger.exception("未处理内部异常 request_id=%s", request_id)
        return JSONResponse(
            status_code=500,
            content={
                "code": "INTERNAL_ERROR",
                "message": "网关内部错误",
                "request_id": request_id,
            },
        )

    @app.post("/admin/register")
    async def register(body: RegisterIn) -> dict[str, Any]:
        inst = new_instance(
            body.service, body.host, body.port,
            weight=body.weight, lease_seconds=body.lease_seconds,
            metadata=body.metadata, clock=clock,
        )
        await registry.register(inst)
        # 注册时确保有对应模拟上游（默认 ok）。
        cluster.add(inst)
        return {"registered": True, "instance": _instance_dict(inst)}

    @app.delete("/admin/instances/{service}/{host}/{port}")
    async def deregister(service: str, host: str, port: int) -> dict[str, Any]:
        iid = InstanceId(service=service, host=host, port=port)
        inst = await registry.deregister(iid)
        if drain is not None:
            # 主动注销同样联动清理连接计数、健康/熔断状态与会话绑定。
            await drain.cleanup_exited(iid)
        else:
            health.remove(iid)
            circuit.remove(iid)
        return {"deregistered": True, "instance": _instance_dict(inst)}

    @app.post("/admin/instances/{service}/{host}/{port}/drain")
    async def begin_drain(service: str, host: str, port: int) -> dict[str, Any]:
        """平滑下线：不接新流量，在途请求跑完、计数归零后真正退出。"""
        from .drain import DrainStatus
        from .errors import (
            DrainInstanceNotFound,
            InstanceAlreadyDraining,
        )

        if drain is None:
            raise DrainInstanceNotFound("网关未启用平滑下线协调器")
        iid = InstanceId(service=service, host=host, port=port)
        result = await drain.begin_drain(iid)
        payload = result.to_dict()
        if result.status is DrainStatus.NOT_FOUND:
            # 不存在 / 已下线 / 已过期：404 + 专用 code，与正常受理可区分。
            raise DrainInstanceNotFound(result.message)
        if result.status is DrainStatus.ALREADY_DRAINING:
            # 下线过程中重复宣告：409 + 专用 code，幂等且可区分。
            raise InstanceAlreadyDraining(result.message)
        return payload

    @app.post("/admin/instances/{service}/{host}/{port}/heartbeat")
    async def heartbeat(service: str, host: str, port: int, body: HeartbeatIn) -> dict[str, Any]:
        iid = InstanceId(service=service, host=host, port=port)
        expires_at = await registry.heartbeat(iid, body.lease_seconds)
        return {"renewed": True, "instance": str(iid), "expires_at": expires_at}

    @app.get("/admin/instances")
    async def list_instances() -> dict[str, Any]:
        services = await registry.all_services()
        result: dict[str, Any] = {}
        for service in services:
            items = []
            for inst in await registry.instances(service):
                items.append(_instance_dict(inst))
            result[service] = items
        return {"services": result}

    @app.post("/admin/rules")
    async def add_rule(body: RuleIn) -> dict[str, Any]:
        rule = RouteRule(
            service=body.service, path_prefix=body.path_prefix,
            require_headers={k.lower(): v for k, v in body.require_headers.items()},
            name=body.name,
        )
        router.add_rule(rule)
        return {"rule_added": True, "rules": [_rule_dict(r) for r in router.rules()]}

    @app.get("/admin/rules")
    async def list_rules() -> dict[str, Any]:
        return {"rules": [_rule_dict(r) for r in router.rules()]}

    @app.post("/admin/instances/{service}/{host}/{port}/mode")
    async def set_mode(service: str, host: str, port: int, body: ModeIn) -> dict[str, Any]:
        # 本地验证辅助：把指定实例的模拟上游切到某种失败形态。
        from .upstream import MockUpstream

        iid = InstanceId(service=service, host=host, port=port)
        inst = await registry.get(iid)
        if inst is None:
            from .errors import InstanceNotFound

            raise InstanceNotFound(f"实例不存在或已过期: {iid}")
        up = cluster.get(inst)
        up.mode = body.mode
        up.status = body.status
        up.hang_seconds = body.hang_seconds
        up.fail_first_n = 0
        return {"mode": body.mode, "instance": str(iid)}

    @app.get("/admin/status/{service}")
    async def service_status(service: str) -> dict[str, Any]:
        instances = await registry.instances(service)
        rows = []
        for inst in instances:
            row = {
                "instance": str(inst.id),
                "weight": inst.weight,
                "draining": inst.draining,
                "lease_expires_at": inst.expires_at,
                "health": health.explain(inst.id),
                "circuit": circuit.explain(inst.id),
                "active_connections": gateway_pool_active(gateway, inst.id),
            }
            if drain is not None:
                row["drain"] = drain.explain(inst.id)
            rows.append(row)
        sessions: list[dict[str, Any]] = []
        if binder is not None:
            sessions = [
                {
                    "session_id": b.session_id,
                    "instance": str(b.instance_id),
                    "bound_at": b.bound_at,
                    "ttl_remaining": round(b.ttl_remaining(clock.now()), 3),
                }
                for b in binder.bindings_of_service(service)
            ]
        return {"service": service, "instances": rows, "sessions": sessions}

    @app.get("/admin/sessions/{service}/{session_id}")
    async def session_status(service: str, session_id: str) -> dict[str, Any]:
        """查询某个会话当前固定在哪个实例、绑定建立时间与剩余 TTL。"""
        from .errors import NoAvailableUpstream

        if binder is None:
            raise NoAvailableUpstream("网关未启用会话保持")
        info = binder.describe(service, session_id)
        if info is None:
            return {"service": service, "session_id": session_id, "bound": False}
        return {"service": service, "bound": True, "binding": info}

    @app.api_route("/proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
    async def proxy(path: str, request: FastAPIRequest) -> JSONResponse:
        body = await request.body()
        headers = {k.lower(): v for k, v in request.headers.items() if k.lower() != "host"}
        gw_request = Request(
            method=request.method,
            path=f"/proxy/{path}",
            headers=headers,
            body=body,
        )
        response = await gateway.dispatch(gw_request)
        return JSONResponse(
            status_code=response.status,
            content=response.body.decode() if _is_jsonish(response.body) else response.body.decode("latin1"),
            headers={k: v for k, v in response.headers.items()},
        )

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "service-registry-gateway", "docs": "/docs"}

    app.router.lifespan_context = _lifespan
    return app


def gateway_pool_active(gateway, instance_id) -> int:
    return gateway._pool.active_of(instance_id)


def _is_jsonish(body: bytes) -> bool:
    try:
        body.decode()
        return True
    except UnicodeDecodeError:
        return False


def _instance_dict(inst: Instance) -> dict[str, Any]:
    return {
        "service": inst.id.service,
        "host": inst.id.host,
        "port": inst.id.port,
        "weight": inst.weight,
        "lease_seconds": inst.lease_seconds,
        "registered_at": inst.registered_at,
        "last_heartbeat": inst.last_heartbeat,
        "expires_at": inst.expires_at,
        "draining": inst.draining,
        "metadata": inst.metadata,
    }


def _rule_dict(rule: RouteRule) -> dict[str, Any]:
    return {
        "service": rule.service,
        "path_prefix": rule.path_prefix,
        "require_headers": rule.require_headers,
        "name": rule.name,
    }


async def gateway_error_handler(
    request: FastAPIRequest,
    exc: GatewayError,
    logger: logging.Logger | None = None,
) -> JSONResponse:
    """对外错误响应：仅含 code/message，不泄露内部堆栈。"""
    logger = logger or logging.getLogger("gateway.api")
    status = _http_status(exc)
    logger.info("归一化失败 path=%s code=%s message=%s",
                request.url.path, exc.code.value, exc.message)
    return JSONResponse(
        status_code=status,
        content={"code": exc.code.value, "message": exc.message},
    )


def _http_status(exc: GatewayError) -> int:
    mapping = {
        "REG_DUPLICATE": 409,
        "UNREG_NOT_FOUND": 404,
        "HEARTBEAT_UNKNOWN": 404,
        "LEASE_EXPIRED": 410,
        "NO_ROUTE_RULE": 404,
        "NO_AVAILABLE_UPSTREAM": 503,
        "ALL_UPSTREAMS_DOWN": 503,
        "UPSTREAM_UNREACHABLE": 502,
        "NAME_NOT_RESOLVED": 502,
        "UPSTREAM_ERROR": 502,
        "UPSTREAM_TIMEOUT": 504,
        "CONCURRENCY_LIMIT": 429,
        "RETRIES_EXHAUSTED": 502,
        "CIRCUIT_OPEN": 503,
        "RETRY_NOT_SAFE": 422,
        "INSTANCE_DRAIN_NOT_FOUND": 404,
        "INSTANCE_ALREADY_DRAINING": 409,
        "INSTANCE_DRAINING": 409,
        "INSTANCE_EXITING": 503,
    }
    return mapping.get(exc.code.value, 500)
