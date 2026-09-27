"""路由与加权选择：路径+请求头匹配规则，会话粘性，健康/权重/熔断/下线过滤，加权随机。"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .circuit import CircuitBreaker
from .errors import (
    AllUpstreamsDown,
    FailureCode,
    NoAvailableUpstream,
    NoRouteRule,
)
from .health import HealthMonitor
from .models import Instance, InstanceId, Request, RouteRule
from .registry import Registry
from .session import SessionStore, normalize_session_key


@dataclass
class SelectionContext:
    """一次选路的可解释结论：粘性命中/切换原因/绑定信息，供日志与调用方观测。"""

    session_key: str | None = None
    sticky: bool = False
    switched: bool = False
    switch_reason: str | None = None
    bound_instance: str | None = None
    bound_at: float | None = None
    binding_expires_at: float | None = None
    notes: list[str] = field(default_factory=list)


class Router:
    """
    路由判定顺序（每一步都产出可解释依据）：
    1. 规则匹配：按 add_rule 的顺序，首个 path_prefix + require_headers 命中的规则胜出；
       全部不命中 -> NO_ROUTE_RULE。
    2. 注册表快照：服务无实例 -> NO_AVAILABLE_UPSTREAM。
    3. 候选过滤：剔除权重<=0、健康 DOWN、熔断未放行、平滑下线中、本次请求已失败的实例；
       全部被剔除 -> ALL_UPSTREAMS_DOWN，并携带每个实例的剔除原因。
    4. 会话粘性：请求携带合法会话标识时，命中未过期绑定且绑定实例仍在候选中则直接固定；
       绑定实例不可用时按切换原因（健康摘除/熔断/下线/租约到期/已注销）改绑到新实例；
       会话标识缺失或不合法时按普通加权选择，不报错。
    5. 加权随机：在候选中按权重抽取（权重为相对值）。
    """

    def __init__(
        self,
        registry: Registry,
        health: HealthMonitor,
        circuit: CircuitBreaker | None = None,
        rng: random.Random | None = None,
        *,
        session_store: SessionStore | None = None,
        drain=None,
    ) -> None:
        self._registry = registry
        self._health = health
        self._circuit = circuit
        self._rules: list[RouteRule] = []
        self._rng = rng or random.Random()
        self._sessions = session_store
        self._drain = drain

    def add_rule(self, rule: RouteRule) -> None:
        self._rules.append(rule)

    def rules(self) -> list[RouteRule]:
        return list(self._rules)

    def _match_rule(self, request: Request) -> RouteRule:
        headers = request.normalized_headers()
        for rule in self._rules:
            if rule.matches(request.path, headers):
                return rule
        raise NoRouteRule(f"无匹配路由规则: {request.method} {request.path}")

    def _is_draining(self, instance_id: InstanceId) -> bool:
        return bool(self._drain is not None and self._drain.is_draining(instance_id))

    async def candidates(
        self, service: str,
    ) -> tuple[list[Instance], list[dict[str, object]]]:
        """返回 (可用候选, 全部实例的判定依据列表)。"""
        instances = await self._registry.instances(service)
        reasons: list[dict[str, object]] = []
        chosen: list[Instance] = []
        for inst in instances:
            reason = self._judge(inst)
            reasons.append(reason)
            if reason["selected_eligible"]:
                chosen.append(inst)
        return chosen, reasons

    def _judge(self, inst: Instance) -> dict[str, object]:
        iid = inst.id
        checks: list[str] = []
        eligible = True
        if inst.weight <= 0:
            eligible = False
            checks.append(f"权重为 {inst.weight}，剔除")
        else:
            checks.append(f"权重 {inst.weight}，通过")
        if not self._health.is_available(iid):
            eligible = False
            checks.append(f"健康={self._health.state_of(iid)}，摘除中")
        else:
            checks.append(f"健康={self._health.state_of(iid)}，通过")
        if self._circuit is not None:
            state = self._circuit.state_of(iid)
            if state.value != "CLOSED":
                eligible = False
                checks.append(f"熔断={state.value}，不放行")
            else:
                checks.append("熔断=CLOSED，通过")
        if self._is_draining(iid):
            eligible = False
            checks.append("平滑下线中，不接新流量")
        return {
            "instance": str(iid),
            "weight": inst.weight,
            "health": self._health.state_of(iid),
            "circuit": self._circuit.state_of(iid).value if self._circuit else "-",
            "draining": self._is_draining(iid),
            "selected_eligible": eligible,
            "reason": "；".join(checks),
        }

    def _weighted_pick(self, candidates: list[Instance]) -> Instance:
        total = sum(i.weight for i in candidates)
        point = self._rng.uniform(0, total)
        upto = 0
        for inst in candidates:
            upto += inst.weight
            if point <= upto:
                return inst
        return candidates[-1]

    def _switch_reason(self, instance_id: InstanceId, reasons: list[dict[str, object]]) -> str:
        """根据判定依据给出绑定实例不可用的可解释原因。"""
        for reason in reasons:
            if reason["instance"] != str(instance_id):
                continue
            if reason.get("draining"):
                return "bound_instance_draining"
            if reason.get("health") == "DOWN":
                return "bound_instance_unhealthy"
            if reason.get("circuit") not in ("CLOSED", "-"):
                return "bound_instance_circuit_open"
            if reason.get("weight", 1) <= 0:
                return "bound_instance_zero_weight"
            return "bound_instance_unavailable"
        # 不在注册表快照中：已注销或租约到期被摘除。
        return "bound_instance_gone"

    async def select(
        self,
        request: Request,
        *,
        ctx: SelectionContext | None = None,
        excluded: frozenset[InstanceId] | None = None,
    ) -> tuple[RouteRule, list[dict[str, object]], Instance]:
        """返回 (命中规则, 判定依据, 选中实例)，失败按可区分 code 抛出。

        ``excluded``：同一次请求中已失败过的实例，重试时不再选中，
        避免反复落在同一个失败实例上。
        """
        rule = self._match_rule(request)
        instances = await self._registry.instances(rule.service)
        if not instances:
            raise NoAvailableUpstream(
                f"服务 {rule.service} 当前无注册且租约有效的实例",
            )
        candidates, reasons = await self.candidates(rule.service)
        if excluded:
            before = len(candidates)
            candidates = [inst for inst in candidates if inst.id not in excluded]
            if ctx is not None and len(candidates) < before:
                ctx.notes.append(
                    f"排除本次已失败实例 {sorted(str(i) for i in excluded)}",
                )
        if not candidates:
            raise AllUpstreamsDown(
                f"服务 {rule.service} 全部上游不可用（含本次已失败实例被排除）",
            )

        session_key = self._session_key_of(request)
        if ctx is not None:
            ctx.session_key = session_key
        if session_key is None or self._sessions is None:
            return rule, reasons, self._weighted_pick(candidates)

        binding = self._sessions.lookup(session_key)
        eligible_ids = {inst.id for inst in candidates}
        if binding is not None and binding.instance_id in eligible_ids:
            # 粘性命中：绑定实例仍可用，稳定落点。
            if ctx is not None:
                ctx.sticky = True
                ctx.bound_instance = str(binding.instance_id)
                ctx.bound_at = binding.bound_at
                ctx.binding_expires_at = binding.expires_at
            return rule, reasons, self._pick_by_id(candidates, binding.instance_id)

        # 需要（重新）绑定：加权选一个可用实例并改写绑定。
        picked = self._weighted_pick(candidates)
        new_binding = self._sessions.bind(session_key, picked.id)
        if ctx is not None:
            ctx.bound_instance = str(picked.id)
            ctx.bound_at = new_binding.bound_at
            ctx.binding_expires_at = new_binding.expires_at
            if binding is not None:
                ctx.switched = True
                ctx.switch_reason = self._switch_reason(binding.instance_id, reasons)
                ctx.notes.append(
                    f"会话 {session_key} 原绑定 {binding.instance_id} 不可用"
                    f"（{ctx.switch_reason}），切换到 {picked.id}",
                )
        return rule, reasons, picked

    @staticmethod
    def _pick_by_id(candidates: list[Instance], instance_id: InstanceId) -> Instance:
        for inst in candidates:
            if inst.id == instance_id:
                return inst
        return candidates[0]  # pragma: no cover - 调用方已保证存在

    @staticmethod
    def _session_key_of(request: Request) -> str | None:
        raw = request.normalized_headers().get("x-session-id")
        return normalize_session_key(raw)
