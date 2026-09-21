"""路由与加权选择：路径+请求头匹配规则，健康/权重/熔断过滤，加权随机。"""

from __future__ import annotations

import random

from .circuit import CircuitBreaker
from .errors import (
    AllUpstreamsDown,
    FailureCode,
    NoAvailableUpstream,
    NoRouteRule,
)
from .health import HealthMonitor
from .models import Instance, Request, RouteRule
from .registry import Registry


class Router:
    """
    路由判定顺序（每一步都产出可解释依据）：
    1. 规则匹配：按 add_rule 的顺序，首个 path_prefix + require_headers 命中的规则胜出；
       全部不命中 -> NO_ROUTE_RULE。
    2. 注册表快照：服务无实例 -> NO_AVAILABLE_UPSTREAM。
    3. 候选过滤：剔除权重<=0、健康 DOWN、熔断未放行的实例；全部被剔除 -> ALL_UPSTREAMS_DOWN，
       并携带每个实例的剔除原因。
    4. 加权随机：在候选中按权重抽取（权重为相对值）。
    """

    def __init__(
        self,
        registry: Registry,
        health: HealthMonitor,
        circuit: CircuitBreaker | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._registry = registry
        self._health = health
        self._circuit = circuit
        self._rules: list[RouteRule] = []
        self._rng = rng or random.Random()

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
        return {
            "instance": str(iid),
            "weight": inst.weight,
            "health": self._health.state_of(iid),
            "circuit": self._circuit.state_of(iid).value if self._circuit else "-",
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

    async def select(self, request: Request) -> tuple[RouteRule, list[dict[str, object]], Instance]:
        """返回 (命中规则, 判定依据, 选中实例)，失败按可区分 code 抛出。"""
        rule = self._match_rule(request)
        instances = await self._registry.instances(rule.service)
        if not instances:
            raise NoAvailableUpstream(
                f"服务 {rule.service} 当前无注册且租约有效的实例",
            )
        candidates, reasons = await self.candidates(rule.service)
        if not candidates:
            raise AllUpstreamsDown(
                f"服务 {rule.service} 全部上游不可用",
            )
        return rule, reasons, self._weighted_pick(candidates)
