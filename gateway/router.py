"""路由匹配与加权上游选择。"""

from __future__ import annotations

import random

from .errors import ErrorType, GatewayError
from .models import Instance, InstanceStatus, Request, RouteRule


def _hmap(headers: dict[str, str]) -> dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


class Router:
    def __init__(self, registry, health, rng: random.Random | None = None) -> None:
        self._registry = registry
        self._health = health
        self._rules: list[RouteRule] = []
        self._rng = rng or random.Random()

    def add_rule(self, rule: RouteRule) -> None:
        # 同名规则覆盖，其余按 priority 降序、前缀长度降序
        self._rules = [r for r in self._rules if r.name != rule.name]
        self._rules.append(rule)
        self._rules.sort(key=lambda r: (-r.priority, -len(r.path_prefix)))

    def match_rule(self, request: Request) -> RouteRule | None:
        """按 priority 降序、path_prefix 最长匹配，头需全部命中。"""
        headers = _hmap(request.headers)
        for rule in self._rules:
            if not request.path.startswith(rule.path_prefix):
                continue
            wanted = _hmap(rule.match_headers)
            if all(headers.get(k) == v for k, v in wanted.items()):
                return rule
        return None

    async def eligible(self, rule: RouteRule) -> list[Instance]:
        """返回匹配规则且在线、租约有效、健康、权重>0 的实例。"""
        result: list[Instance] = []
        for inst in await self._registry.snapshot():
            if rule.targets and inst.id not in rule.targets:
                continue
            if self._health.is_available(inst):
                result.append(inst)
        return result

    async def diagnose(self, rule: RouteRule) -> list[dict]:
        """给出规则候选实例的不可用原因，便于解释 ROUTE_NO_AVAILABLE。"""
        report = []
        for inst in await self._registry.snapshot():
            if rule.targets and inst.id not in rule.targets:
                continue
            report.append(
                {
                    "instance_id": inst.id,
                    "status": inst.status.value,
                    "weight": inst.weight,
                    "healthy": self._health.state_of(inst.id).healthy,
                    "reason": self._ineligible_reason(inst),
                }
            )
        return report

    def _ineligible_reason(self, inst: Instance) -> str:
        st = self._health.state_of(inst.id)
        if inst.status == InstanceStatus.DEREGISTERED:
            return "deregistered"
        if inst.status == InstanceStatus.EXPIRED:
            return "lease_expired"
        if inst.status == InstanceStatus.SICK:
            return "health_evicted:" + (st.last_reason or "unknown")
        if inst.weight <= 0:
            return "zero_weight"
        if not st.healthy:
            return "marked_unhealthy:" + (st.last_reason or "unknown")
        return "ok"

    async def select(
        self, request: Request, exclude: set[str] | None = None
    ) -> tuple[RouteRule, Instance]:
        """匹配规则并在可用实例中加权随机选择。

        - 无匹配规则 -> ROUTE_NO_MATCH
        - 规则存在但全部不可用 -> ROUTE_NO_AVAILABLE（detail 带每个候选的原因）
        """
        rule = self.match_rule(request)
        if rule is None:
            raise GatewayError(
                ErrorType.ROUTE_NO_MATCH,
                "no route rule matches path/headers",
                detail={"path": request.path, "method": request.method},
            )
        candidates = await self.eligible(rule)
        if exclude:
            candidates = [c for c in candidates if c.id not in exclude]
        if not candidates:
            raise GatewayError(
                ErrorType.ROUTE_NO_AVAILABLE,
                f"rule {rule.name} matched but no upstream available",
                detail={"rule": rule.name, "candidates": await self.diagnose(rule)},
            )
        return rule, self.weighted_pick(candidates, self._rng)

    @staticmethod
    def weighted_pick(instances: list[Instance], rng: random.Random) -> Instance:
        total = sum(i.weight for i in instances)
        point = rng.uniform(0, total)
        upto = 0.0
        for inst in instances:
            upto += inst.weight
            if point <= upto:
                return inst
        return instances[-1]
