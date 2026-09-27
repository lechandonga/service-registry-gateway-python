"""路由与加权选择：路径+请求头匹配规则，健康/权重/熔断/下线过滤，加权随机，会话粘性。"""

from __future__ import annotations

import random

from .circuit import CircuitBreaker
from .errors import (
    AllUpstreamsDown,
    NoAvailableUpstream,
    NoRouteRule,
)
from .health import HealthMonitor
from .models import Instance, InstanceId, Request, RouteRule
from .registry import Registry
from .sticky import StickyDecision, SessionBinder


class Router:
    """
    路由判定顺序（每一步都产出可解释依据）：
    1. 规则匹配：按 add_rule 的顺序，首个 path_prefix + require_headers 命中的规则胜出；
       全部不命中 -> NO_ROUTE_RULE。
    2. 注册表快照：服务无实例 -> NO_AVAILABLE_UPSTREAM。
    3. 候选过滤：剔除权重<=0、健康 DOWN、熔断未放行、正在平滑下线的实例；全部被剔除
       -> ALL_UPSTREAMS_DOWN，并携带每个实例的剔除原因。
    4. 会话粘性（可选）：合法会话标识已绑定且实例仍可用 -> 稳定命中；
       绑定实例退出/不可用/绑定过期 -> 重新加权选择并给出可解释切换原因；
       标识缺失或非法 -> 静默退化为普通加权选择。
    5. 加权随机：在候选中按权重抽取（权重为相对值）。
    """

    def __init__(
        self,
        registry: Registry,
        health: HealthMonitor,
        circuit: CircuitBreaker | None = None,
        rng: random.Random | None = None,
        *,
        binder: SessionBinder | None = None,
        drain_coordinator=None,
    ) -> None:
        self._registry = registry
        self._health = health
        self._circuit = circuit
        self._binder = binder
        self._drain = drain_coordinator
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
        drain_state = self._drain.state_of(iid) if self._drain is not None else "SERVING"
        if drain_state != "SERVING":
            eligible = False
            checks.append(f"平滑下线={drain_state}，不接新流量")
        else:
            checks.append("平滑下线=SERVING，通过")
        return {
            "instance": str(iid),
            "weight": inst.weight,
            "health": self._health.state_of(iid),
            "circuit": self._circuit.state_of(iid).value if self._circuit else "-",
            "drain": drain_state,
            "selected_eligible": eligible,
            "reason": "；".join(checks),
        }

    def _weighted_pick(
        self, candidates: list[Instance], exclude: frozenset[InstanceId] = frozenset(),
    ) -> Instance | None:
        """在（排除已尝试失败实例后的）候选中按权重抽取；全被排除返回 None。"""
        pool = [i for i in candidates if i.id not in exclude]
        if not pool:
            return None
        total = sum(i.weight for i in pool)
        point = self._rng.uniform(0, total)
        upto = 0
        for inst in pool:
            upto += inst.weight
            if point <= upto:
                return inst
        return pool[-1]

    async def select(self, request: Request) -> tuple[RouteRule, list[dict[str, object]], Instance]:
        """返回 (命中规则, 判定依据, 选中实例)，失败按可区分 code 抛出。"""
        rule, reasons, _, instance = await self.select_with_session(request)
        return rule, reasons, instance

    async def select_with_session(
        self,
        request: Request,
        *,
        exclude: frozenset[InstanceId] = frozenset(),
    ) -> tuple[RouteRule, list[dict[str, object]], StickyDecision, Instance]:
        """带会话粘性的选路；exclude 为本请求此前已失败的实例，重试不会再选中它们。"""
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

        decision = self._sticky_decision(rule.service, request, candidates, reasons, exclude)
        return rule, reasons, decision, decision_chosen(decision, candidates)

    def _sticky_decision(
        self,
        service: str,
        request: Request,
        candidates: list[Instance],
        reasons: list[dict[str, object]],
        exclude: frozenset[InstanceId],
    ) -> StickyDecision:
        eligible_ids = {inst.id for inst in candidates}

        def pick_weighted(
            action: str, detail: str, *, session_id: str | None, valid: bool,
            bound: str | None = None, switch_reason: str | None = None,
        ) -> StickyDecision:
            chosen = self._weighted_pick(candidates, exclude)
            return StickyDecision(
                session_id=session_id,
                valid=valid,
                action=action,
                bound_instance=bound,
                chosen_instance=str(chosen.id) if chosen else None,
                switch_reason=switch_reason,
                detail=detail + ("；已尝试失败实例被排除" if exclude else ""),
            )

        if self._binder is None:
            return pick_weighted("NONE", "未启用会话保持", session_id=None, valid=False)

        session_id, invalid_raw = self._binder.extract_session(
            service, request.normalized_headers(),
        )
        if invalid_raw is not None:
            # 标识明显非法：不报错、不判无可用上游，退化为普通加权选择。
            return pick_weighted(
                "INVALID_FALLBACK",
                f"会话标识非法({invalid_raw!r})，退化为加权选择",
                session_id=None, valid=False,
            )
        if session_id is None:
            return pick_weighted(
                "NONE", "请求未携带会话标识，按加权选择",
                session_id=None, valid=False,
            )

        binding = self._binder.lookup(service, session_id)
        if binding is None:
            expired = self._binder.expired_binding(service, session_id)
            decision = pick_weighted(
                "EXPIRED_REBIND" if expired else "NEW_BIND",
                "粘性已过期，重新选择并绑定" if expired else "新会话，首次选择并绑定",
                session_id=session_id, valid=True,
                bound=str(expired.instance_id) if expired else None,
                switch_reason="STICKY_TTL_EXPIRED" if expired else None,
            )
        else:
            reason_row = next(
                (r for r in reasons if r["instance"] == str(binding.instance_id)), None,
            )
            if binding.instance_id in eligible_ids and binding.instance_id not in exclude:
                chosen = next(i for i in candidates if i.id == binding.instance_id)
                # 命中即滑动续期：粘性有效期随每次使用刷新。
                self._binder.bind(service, session_id, chosen.id)
                decision = StickyDecision(
                    session_id=session_id, valid=True, action="BOUND_HIT",
                    bound_instance=str(binding.instance_id),
                    chosen_instance=str(chosen.id), switch_reason=None,
                    detail="命中粘性绑定，滑动续期",
                )
            else:
                if binding.instance_id in exclude:
                    switch = "BOUND_INSTANCE_FAILED"
                    detail = "绑定实例本次请求已失败，切换到其他实例重试"
                elif reason_row is not None:
                    switch = "BOUND_INSTANCE_UNAVAILABLE"
                    detail = f"绑定实例不再可用：{reason_row['reason']}"
                else:
                    switch = "BOUND_INSTANCE_GONE"
                    detail = "绑定实例已退出注册表（下线完成/注销/租约过期）"
                decision = pick_weighted(
                    "SWITCH", detail,
                    session_id=session_id, valid=True,
                    bound=str(binding.instance_id), switch_reason=switch,
                )
        return decision


def decision_chosen(
    decision: StickyDecision, candidates: list[Instance],
) -> Instance:
    """按粘性决策落地实例；重试排除后无其他候选时抛 AllUpstreamsDown。"""
    if decision.chosen_instance is None:
        raise AllUpstreamsDown("除已失败实例外无其他可用上游")
    for inst in candidates:
        if str(inst.id) == decision.chosen_instance:
            return inst
    # 理论不可达：chosen 一定来自候选；防御性报错。
    raise AllUpstreamsDown("粘性决策的候选实例已不存在")
