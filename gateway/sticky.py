"""会话保持：按请求携带的会话标识做粘性选择，绑定滑动 TTL 与可解释切换。"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass

from .clock import Clock, SystemClock
from .models import InstanceId

# 合法会话标识：字母/数字及常见分隔符，长度受限；缺失或非法一律退化为普通加权选择。
_VALID_SESSION = re.compile(r"^[A-Za-z0-9._:\-@]+$")


@dataclass(frozen=True)
class SessionKey:
    """一个服务内的会话标识（粘性只在同一 service 维度生效）。"""

    service: str
    session_id: str


@dataclass
class SessionBinding:
    """会话 -> 实例的粘性绑定；expires_at 采用滑动续期。"""

    service: str
    session_id: str
    instance_id: InstanceId
    bound_at: float
    expires_at: float

    def ttl_remaining(self, now: float) -> float:
        return max(0.0, self.expires_at - now)


@dataclass
class StickyDecision:
    """一次选路对会话粘性的判定（全部字段面向日志/观测，可解释）。"""

    session_id: str | None
    valid: bool
    # NONE / INVALID_FALLBACK / BOUND_HIT / EXPIRED_REBIND / SWITCH
    action: str
    bound_instance: str | None
    chosen_instance: str | None
    switch_reason: str | None
    detail: str

    def to_dict(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "valid": self.valid,
            "action": self.action,
            "bound_instance": self.bound_instance,
            "chosen_instance": self.chosen_instance,
            "switch_reason": self.switch_reason,
            "detail": self.detail,
        }


class SessionBinder:
    """会话绑定表：线程安全，滑动 TTL，实例退出时按 id 清理陈旧绑定。"""

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        ttl_seconds: float = 300.0,
        session_header: str = "x-session-id",
        max_session_length: int = 128,
        logger: logging.Logger | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._ttl = ttl_seconds
        self._header = session_header.lower()
        self._max_len = max_session_length
        self._logger = logger or logging.getLogger("gateway.sticky")
        self._bindings: dict[SessionKey, SessionBinding] = {}
        self._lock = threading.Lock()

    # ---- 会话标识提取与校验 -------------------------------------------------
    def valid_session_id(self, raw: str) -> bool:
        if not raw or len(raw) > self._max_len:
            return False
        return bool(_VALID_SESSION.match(raw))

    def extract_session(
        self, service: str, headers: dict[str, str],
    ) -> tuple[str | None, str | None]:
        """从（已归一化或未归一化的）请求头提取会话标识。

        返回 (session_id, invalid_raw)：
        - 头缺失       -> (None, None)：按无会话处理；
        - 头值合法     -> (session_id, None)；
        - 头值明显非法 -> (None, raw)：调用方退化为普通加权选择，不得报错。
        """
        lowered = {str(k).lower(): str(v) for k, v in headers.items()}
        raw = lowered.get(self._header)
        if raw is None:
            return None, None
        candidate = raw.strip()
        if not candidate:
            return None, raw
        if self.valid_session_id(candidate):
            return candidate, None
        return None, raw

    # ---- 绑定读写 -----------------------------------------------------------
    def lookup(self, service: str, session_id: str) -> SessionBinding | None:
        """返回仍在有效期内的绑定；不存在或已过期返回 None（过期条目由 prune 清理）。"""
        with self._lock:
            binding = self._bindings.get(SessionKey(service, session_id))
            if binding is None:
                return None
            if binding.ttl_remaining(self._clock.now()) <= 0:
                return None
            return binding

    def _peek(self, service: str, session_id: str) -> SessionBinding | None:
        """不过期校验的读取（供选路层判定 EXPIRED_REBIND 时使用）。"""
        with self._lock:
            return self._bindings.get(SessionKey(service, session_id))

    def bind(
        self, service: str, session_id: str, instance_id: InstanceId,
    ) -> SessionBinding:
        """建立或滑动续期绑定（同一 session 命中同实例时刷新 TTL）。"""
        now = self._clock.now()
        key = SessionKey(service, session_id)
        with self._lock:
            existing = self._bindings.get(key)
            if existing is not None and existing.instance_id == instance_id:
                existing.expires_at = now + self._ttl
                binding = existing
            else:
                binding = SessionBinding(
                    service=service,
                    session_id=session_id,
                    instance_id=instance_id,
                    bound_at=now,
                    expires_at=now + self._ttl,
                )
                self._bindings[key] = binding
            self._logger.info(
                "会话绑定 service=%s session=%s -> instance=%s ttl=%.1fs 建立/续期于 now=%.3f",
                service, session_id, instance_id, self._ttl, now,
            )
            return binding

    def unbind_instance(self, instance_id: InstanceId) -> int:
        """实例真正退出（下线完成/注销/过期）时清理指向它的全部绑定，返回清理条数。

        健康 DOWN / 熔断 OPEN 不调用本方法：保留绑定以便实例恢复后继续粘住。
        """
        with self._lock:
            stale = [
                key for key, binding in self._bindings.items()
                if binding.instance_id == instance_id
            ]
            for key in stale:
                del self._bindings[key]
            if stale:
                self._logger.info(
                    "实例退出，清理会话绑定 instance=%s 清理条数=%d sessions=%s",
                    instance_id, len(stale), [k.session_id for k in stale],
                )
            return len(stale)

    def prune(self) -> int:
        """清理所有过期绑定（后台周期调用），返回清理条数。"""
        now = self._clock.now()
        with self._lock:
            expired = [k for k, b in self._bindings.items() if b.ttl_remaining(now) <= 0]
            for key in expired:
                del self._bindings[key]
            return len(expired)

    # ---- 观测 ---------------------------------------------------------------
    def describe(self, service: str, session_id: str) -> dict[str, object] | None:
        """对外状态查询：当前固定实例、建立时间、剩余 TTL；无有效绑定返回 None。"""
        binding = self.lookup(service, session_id)
        if binding is None:
            return None
        now = self._clock.now()
        return {
            "service": service,
            "session_id": session_id,
            "instance": str(binding.instance_id),
            "bound_at": binding.bound_at,
            "expires_at": binding.expires_at,
            "ttl_remaining": round(binding.ttl_remaining(now), 3),
        }

    def bindings_of_service(self, service: str) -> list[SessionBinding]:
        now = self._clock.now()
        with self._lock:
            return [
                b for b in self._bindings.values()
                if b.service == service and b.ttl_remaining(now) > 0
            ]

    def expired_binding(self, service: str, session_id: str) -> SessionBinding | None:
        """若存在但已过期的绑定则返回（供选路层记录 EXPIRED_REBIND 依据），否则 None。"""
        binding = self._peek(service, session_id)
        if binding is None:
            return None
        if binding.ttl_remaining(self._clock.now()) > 0:
            return None
        return binding
