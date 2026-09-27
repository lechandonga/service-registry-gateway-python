"""会话粘性：按请求携带的会话标识把同一会话稳定绑定到同一实例。

语义约定：
- 会话标识来自请求头（默认 ``x-session-id``）；缺失或明显不合法时返回 None，
  调用方按普通加权选择处理，绝不因此报错。
- 绑定采用滑动有效期：每次命中绑定都会把到期时间顺延 ``ttl_seconds``，
  ``bound_at`` 保留首次建立时间，便于观测"绑了多久"。
- 绑定只作为"偏好"：绑定实例不可用时由路由层切换并改写绑定，
  本模块自身不做可用性判断。
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass

from .clock import Clock, SystemClock
from .models import InstanceId

DEFAULT_SESSION_HEADER = "x-session-id"

# 会话标识合法形态：1~128 位字母数字及 -._~，其余一律视为不合法。
_SESSION_KEY_RE = re.compile(r"^[A-Za-z0-9._~-]{1,128}$")


def normalize_session_key(raw: str | None) -> str | None:
    """把请求头里的原始会话标识归一化；缺失/不合法返回 None（不抛错）。"""
    if raw is None:
        return None
    key = raw.strip()
    if not key or not _SESSION_KEY_RE.match(key):
        return None
    return key


@dataclass(frozen=True)
class SessionBinding:
    """一条会话绑定：绑定实例、建立时间与滑动到期时间。"""

    session_key: str
    instance_id: InstanceId
    bound_at: float
    expires_at: float

    def remaining(self, now: float) -> float:
        return max(0.0, self.expires_at - now)

    def to_dict(self, now: float) -> dict[str, object]:
        return {
            "session_key": self.session_key,
            "instance": str(self.instance_id),
            "bound_at": self.bound_at,
            "expires_at": self.expires_at,
            "remaining_seconds": round(self.remaining(now), 3),
        }


class SessionStore:
    """
    会话绑定表（threading.Lock 保护，临界区内无 await，同步/异步路径均安全）。

    - ``lookup``：命中且未过期则滑动续期并返回绑定；过期则惰性清除并返回 None。
    - ``bind``：建立或改写绑定（切换实例时覆盖旧绑定，``bound_at`` 重置）。
    - ``unbind_instance``：实例退出（注销/平滑下线完成）时清除所有指向它的绑定，
      杜绝陈旧绑定在后续请求中错误命中。
    """

    def __init__(self, clock: Clock | None = None, *, ttl_seconds: float = 300.0) -> None:
        self._clock = clock or SystemClock()
        self._ttl = float(ttl_seconds)
        self._bindings: dict[str, SessionBinding] = {}
        self._lock = threading.Lock()

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    def lookup(self, session_key: str) -> SessionBinding | None:
        """查询有效绑定；命中即滑动续期（expires_at 顺延，bound_at 不变）。"""
        with self._lock:
            binding = self._bindings.get(session_key)
            if binding is None:
                return None
            now = self._clock.now()
            if now >= binding.expires_at:
                # 过期绑定惰性清除，不留下会导致错误命中的陈旧记录。
                del self._bindings[session_key]
                return None
            renewed = SessionBinding(
                session_key=binding.session_key,
                instance_id=binding.instance_id,
                bound_at=binding.bound_at,
                expires_at=now + self._ttl,
            )
            self._bindings[session_key] = renewed
            return renewed

    def bind(self, session_key: str, instance_id: InstanceId) -> SessionBinding:
        """建立/改写绑定；返回新绑定（bound_at 为本次建立时间）。"""
        now = self._clock.now()
        binding = SessionBinding(
            session_key=session_key,
            instance_id=instance_id,
            bound_at=now,
            expires_at=now + self._ttl,
        )
        with self._lock:
            self._bindings[session_key] = binding
        return binding

    def unbind(self, session_key: str) -> bool:
        """按会话键解除绑定；不存在返回 False。"""
        with self._lock:
            return self._bindings.pop(session_key, None) is not None

    def unbind_instance(self, instance_id: InstanceId) -> int:
        """清除所有指向该实例的绑定，返回清除条数。"""
        with self._lock:
            keys = [
                key
                for key, binding in self._bindings.items()
                if binding.instance_id == instance_id
            ]
            for key in keys:
                del self._bindings[key]
            return len(keys)

    def explain(self, session_key: str) -> dict[str, object]:
        """对外观测：某会话当前绑定在哪个实例、何时建立、还剩多久。"""
        now = self._clock.now()
        binding = self.lookup(session_key)
        if binding is None:
            return {"session_key": session_key, "bound": False}
        data = binding.to_dict(now)
        data["bound"] = True
        return data

    def snapshot(self) -> list[dict[str, object]]:
        """全部有效绑定（顺带惰性清理过期项），用于状态查询。"""
        now = self._clock.now()
        with self._lock:
            expired = [
                key for key, b in self._bindings.items() if now >= b.expires_at
            ]
            for key in expired:
                del self._bindings[key]
            items = [b.to_dict(now) for b in self._bindings.values()]
        items.sort(key=lambda d: str(d["session_key"]))
        return items

    def __len__(self) -> int:
        with self._lock:
            return len(self._bindings)
