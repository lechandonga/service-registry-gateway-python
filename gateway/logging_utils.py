"""判定依据与上游状态的结构化日志。"""

from __future__ import annotations

import json
import logging
import sys


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # 结构化字段挂在 extra 的 data 中。
        data = getattr(record, "data", None)
        if isinstance(data, dict):
            payload["data"] = data
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    root = logging.getLogger("gateway")
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
        root.addHandler(handler)
    root.propagate = False
    return root


def log_decision(
    logger: logging.Logger,
    event: str,
    *,
    request_id: str | None = None,
    **fields: object,
) -> None:
    data: dict[str, object] = {"event": event}
    if request_id is not None:
        data["request_id"] = request_id
    data.update(fields)
    logger.info("%s", event, extra={"data": data})
