"""本地启动入口：uv run uvicorn main:app --reload"""

from __future__ import annotations

import logging

from gateway.factory import build_gateway

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

_bundle = build_gateway()
app = _bundle["app"]
gateway = _bundle["gateway"]
