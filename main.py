"""本地启动入口：uv run uvicorn main:app。"""

from gateway.bootstrap import build_local_gateway

app = build_local_gateway()
