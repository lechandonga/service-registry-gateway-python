"""传输层测试：四种失败形态归一化为可区分 code，内部堆栈不外泄。"""

import logging

import pytest

from gateway.clock import FakeClock
from gateway.errors import (
    FailureCode,
    NameNotResolved,
    UpstreamBadStatus,
    UpstreamTimeout,
    UpstreamUnreachable,
)
from gateway.models import Request, new_instance
from gateway.transport import UpstreamTransport
from gateway.upstream import UpstreamCluster

LOG = logging.getLogger("test.transport")


def _setup(mode, **kw):
    clock = FakeClock()
    inst = new_instance("svc", "host-a", 8000, clock=clock)
    cluster = UpstreamCluster()
    cluster.set_mode(inst, mode, **kw)
    return inst, UpstreamTransport(cluster)


async def _invoke(inst, transport, **kw):
    return await transport.invoke(inst, Request("GET", "/x"), **kw)


@pytest.mark.parametrize(
    "mode,exc_type,code,extra",
    [
        ("unreachable", UpstreamUnreachable, FailureCode.UPSTREAM_UNREACHABLE, {}),
        ("unresolvable", NameNotResolved, FailureCode.NAME_NOT_RESOLVED, {}),
        ("crash", UpstreamBadStatus, FailureCode.UPSTREAM_ERROR, {}),
        ("timeout", UpstreamTimeout, FailureCode.UPSTREAM_TIMEOUT, {"timeout": 0.05}),
        ("http_error", UpstreamBadStatus, FailureCode.UPSTREAM_ERROR, {}),
    ],
)
@pytest.mark.asyncio
async def test_failure_normalization(mode, exc_type, code, extra):
    inst, transport = _setup(mode, hang_seconds=5.0)
    with pytest.raises(exc_type) as ei:
        await _invoke(inst, transport, **extra)
    assert ei.value.code == code
    # 对外信息不含内部堆栈/追踪帧。
    assert "Traceback" not in ei.value.message
    LOG.info("模式=%s 归一化=%s 说明=%s", mode, code.value, ei.value.message)


@pytest.mark.asyncio
async def test_success_and_fail_first_n_recovery():
    inst, transport = _setup("unreachable", fail_first_n=2)
    with pytest.raises(UpstreamUnreachable):
        await _invoke(inst, transport)
    with pytest.raises(UpstreamUnreachable):
        await _invoke(inst, transport)
    resp = await _invoke(inst, transport)
    assert resp.ok and resp.upstream
    LOG.info("前两次失败后恢复: status=%d upstream=%s", resp.status, resp.upstream)
