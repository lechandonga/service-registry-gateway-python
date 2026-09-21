"""连接池测试：上限拒绝可区分、失败无残留、异常路径不漏减、重复释放安全。"""

import asyncio
import logging

import pytest

from gateway.errors import ConcurrencyLimitReached
from gateway.models import new_instance
from gateway.pool import ConnectionPool

LOG = logging.getLogger("test.pool")


@pytest.mark.asyncio
async def test_per_upstream_limit_and_clean_release():
    pool = ConnectionPool(max_per_upstream=2)
    iid = new_instance("svc", "h", 1).id
    l1 = await pool.acquire(iid)
    l2 = await pool.acquire(iid)
    with pytest.raises(ConcurrencyLimitReached) as ei:
        await pool.acquire(iid)
    assert ei.value.code.value == "CONCURRENCY_LIMIT"
    LOG.info("达上限: 活跃=%d 原因=%s", pool.active_of(iid), ei.value.message)
    assert pool.active_of(iid) == 2
    l1.release()
    l2.release()
    assert pool.active_of(iid) == 0
    assert pool.active_total() == 0


@pytest.mark.asyncio
async def test_double_release_no_double_count():
    pool = ConnectionPool(max_per_upstream=5)
    iid = new_instance("svc", "h", 1).id
    lease = await pool.acquire(iid)
    assert lease.release() is True
    assert lease.release() is False  # 幂等
    assert pool.active_of(iid) == 0


@pytest.mark.asyncio
async def test_exception_path_releases_lease():
    pool = ConnectionPool(max_per_upstream=3)
    iid = new_instance("svc", "h", 1).id
    with pytest.raises(RuntimeError):
        async with await pool.acquire(iid):
            raise RuntimeError("模拟处理失败，连接必须释放")
    assert pool.active_of(iid) == 0
    assert pool.active_total() == 0
    LOG.info("异常后计数: per=%d total=%d", pool.active_of(iid), pool.active_total())


@pytest.mark.asyncio
async def test_global_limit():
    pool = ConnectionPool(max_per_upstream=100, max_total=2)
    a = new_instance("svc", "h1", 1).id
    b = new_instance("svc", "h2", 2).id
    await pool.acquire(a)
    await pool.acquire(b)
    with pytest.raises(ConcurrencyLimitReached) as ei:
        await pool.acquire(a)
    assert "全局" in ei.value.message


@pytest.mark.asyncio
async def test_concurrent_acquires_respect_limit_no_leak():
    pool = ConnectionPool(max_per_upstream=10)
    iid = new_instance("svc", "h", 1).id
    acquired = 0
    rejected = 0
    peak = 0

    async def worker():
        nonlocal acquired, rejected, peak
        try:
            lease = await pool.acquire(iid)
        except ConcurrencyLimitReached:
            rejected += 1
            return
        acquired += 1
        peak = max(peak, pool.active_of(iid))
        assert pool.active_of(iid) <= 10
        await asyncio.sleep(0.01)
        lease.release()

    await asyncio.gather(*(worker() for _ in range(100)))
    LOG.info("并发100(持有期间撞上限): 成功=%d 拒绝=%d 峰值=%d 残留per=%d 残留total=%d",
             acquired, rejected, peak, pool.active_of(iid), pool.active_total())
    assert peak == 10
    assert rejected > 0
    assert pool.active_of(iid) == 0
    assert pool.active_total() == 0
