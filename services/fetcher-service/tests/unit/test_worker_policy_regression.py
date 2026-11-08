import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from crawler_contracts import FetchUrlEvent

from crawler_fetcher.origin_policy import OriginPolicy
from crawler_fetcher.worker import FetcherWorker


@pytest.mark.asyncio
@pytest.mark.parametrize(('status', 'failures', 'limited'), [
    (400, 0, 0), (404, 0, 0), (429, 0, 1), (500, 1, 0), (599, 1, 0),
])
async def test_http_status_updates_origin_policy(status, failures, limited):
    worker = FetcherWorker(Mock())
    worker.origin_policy = Mock(record_failure=AsyncMock(), record_rate_limit=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url='https://example.com/', depth=0)
    await worker._failure(event, 'https://example.com', detail='error',
                          http_status=status, retry_after='42')
    assert worker.origin_policy.record_failure.await_count == failures
    assert worker.origin_policy.record_rate_limit.await_count == limited
    if limited:
        worker.origin_policy.record_rate_limit.assert_awaited_once_with('https://example.com', 42)


@pytest.mark.asyncio
async def test_shutdown_waits_for_cancelled_task_before_closing_dependencies():
    worker = FetcherWorker(Mock())
    worker._shutdown_timeout_seconds = 0.01
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    closed = False

    async def dependency_close():
        nonlocal closed
        closed = True

    async def work():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    worker._stack.push_async_callback(dependency_close)
    task = asyncio.create_task(work())
    worker._inflight.add(task)
    await entered.wait()
    closing = asyncio.create_task(worker.close())
    await cancelled.wait()
    await asyncio.sleep(1.1)
    assert not closed
    release.set()
    await closing
    assert task.done() and closed


@pytest.mark.asyncio
async def test_http_date_outcome_survives_redis_timeout():
    class HungRedis:
        async def eval(self, *args):
            await asyncio.Event().wait()

    worker = FetcherWorker(Mock())
    worker.origin_policy = OriginPolicy(HungRedis(), SimpleNamespace(
        origin_requests_per_second=1, circuit_breaker_failure_threshold=5,
        redis_operation_timeout_seconds=0.02,
    ))
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url='https://example.com/', depth=0)
    header = format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True)
    result = await asyncio.wait_for(worker._failure(
        event, 'https://example.com', detail='HTTP 429', http_status=429, retry_after=header
    ), 0.5)
    assert 118 <= result.suggested_delay_seconds <= 120
