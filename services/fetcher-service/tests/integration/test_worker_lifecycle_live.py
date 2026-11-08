import asyncio
import os
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from crawler_contracts import FetchUrlEvent
from redis.asyncio import Redis

from crawler_fetcher.lease_client import LeaseGrant
from crawler_fetcher.origin_policy import OriginDeferred, OriginPolicy
from crawler_fetcher.worker import FetcherWorker

pytestmark = [pytest.mark.integration, pytest.mark.requires_infra]


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('TEST_REDIS_URL'), reason='requires isolated Redis')
async def test_live_redis_concurrent_429_shared_across_workers():
    redis = Redis.from_url(os.environ['TEST_REDIS_URL'])
    settings = SimpleNamespace(origin_requests_per_second=100, circuit_breaker_failure_threshold=2)
    workers = [OriginPolicy(redis, settings) for _ in range(20)]
    origin = f'https://{uuid4().hex}.example'
    key = workers[0].key_for(origin)
    try:
        await asyncio.gather(*(worker.record_rate_limit(origin, delay)
                               for worker, delay in zip(workers, [1, 2] * 10, strict=True)))
        results = await asyncio.gather(*(worker.acquire(origin) for worker in workers),
                                       return_exceptions=True)
        assert all(isinstance(result, OriginDeferred) and result.delay_seconds > 1.5
                   for result in results)
        await workers[0].record_success(origin)
        with pytest.raises(OriginDeferred):
            await workers[1].acquire(origin)
        await asyncio.sleep(2.1)
        await workers[2].acquire(origin)
    finally:
        await redis.delete(key)
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('TEST_REDIS_URL'), reason='requires isolated Redis')
async def test_http_date_outcome_sets_shared_redis_cooldown():
    redis = Redis.from_url(os.environ['TEST_REDIS_URL'])
    settings = SimpleNamespace(origin_requests_per_second=1, circuit_breaker_failure_threshold=2)
    first, second = OriginPolicy(redis, settings), OriginPolicy(redis, settings)
    origin = f'https://{uuid4().hex}.example'
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url=f'{origin}/', depth=0)
    worker = FetcherWorker(Mock())
    worker.origin_policy = first
    header = format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True)
    key = first.key_for(origin)
    try:
        seconds, microseconds = await redis.time()
        before = seconds * 1000 + microseconds // 1000
        outcome = await worker._failure(event, origin, detail='HTTP 429', http_status=429,
                                        retry_after=header)
        seconds, microseconds = await redis.time()
        after = seconds * 1000 + microseconds // 1000
        delay = outcome.suggested_delay_seconds
        assert 115 <= delay <= 120
        until = int(await redis.hget(key, 'rate_limited_until'))
        assert before + delay * 1000 <= until <= after + delay * 1000
        with pytest.raises(OriginDeferred) as deferred:
            await second.acquire(origin)
        assert 0 < deferred.value.delay_seconds <= delay
        await second.record_success(origin)
        await second.record_rate_limit(origin, 1)
        assert int(await redis.hget(key, 'rate_limited_until')) == until
        with pytest.raises(OriginDeferred):
            await first.acquire(origin)
    finally:
        await redis.delete(key)
        await redis.aclose()


def make_worker():
    settings = SimpleNamespace(fetch_lease_seconds=0.03, http_max_body_bytes=1000,
                               http_chain_timeout_seconds=5)
    worker = FetcherWorker(settings)
    worker.origin_policy = Mock(acquire=AsyncMock(), record_success=AsyncMock())
    worker.storage = Mock(put_for_event=AsyncMock(return_value='key'))
    worker.session = worker.resolver = None
    worker.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(
        uuid4(), datetime.now(UTC) + timedelta(minutes=1))), renew=AsyncMock(), submit=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url='https://example.com/', depth=0)
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock())
    return worker, message






