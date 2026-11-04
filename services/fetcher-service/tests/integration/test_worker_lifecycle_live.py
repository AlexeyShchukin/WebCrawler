import asyncio
import os
from datetime import UTC, datetime, timedelta
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






