import asyncio
import os
from dataclasses import dataclass
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from crawler_fetcher.origin_policy import OriginDeferred, OriginPolicy

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="requires isolated Redis"),
]


@dataclass
class Settings:
    origin_requests_per_second: float = 0.1
    circuit_breaker_failure_threshold: int = 2


@pytest_asyncio.fixture
async def shared_origin():
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"])
    origin = f"https://{uuid4().hex}.example"
    try:
        yield redis, origin
    finally:
        await redis.delete(OriginPolicy.key_for(origin))
        await redis.aclose()


async def test_concurrent_workers_share_rate_limit(shared_origin):
    redis, origin = shared_origin
    workers = [OriginPolicy(redis, Settings()) for _ in range(25)]
    results = await asyncio.gather(*(worker.acquire(origin) for worker in workers), return_exceptions=True)
    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, OriginDeferred) for result in results) == 24


async def test_cooldown_threshold_failure_does_not_extend_and_expiry_allows_probe(shared_origin):
    redis, origin = shared_origin
    first, second = OriginPolicy(redis, Settings()), OriginPolicy(redis, Settings())
    key = first.key_for(origin)
    await first.record_failure(origin)
    await second.record_failure(origin)
    before = int(await redis.hget(key, "cooldown_until"))
    with pytest.raises(OriginDeferred):
        await first.acquire(origin)
    await asyncio.sleep(0.05)
    await second.record_failure(origin)
    assert int(await redis.hget(key, "cooldown_until")) == before
    await redis.hset(key, "cooldown_until", 1)
    await first.acquire(origin)


async def test_success_closes_shared_cooldown_and_clears_failures(shared_origin):
    redis, origin = shared_origin
    first, second = OriginPolicy(redis, Settings()), OriginPolicy(redis, Settings())
    await first.record_failure(origin)
    await first.record_failure(origin)
    await second.record_success(origin)
    assert int(await redis.hget(first.key_for(origin), "failures")) == 0
    assert int(await redis.hget(first.key_for(origin), "cooldown_until")) == 0
    await first.acquire(origin)


async def test_concurrent_success_and_failure_respect_redis_processing_order(shared_origin):
    redis, origin = shared_origin
    settings = Settings(circuit_breaker_failure_threshold=1)
    first, second = OriginPolicy(redis, settings), OriginPolicy(redis, settings)
    key = first.key_for(origin)
    for _ in range(20):
        await asyncio.gather(first.record_success(origin), second.record_failure(origin))
        failures, cooldown = (int(value) for value in await redis.hmget(key, "failures", "cooldown_until"))
        assert (failures == 0 and cooldown == 0) or (failures == 1 and cooldown > 0)
    await first.record_success(origin)
    await second.record_failure(origin)
    assert int(await redis.hget(key, "failures")) == 1
    assert int(await redis.hget(key, "cooldown_until")) > 0
    await second.record_failure(origin)
    await first.record_success(origin)
    assert [int(value) for value in await redis.hmget(key, "failures", "cooldown_until")] == [0, 0]
