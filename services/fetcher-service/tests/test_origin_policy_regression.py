import asyncio
import math
import os
from dataclasses import dataclass
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from crawler_fetcher.origin_policy import (
    OriginDeferred,
    OriginPolicy,
    OriginPolicyUnavailable,
)


@dataclass
class Settings:
    origin_requests_per_second: float = 1.0
    circuit_breaker_failure_threshold: int = 2


class ResponseRedis:
    def __init__(self, result: object) -> None:
        self.result = result

    async def eval(self, script: str, numkeys: int, *args: object) -> object:
        return self.result


@pytest.mark.parametrize("rate", [0.0, -1.0, math.inf, math.nan])
def test_invalid_origin_rate_is_rejected(rate: float) -> None:
    with pytest.raises(ValueError):
        OriginPolicy(ResponseRedis([0, 0]), Settings(origin_requests_per_second=rate))


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [None, [], [0], [1, 0], [0, 1], [0, 0, 0], ["bad", 0]])
@pytest.mark.parametrize("method", ["record_success", "record_failure"])
async def test_record_rejects_unexpected_redis_response(result: object, method: str) -> None:
    policy = OriginPolicy(ResponseRedis(result), Settings())
    with pytest.raises(OriginPolicyUnavailable):
        await getattr(policy, method)("https://example.test")


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="requires an isolated Redis server")
async def test_shared_circuit_transitions_are_atomic() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"])
    origin = f"https://{uuid4().hex}.example"
    first, second = OriginPolicy(redis, Settings()), OriginPolicy(redis, Settings())
    key = first.key_for(origin)
    try:
        await asyncio.gather(first.record_failure(origin), second.record_failure(origin))
        before = int(await redis.hget(key, "cooldown_until"))
        with pytest.raises(OriginDeferred):
            await second.acquire(origin)
        await second.record_failure(origin)
        assert int(await redis.hget(key, "cooldown_until")) == before
        await first.record_success(origin)
        await second.acquire(origin)
        await redis.hset(key, "next_allowed_at", 0)
        await first.record_failure(origin)
        await second.acquire(origin)
    finally:
        await redis.delete(key)
        await redis.aclose()
