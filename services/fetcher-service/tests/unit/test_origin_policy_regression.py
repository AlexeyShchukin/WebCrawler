import math
from dataclasses import dataclass

import pytest

from crawler_fetcher.origin_policy import (
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


