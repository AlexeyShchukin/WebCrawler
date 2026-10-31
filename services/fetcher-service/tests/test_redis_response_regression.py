from dataclasses import dataclass

import pytest

from crawler_fetcher.origin_policy import (
    OriginDeferred,
    OriginPolicy,
    OriginPolicyUnavailable,
)


@dataclass
class Settings:
    origin_requests_per_second: float = 1.0
    circuit_breaker_failure_threshold: int = 2


class RedisResponse:
    def __init__(self, response: object) -> None:
        self.response = response

    async def eval(self, script: str, numkeys: int, *args: object) -> object:
        return self.response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [None, [], [0], [0, 999], [0.5, 0], [False, 0], [0, False], [0.0, 0],
     [0, 0.0], ["0", 0], [0, "0"], [3, 1], [1, 0], [2, -1], [0, 0, 0]],
)
async def test_acquire_fails_closed_on_invalid_redis_response(response: object) -> None:
    with pytest.raises(OriginPolicyUnavailable):
        await OriginPolicy(RedisResponse(response), Settings()).acquire("https://example.test")


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [[False, 0], [0, False], [0.0, 0], [0, 0.0], [1, 5]])
@pytest.mark.parametrize("method", ["record_success", "record_failure"])
async def test_record_rejects_non_integer_and_non_success_response(response: object, method: str) -> None:
    policy = OriginPolicy(RedisResponse(response), Settings())
    with pytest.raises(OriginPolicyUnavailable):
        await getattr(policy, method)("https://example.test")


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [1, 2])
async def test_acquire_preserves_valid_deferred_delay(code: int) -> None:
    with pytest.raises(OriginDeferred) as caught:
        await OriginPolicy(RedisResponse([code, 1500]), Settings()).acquire("https://example.test")
    assert caught.value.delay_seconds == 1.5
