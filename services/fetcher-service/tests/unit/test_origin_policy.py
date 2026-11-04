from dataclasses import dataclass

import pytest

from crawler_fetcher.origin_policy import (
    OriginDeferred,
    OriginPolicy,
    OriginPolicyUnavailable,
)


@dataclass(frozen=True)
class Settings:
    origin_requests_per_second: float = 1.0
    circuit_breaker_failure_threshold: int = 2


class InMemoryRedis:
    """Small atomic Redis-script double for the adapter contract."""

    def __init__(self) -> None:
        self.state: dict[str, dict[str, int]] = {}
        self.expiry: dict[str, int] = {}
        self.now_ms = 1_000
        self.unavailable = False

    async def eval(self, script: str, numkeys: int, *arguments: object) -> list[int]:
        if self.unavailable:
            raise ConnectionError("redis unavailable")
        assert numkeys == 1
        key = str(arguments[0])
        if self.expiry.get(key, self.now_ms + 1) <= self.now_ms:
            self.state.pop(key, None)
        values = self.state.setdefault(key, {})
        if "if next_allowed_at > now_ms then" in script:
            interval_ms = int(arguments[1])
            if values.get("cooldown_until", 0) > self.now_ms:
                return [1, values["cooldown_until"] - self.now_ms]
            if values.get("next_allowed_at", 0) > self.now_ms:
                return [2, values["next_allowed_at"] - self.now_ms]
            values["next_allowed_at"] = self.now_ms + interval_ms
            self.expiry[key] = self.now_ms + max(interval_ms, int(arguments[2]))
            return [0, 0]
        is_success = int(arguments[1])
        threshold = int(arguments[2])
        if is_success:
            values["failures"] = 0
        else:
            failures = values.get("failures", 0) + 1
            values["failures"] = failures
            if failures >= threshold:
                values["cooldown_until"] = self.now_ms + 60_000
        self.expiry[key] = self.now_ms + max(
            int(arguments[4]), values.get('next_allowed_at', 0) - self.now_ms
        )
        return [0, 0]


@pytest.fixture
def redis_client() -> InMemoryRedis:
    return InMemoryRedis()


@pytest.mark.asyncio
async def test_origin_policy_is_atomic_across_instances(redis_client: InMemoryRedis) -> None:
    first = OriginPolicy(redis_client, Settings())
    second = OriginPolicy(redis_client, Settings())

    await first.acquire("https://example.test:443")

    with pytest.raises(OriginDeferred):
        await second.acquire("https://example.test:443")


@pytest.mark.asyncio
async def test_threshold_failure_defers_admission_until_cooldown_expires(
    redis_client: InMemoryRedis,
) -> None:
    policy = OriginPolicy(redis_client, Settings())
    origin = "https://example.test:443"

    await policy.record_failure(origin)
    await policy.record_failure(origin)

    with pytest.raises(OriginDeferred):
        await policy.acquire(origin)

    redis_client.now_ms += 60_000
    await policy.acquire(origin)


@pytest.mark.asyncio
async def test_success_clears_consecutive_failures(redis_client: InMemoryRedis) -> None:
    policy = OriginPolicy(redis_client, Settings())
    origin = "https://example.test:443"

    await policy.record_failure(origin)
    await policy.record_success(origin)
    await policy.record_failure(origin)

    await policy.acquire(origin)


@pytest.mark.asyncio
async def test_redis_unavailable_never_bypasses_policy(redis_client: InMemoryRedis) -> None:
    redis_client.unavailable = True

    with pytest.raises(OriginPolicyUnavailable):
        await OriginPolicy(redis_client, Settings()).acquire("https://example.test:443")


def test_policy_key_hashes_origin_without_storing_it() -> None:
    key = OriginPolicy.key_for("https://example.test:443")

    assert key.startswith("fetcher:origin:")
    assert "example.test" not in key
    assert len(key.removeprefix("fetcher:origin:")) == 64

@pytest.mark.asyncio
async def test_deferred_origin_exposes_remaining_delay(redis_client: InMemoryRedis) -> None:
    policy = OriginPolicy(redis_client, Settings())
    await policy.acquire('https://example.test')
    with pytest.raises(OriginDeferred) as caught:
        await policy.acquire('https://example.test')
    assert caught.value.delay_seconds == 1.0


@pytest.mark.asyncio
@pytest.mark.parametrize('record', ['record_success', 'record_failure'])
async def test_recording_outcome_preserves_long_rate_limit(redis_client, record):
    policy = OriginPolicy(redis_client, Settings(origin_requests_per_second=1 / 7200))
    origin = 'https://example.test'
    await policy.acquire(origin)
    await getattr(policy, record)(origin)
    redis_client.now_ms += 3_600_001
    with pytest.raises(OriginDeferred) as caught:
        await policy.acquire(origin)
    assert caught.value.delay_seconds == 3599.999
