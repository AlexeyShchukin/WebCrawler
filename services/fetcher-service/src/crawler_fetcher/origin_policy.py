"""Shared, fail-closed Redis admission policy for Fetcher origins."""

import hashlib
import math
from typing import Protocol

_COOLDOWN_MILLISECONDS = 60_000
_STATE_TTL_MILLISECONDS = 3_600_000

_ACQUIRE_LUA = """
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local cooldown_until = tonumber(redis.call('HGET', KEYS[1], 'cooldown_until') or '0')
if cooldown_until > now_ms then
  return {1, cooldown_until - now_ms}
end
local next_allowed_at = tonumber(redis.call('HGET', KEYS[1], 'next_allowed_at') or '0')
if next_allowed_at > now_ms then
  return {2, next_allowed_at - now_ms}
end
redis.call('HSET', KEYS[1], 'next_allowed_at', now_ms + tonumber(ARGV[1]))
redis.call('PEXPIRE', KEYS[1], math.max(tonumber(ARGV[2]), tonumber(ARGV[1])))
return {0, 0}
"""

_RECORD_LUA = """
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
if tonumber(ARGV[1]) == 1 then
  redis.call('HSET', KEYS[1], 'failures', 0, 'cooldown_until', 0)
else
  local failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
  local cooldown_until = tonumber(redis.call('HGET', KEYS[1], 'cooldown_until') or '0')
  if failures >= tonumber(ARGV[2]) and cooldown_until <= now_ms then
    redis.call('HSET', KEYS[1], 'cooldown_until', now_ms + tonumber(ARGV[3]))
  end
end
local next_allowed_at = tonumber(redis.call('HGET', KEYS[1], 'next_allowed_at') or '0')
redis.call('PEXPIRE', KEYS[1], math.max(tonumber(ARGV[4]), next_allowed_at - now_ms))
return {0, 0}
"""


class RedisClient(Protocol):
    """Minimal async Redis script interface used by the policy."""

    async def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object: ...


class OriginPolicySettings(Protocol):
    """The Fetcher settings consumed by the shared origin policy."""

    origin_requests_per_second: float
    circuit_breaker_failure_threshold: int


class OriginPolicyUnavailable(Exception):
    """Redis policy state could not be consulted; do not bypass it."""


class OriginDeferred(Exception):
    """The shared circuit breaker or origin rate limit has deferred admission."""

    def __init__(self, delay_seconds: float = 1.0) -> None:
        self.delay_seconds = delay_seconds
        super().__init__('origin admission deferred')


class OriginPolicy:
    """Lua-backed per-origin policy shared across workers in Redis event order."""

    def __init__(self, redis: RedisClient, settings: OriginPolicySettings) -> None:
        self._redis = redis
        rate = settings.origin_requests_per_second
        if not math.isfinite(rate) or rate <= 0 or not math.isfinite(1000 / rate):
            raise ValueError("origin_requests_per_second must be finite and positive")
        self._interval_milliseconds = max(1, int(1000 / rate))
        self._failure_threshold = settings.circuit_breaker_failure_threshold

    @staticmethod
    def key_for(origin: str) -> str:
        digest = hashlib.sha256(origin.encode("utf-8")).hexdigest()
        return f"fetcher:origin:{digest}"

    async def acquire(self, origin: str) -> None:
        result = await self._eval(
            _ACQUIRE_LUA,
            self.key_for(origin),
            self._interval_milliseconds,
            _STATE_TTL_MILLISECONDS,
        )
        code, delay_ms = _parse_result(result)
        if code != 0:
            raise OriginDeferred(delay_ms / 1000)

    async def record_success(self, origin: str) -> None:
        await self._record(origin, is_success=True)

    async def record_failure(self, origin: str) -> None:
        await self._record(origin, is_success=False)

    async def _record(self, origin: str, *, is_success: bool) -> None:
        result = await self._eval(
            _RECORD_LUA,
            self.key_for(origin),
            int(is_success),
            self._failure_threshold,
            _COOLDOWN_MILLISECONDS,
            _STATE_TTL_MILLISECONDS,
        )
        if _parse_result(result) != (0, 0):
            raise OriginPolicyUnavailable()

    async def _eval(self, script: str, key: str, *args: object) -> object:
        try:
            return await self._redis.eval(script, 1, key, *args)
        except Exception as error:
            raise OriginPolicyUnavailable() from error


def _parse_result(result: object) -> tuple[int, int]:
    """Validate the complete Redis Lua response without coercion."""
    if (
        not isinstance(result, (list, tuple))
        or len(result) != 2
        or type(result[0]) is not int
        or type(result[1]) is not int
    ):
        raise OriginPolicyUnavailable()
    code, delay_ms = result
    if code not in (0, 1, 2) or delay_ms < 0:
        raise OriginPolicyUnavailable()
    if (code == 0 and delay_ms != 0) or (code != 0 and delay_ms == 0):
        raise OriginPolicyUnavailable()
    return code, delay_ms
