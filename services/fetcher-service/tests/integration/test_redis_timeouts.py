import asyncio
from types import SimpleNamespace

import pytest
from redis.asyncio import Redis

from crawler_fetcher.origin_policy import OriginPolicy, OriginPolicyUnavailable

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_real_redis_client_blackhole_is_bounded_and_connection_closes():
    accepted = asyncio.Event()
    disconnected = asyncio.Event()

    async def blackhole(reader, writer):
        accepted.set()
        try:
            while await reader.read(1024):
                pass
        finally:
            writer.close()
            await writer.wait_closed()
            disconnected.set()

    server = await asyncio.start_server(blackhole, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    redis = Redis.from_url(
        f"redis://127.0.0.1:{port}/0?socket_timeout=30&socket_connect_timeout=30"
    )
    settings = SimpleNamespace(origin_requests_per_second=1, circuit_breaker_failure_threshold=5,
                               redis_operation_timeout_seconds=0.05)
    try:
        with pytest.raises(OriginPolicyUnavailable):
            await asyncio.wait_for(OriginPolicy(redis, settings).acquire("https://example.com"), 1)
        assert accepted.is_set()
    finally:
        await redis.aclose()
        server.close()
        await server.wait_closed()
    await asyncio.wait_for(disconnected.wait(), 1)
