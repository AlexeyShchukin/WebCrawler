import asyncio
import socket

import aiohttp
import pytest
from aiohttp import web

from crawler_fetcher.network import NetworkFailure, fetch_html

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_total_timeout_stops_continuously_streaming_response():
    class Route(aiohttp.abc.AbstractResolver):
        async def resolve(self, host, port=0, family=socket.AF_UNSPEC):
            return [
                {
                    "hostname": host,
                    "host": "127.0.0.1",
                    "port": port,
                    "family": socket.AF_INET,
                    "proto": 0,
                    "flags": 0,
                }
            ]

        async def close(self):
            pass

    class Resolver:
        async def resolve(self, host):
            return ["8.8.8.8"]

    finished = asyncio.Event()

    async def stream(request):
        response = web.StreamResponse(headers={"Content-Type": "text/html"})
        await response.prepare(request)
        try:
            for _ in range(20):
                await response.write(b"x")
                await asyncio.sleep(0.02)
        except ConnectionResetError:
            pass
        finally:
            finished.set()
        return response

    app = web.Application()
    app.router.add_get("/", stream)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        timeout = aiohttp.ClientTimeout(total=0.12, sock_read=0.08)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(resolver=Route()), timeout=timeout
        ) as session:
            with pytest.raises(NetworkFailure, match="timeout"):
                await fetch_html(
                    session,
                    f"http://public.test:{port}/",
                    max_body_bytes=1024,
                    resolver=Resolver(),
                    chain_timeout_seconds=2,
                )
        await asyncio.wait_for(finished.wait(), 1)
    finally:
        await runner.cleanup()
