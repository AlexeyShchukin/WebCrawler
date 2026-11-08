import asyncio
import socket

import aiohttp
import pytest
from aiohttp import web

from crawler_fetcher.network import (
    NetworkFailure,
    fetch_html,
)
from crawler_fetcher.settings import FetcherSettings

pytestmark = pytest.mark.integration


def settings(**kwargs):
    return FetcherSettings(
        rabbitmq_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0",
        object_storage_endpoint="http://localhost:9000",
        object_storage_access_key="a",
        object_storage_secret_key="b",
        object_storage_bucket="crawler-content",
        **kwargs,
    )







@pytest.mark.asyncio
async def test_chain_deadline_includes_redirect_admission():
    class Resolver:
        async def resolve(self, host):
            return ["8.8.8.8"]

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

    async def redirect(request):
        raise web.HTTPFound("/next")

    app = web.Application()
    app.router.add_get("/start", redirect)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    async def admit(origin):
        await asyncio.sleep(1)

    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(resolver=Route())
        ) as session:
            with pytest.raises(NetworkFailure, match="timeout"):
                await fetch_html(
                    session,
                    f"http://public.test:{port}/start",
                    max_body_bytes=100,
                    resolver=Resolver(),
                    admit_redirect=admit,
                    chain_timeout_seconds=0.05,
                )
    finally:
        await runner.cleanup()
