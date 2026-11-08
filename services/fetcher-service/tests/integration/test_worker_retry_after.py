import socket
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import aiohttp
import pytest
from aiohttp import web
from crawler_contracts import FetchRetryRequestedEvent, FetchUrlEvent, PageFailedEvent

from crawler_fetcher.lease_client import LeaseGrant
from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.worker import FetcherWorker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize("status", [404, 429, 503])
@pytest.mark.parametrize("header_kind", ["long_decimal", "http_date"])
async def test_retry_after_does_not_stop_consumer(status, header_kind):
    retry_after = ("9" * 5000 if header_kind == "long_decimal" else
                   format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True))
    async def handler(request):
        return web.Response(status=status, headers={"Retry-After": retry_after})

    class Route(aiohttp.abc.AbstractResolver):
        async def resolve(self, host, port=0, family=socket.AF_UNSPEC):
            return [{"hostname": host, "host": "127.0.0.1", "port": port,
                     "family": socket.AF_INET, "proto": 0, "flags": 0}]

        async def close(self):
            pass

    class PublicResolver:
        async def resolve(self, host):
            return ["8.8.8.8"]

    app = web.Application()
    app.router.add_get("/", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    settings = FetcherSettings(rabbitmq_url="amqp://localhost/", redis_url="redis://localhost/",
                               object_storage_endpoint="http://localhost:9000",
                               object_storage_access_key="a", object_storage_secret_key="b",
                               object_storage_bucket="crawler")
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url=f"http://public.test:{port}/", depth=0)
    order = []
    async def submit(*args, **kwargs):
        order.append("submit")
    async def ack():
        order.append("ack")
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock(side_effect=ack))
    worker = FetcherWorker(settings)
    worker.publisher = Mock(channel=Mock(close=AsyncMock()))
    worker.request_shutdown = Mock()
    worker.origin_policy = Mock(acquire=AsyncMock(), record_rate_limit=AsyncMock(),
                                record_failure=AsyncMock())
    worker.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(
        uuid4(), datetime.now(UTC) + timedelta(seconds=120))),
        renew=AsyncMock(), submit=AsyncMock(side_effect=submit))
    worker.resolver = PublicResolver()
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=Route())) as session:
            worker.session = session
            await worker._consume_message(message)
        result = worker.lease_client.submit.await_args.args[0]
        assert isinstance(result, PageFailedEvent if status == 404 else FetchRetryRequestedEvent)
        if status != 404:
            delay = result.suggested_delay_seconds
            if header_kind == "long_decimal":
                assert delay == 3600
            else:
                assert 115 <= delay <= 120
            if status == 429:
                worker.origin_policy.record_rate_limit.assert_awaited_once_with(
                    f"http://public.test:{port}", delay
                )
        assert order == ["submit", "ack"]
        message.ack.assert_awaited_once()
        worker.publisher.channel.close.assert_not_awaited()
        worker.request_shutdown.assert_not_called()
    finally:
        await runner.cleanup()
