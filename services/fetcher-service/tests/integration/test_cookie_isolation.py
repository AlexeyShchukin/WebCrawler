import asyncio
import socket
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from crawler_contracts import FetchUrlEvent

from crawler_fetcher.lease_client import LeaseGrant
from crawler_fetcher.network import create_http_session
from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.worker import FetcherWorker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class LocalRoute(aiohttp.abc.AbstractResolver):
    async def resolve(self, host, port=0, family=socket.AF_UNSPEC):
        return [{"hostname": host, "host": "127.0.0.1", "port": port,
                 "family": socket.AF_INET, "proto": 0, "flags": 0}]

    async def close(self):
        pass


class PublicResolver:
    async def resolve(self, host):
        return ["8.8.8.8"]


@pytest_asyncio.fixture
async def cookie_server() -> AsyncIterator[tuple[int, list]]:
    observed = []

    async def handler(request):
        observed.append((request.path, request.headers.get("Cookie"), request.transport))
        if request.path == "/redirect":
            response = web.Response(status=302, headers={"Location": "/destination"})
            response.set_cookie("redirect_cookie", "redirect-state")
            return response
        response = web.Response(text="<html>public</html>", content_type="text/html")
        if request.path == "/set":
            response.set_cookie("session", "first-crawl")
        return response

    app = web.Application()
    app.router.add_get("/{path:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield port, observed
    finally:
        await runner.cleanup()


@pytest_asyncio.fixture
async def cookie_worker() -> AsyncIterator[FetcherWorker]:
    settings = FetcherSettings(rabbitmq_url="amqp://localhost/", redis_url="redis://localhost/",
                               object_storage_endpoint="http://localhost:9000",
                               object_storage_access_key="a", object_storage_secret_key="b",
                               object_storage_bucket="crawler")
    worker = FetcherWorker(settings)
    worker.origin_policy = Mock(acquire=AsyncMock(), record_success=AsyncMock())
    worker.storage = Mock(put_for_event=AsyncMock(return_value="snapshot"))
    worker.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(
        uuid4(), datetime.now(UTC) + timedelta(minutes=2))), renew=AsyncMock(), submit=AsyncMock())
    worker.resolver = PublicResolver()
    async with create_http_session(settings, LocalRoute()) as session:
        worker.session = session
        yield worker


async def deliver(worker, port, path, *, crawl_id, url_id=None, attempt=1):
    event = FetchUrlEvent(crawl_id=crawl_id, url_id=url_id or uuid4(), fetch_attempt=attempt,
                          url=f"http://public.test:{port}{path}", depth=0)
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock())
    await worker.handle_message(message)
    message.ack.assert_awaited_once()


async def test_separate_crawls_retry_and_redirect_never_send_stored_cookies(
        cookie_server, cookie_worker):
    port, observed = cookie_server
    first_crawl, second_crawl, first_url = uuid4(), uuid4(), uuid4()
    await deliver(cookie_worker, port, "/set", crawl_id=first_crawl, url_id=first_url)
    await deliver(cookie_worker, port, "/other-crawl", crawl_id=second_crawl)
    await deliver(cookie_worker, port, "/retry", crawl_id=first_crawl, url_id=first_url, attempt=2)
    await deliver(cookie_worker, port, "/redirect", crawl_id=first_crawl)

    assert [path for path, _, _ in observed] == [
        "/set", "/other-crawl", "/retry", "/redirect", "/destination"
    ]
    assert all(cookie is None for _, cookie, _ in observed)
    assert observed[0][2] is observed[1][2]
    assert not cookie_worker.session.closed


async def test_concurrent_crawls_do_not_inherit_cookie_from_prior_response(
        cookie_server, cookie_worker):
    port, observed = cookie_server
    await deliver(cookie_worker, port, "/set", crawl_id=uuid4())
    await asyncio.gather(*(deliver(cookie_worker, port, f"/concurrent-{i}", crawl_id=uuid4())
                           for i in range(6)))
    assert len(observed) == 7
    assert all(cookie is None for _, cookie, _ in observed)
