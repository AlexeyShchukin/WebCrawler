import asyncio
import inspect
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from yarl import URL

from crawler_fetcher.network import (
    NetworkFailure,
    PublicResolver,
    create_http_session,
    fetch_html,
    validate_public_target,
)
from crawler_fetcher.settings import FetcherSettings


class StubResolver:
    def __init__(self, answers: dict[str, list[str]]) -> None:
        self.answers = answers

    async def resolve(self, host: str) -> list[str]:
        return self.answers[host]


@pytest.mark.asyncio
@pytest.mark.parametrize(("mime", "limit", "error"), [
    ("application/xhtml+xml", 100, None),
    ("application/xhtml+xml", 1, "response_too_large"),
    ("application/xml", 100, "unsupported_mime"),
])
async def test_xhtml_and_rejected_responses_are_released(mime, limit, error):
    async def chunks(size):
        yield b"<html/>"

    response = Mock(status=200, content_type=mime, content=Mock(iter_chunked=chunks))
    session = Mock(get=AsyncMock(return_value=response))
    resolver = StubResolver({"public.test": ["8.8.8.8"]})
    if error:
        with pytest.raises(NetworkFailure, match=error):
            await fetch_html(session, "https://public.test/", max_body_bytes=limit,
                             resolver=resolver, chain_timeout_seconds=1)
    else:
        await fetch_html(session, "https://public.test/", max_body_bytes=limit,
                         resolver=resolver, chain_timeout_seconds=1)
    response.release.assert_called_once()


class LocalRoutingResolver(aiohttp.abc.AbstractResolver):
    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = 0,
    ) -> list[aiohttp.abc.ResolveResult]:
        return [
            {
                "hostname": host,
                "host": "127.0.0.1",
                "port": port,
                "family": family,
                "proto": 0,
                "flags": 0,
            }
        ]

    async def close(self) -> None:
        pass


class FixedAiohttpResolver(aiohttp.abc.AbstractResolver):
    def __init__(self, address: str) -> None:
        self.address = address

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = 0,
    ) -> list[aiohttp.abc.ResolveResult]:
        return [
            {
                "hostname": host,
                "host": self.address,
                "port": port,
                "family": family,
                "proto": 0,
                "flags": 0,
            }
        ]

    async def close(self) -> None:
        pass


class MalformedAiohttpResolver(aiohttp.abc.AbstractResolver):
    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = 0,
    ) -> list[aiohttp.abc.ResolveResult]:
        return [{}]

    async def close(self) -> None:
        pass


class HttpServer:
    def __init__(self) -> None:
        self.app = web.Application()
        self.runner = web.AppRunner(self.app)
        self.port: int | None = None
        self.handlers: dict[str, Callable[[web.Request], web.StreamResponse]] = {}
        self.app.router.add_get("/{path:.*}", self._dispatch)

    async def _dispatch(self, request: web.Request) -> web.StreamResponse:
        return await self.handlers[request.path](request)

    def url(self, path: str) -> str:
        assert self.port is not None
        return f"http://public.test:{self.port}{path}"

    def redirect(self, path: str, target: str) -> None:
        async def handler(_: web.Request) -> web.Response:
            raise web.HTTPFound(target)

        self.handlers[path] = handler

    def stream(self, path: str, body: bytes, *, content_type: str) -> None:
        async def handler(_: web.Request) -> web.StreamResponse:
            response = web.StreamResponse(headers={"Content-Type": content_type})
            await response.prepare(_)
            await response.write(body)
            await response.write_eof()
            return response

        self.handlers[path] = handler

    def response(
        self,
        path: str,
        *,
        body: bytes,
        status: int = 200,
        content_type: str = "text/html",
    ) -> None:
        async def handler(_: web.Request) -> web.Response:
            return web.Response(body=body, status=status, content_type=content_type)

        self.handlers[path] = handler

    def delayed_response(self, path: str) -> None:
        async def handler(_: web.Request) -> web.Response:
            await asyncio.sleep(0.1)
            return web.Response(text="slow", content_type="text/html")

        self.handlers[path] = handler


@pytest_asyncio.fixture
async def http_server() -> AsyncIterator[HttpServer]:
    server = HttpServer()
    await server.runner.setup()
    site = web.TCPSite(server.runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets if site._server else []
    server.port = sockets[0].getsockname()[1]
    try:
        yield server
    finally:
        await server.runner.cleanup()


@asynccontextmanager
async def local_session(*, sock_read: float = 1.0) -> AsyncIterator[aiohttp.ClientSession]:
    connector = aiohttp.TCPConnector(resolver=LocalRoutingResolver())
    timeout = aiohttp.ClientTimeout(connect=1.0, sock_read=sock_read)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        yield session


@pytest.fixture
def public_resolver() -> StubResolver:
    return StubResolver({"public.test": ["8.8.8.8"]})


@pytest.fixture
def settings() -> FetcherSettings:
    return FetcherSettings(
        rabbitmq_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0",
        object_storage_endpoint="http://localhost:9000",
        object_storage_access_key="minioadmin",
        object_storage_secret_key="minioadmin",
        object_storage_bucket="crawler-content",
    )


@pytest.mark.asyncio
async def test_rejects_private_dns_answer() -> None:
    resolver = StubResolver({"internal.test": ["10.0.0.7"]})

    with pytest.raises(NetworkFailure, match="ssrf_blocked"):
        await validate_public_target(URL("http://internal.test/"), resolver)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "ftp://public.test/file",
        "http://user:password@public.test/",
        "http:///missing-host",
        "http://public.test:0/",
    ],
)
async def test_rejects_malformed_or_unsafe_url_forms(url: str, public_resolver: StubResolver) -> None:
    with pytest.raises(NetworkFailure, match="invalid_url"):
        await validate_public_target(URL(url), public_resolver)


@pytest.mark.asyncio
async def test_rejects_an_out_of_range_port_before_connecting(
    public_resolver: StubResolver,
) -> None:
    async with local_session() as session:
        with pytest.raises(NetworkFailure, match="invalid_url"):
            await fetch_html(
                session,
                "http://public.test:65536/",
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )


@pytest.mark.asyncio
async def test_connector_resolver_rejects_a_private_dns_result(
    settings: FetcherSettings,
) -> None:
    resolver = PublicResolver(FixedAiohttpResolver("127.0.0.1"))
    session = create_http_session(settings, resolver)
    try:
        with pytest.raises(NetworkFailure, match="ssrf_blocked"):
            await session.get("http://public.test/", allow_redirects=False)
        assert session.connector is not None
        assert session.connector._use_dns_cache is False
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_connector_resolver_normalizes_malformed_dns_records() -> None:
    resolver = PublicResolver(MalformedAiohttpResolver())

    with pytest.raises(NetworkFailure, match="dns_failure"):
        await resolver.resolve("public.test")

























class Rebinding(aiohttp.abc.AbstractResolver):
    def __init__(self):
        self.calls = 0

    async def resolve(self, host, port=0, family=socket.AF_UNSPEC):
        self.calls += 1
        return [
            {
                "hostname": host,
                "host": "8.8.8.8" if self.calls == 1 else "127.0.0.1",
                "port": port,
                "family": socket.AF_INET,
                "proto": 0,
                "flags": 0,
            }
        ]

    async def close(self):
        pass





def test_chain_timeout_is_required() -> None:
    assert inspect.signature(fetch_html).parameters["chain_timeout_seconds"].default is inspect.Parameter.empty


