import asyncio
import inspect
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

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


@pytest.mark.asyncio
async def test_fetches_html_from_a_validated_target(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.response("/page", body=b"<h1>hello</h1>")

    async with local_session() as session:
        fetched = await fetch_html(
            session, http_server.url("/page"), max_body_bytes=1024, chain_timeout_seconds=120, resolver=public_resolver
        )

    assert fetched.final_url == http_server.url("/page")
    assert fetched.status == 200
    assert fetched.content_type == "text/html"
    assert fetched.body == b"<h1>hello</h1>"


@pytest.mark.asyncio
async def test_manual_redirect_revalidates_target(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.redirect("/start", "http://127.0.0.1/private")

    async with local_session() as session:
        with pytest.raises(NetworkFailure, match="ssrf_blocked"):
            await fetch_html(
                session,
                http_server.url("/start"),
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )


@pytest.mark.asyncio
async def test_follows_a_validated_redirect(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.redirect("/start", "/destination")
    http_server.response("/destination", body=b"redirected")

    async with local_session() as session:
        fetched = await fetch_html(
            session, http_server.url("/start"), max_body_bytes=1024, chain_timeout_seconds=120, resolver=public_resolver
        )

    assert fetched.final_url == http_server.url("/destination")
    assert fetched.body == b"redirected"


@pytest.mark.asyncio
@pytest.mark.parametrize("location", [None, "http://["])
async def test_rejects_empty_or_invalid_redirect_location(
    http_server: HttpServer, public_resolver: StubResolver, location: str | None
) -> None:
    async def redirect(_: web.Request) -> web.Response:
        headers = {} if location is None else {"Location": location}
        return web.Response(status=302, headers=headers)

    http_server.handlers["/redirect"] = redirect

    async with local_session() as session:
        with pytest.raises(NetworkFailure, match="invalid_redirect"):
            await fetch_html(
                session,
                http_server.url("/redirect"),
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )


@pytest.mark.asyncio
async def test_rejects_more_than_ten_redirects(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.redirect("/loop", "/loop")

    async with local_session() as session:
        with pytest.raises(NetworkFailure, match="too_many_redirects"):
            await fetch_html(
                session,
                http_server.url("/loop"),
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )


@pytest.mark.asyncio
async def test_stops_stream_after_response_limit(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.stream("/large", b"x" * 1025, content_type="text/html")

    async with local_session() as session:
        with pytest.raises(NetworkFailure, match="response_too_large"):
            await fetch_html(
                session,
                http_server.url("/large"),
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )


@pytest.mark.asyncio
async def test_returns_http_error_status_for_retry_classification(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.response("/missing", body=b"missing", status=404)
    http_server.response("/limited", body=b"wait", status=429)

    async with local_session() as session:
        missing = await fetch_html(
            session, http_server.url("/missing"), max_body_bytes=1024, chain_timeout_seconds=120, resolver=public_resolver
        )
        limited = await fetch_html(
            session, http_server.url("/limited"), max_body_bytes=1024, chain_timeout_seconds=120, resolver=public_resolver
        )

    assert missing.status == 404
    assert limited.status == 429


@pytest.mark.asyncio
async def test_rejects_unsupported_response_mime(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.response("/json", body=b"{}", content_type="application/json")

    async with local_session() as session:
        with pytest.raises(NetworkFailure, match="unsupported_mime"):
            await fetch_html(
                session,
                http_server.url("/json"),
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )


@pytest.mark.asyncio
async def test_accepts_html_content_type_with_parameters(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    async def html(_: web.Request) -> web.Response:
        return web.Response(
            body=b"<p>parameterized</p>",
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    http_server.handlers["/html-with-charset"] = html

    async with local_session() as session:
        fetched = await fetch_html(
            session,
            http_server.url("/html-with-charset"),
            max_body_bytes=1024,
            chain_timeout_seconds=120, resolver=public_resolver,
        )

    assert fetched.content_type == "text/html"


@pytest.mark.asyncio
async def test_classifies_socket_read_timeout(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    http_server.delayed_response("/slow")

    async with local_session(sock_read=0.01) as session:
        with pytest.raises(NetworkFailure, match="timeout"):
            await fetch_html(
                session,
                http_server.url("/slow"),
                max_body_bytes=1024,
                chain_timeout_seconds=120, resolver=public_resolver,
            )

@pytest.mark.asyncio
async def test_http_error_preserves_retry_after_without_reading_body(
    http_server: HttpServer, public_resolver: StubResolver
) -> None:
    async def limited(_: web.Request) -> web.Response:
        return web.Response(status=429, body=b"large error body", headers={"Retry-After": "120"})

    http_server.handlers["/limited-plain"] = limited
    async with local_session() as session:
        result = await fetch_html(
            session, http_server.url("/limited-plain"), max_body_bytes=1, chain_timeout_seconds=120, resolver=public_resolver
        )
    assert result.status == 429
    assert result.retry_after == "120"
    assert result.body == b""

@pytest.mark.asyncio
async def test_redirect_admission_happens_before_redirect_request(http_server, public_resolver):
    from unittest.mock import AsyncMock

    http_server.redirect('/start', '/destination')
    http_server.response('/destination', body=b'ok')
    admit = AsyncMock(side_effect=RuntimeError('admission denied'))
    async with local_session() as session:
        with pytest.raises(RuntimeError, match='admission denied'):
            await fetch_html(session, http_server.url('/start'), max_body_bytes=1000,
                             chain_timeout_seconds=120, resolver=public_resolver, admit_redirect=admit)
    admit.assert_awaited_once()



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


@pytest.mark.asyncio
async def test_rebinding_does_not_connect_to_local_server(settings: FetcherSettings):
    connections = []

    async def connected(reader, writer):
        connections.append(True)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(connected, "127.0.0.1", 0)
    dns = Rebinding()
    resolver = PublicResolver(dns)
    try:
        async with create_http_session(settings, resolver) as session:
            with pytest.raises(NetworkFailure, match="ssrf_blocked"):
                await fetch_html(
                    session,
                    f"http://rebind.test:{server.sockets[0].getsockname()[1]}/",
                    max_body_bytes=1024,
                    resolver=resolver,
                    chain_timeout_seconds=120,
                )
        await asyncio.sleep(0.05)
        assert dns.calls == 2
        assert not connections
    finally:
        server.close()
        await server.wait_closed()



def test_chain_timeout_is_required() -> None:
    assert inspect.signature(fetch_html).parameters["chain_timeout_seconds"].default is inspect.Parameter.empty


@pytest.mark.asyncio
async def test_chain_timeout_covers_multiple_redirects(http_server, public_resolver):
    visited = []

    async def redirect(request):
        visited.append(request.path)
        await asyncio.sleep(0.4)
        raise web.HTTPFound(f"/{len(visited)}")

    for path in ("/start", "/1", "/2", "/3"):
        http_server.handlers[path] = redirect

    connector = aiohttp.TCPConnector(resolver=LocalRoutingResolver())
    timeout = aiohttp.ClientTimeout(total=1.0, sock_read=1.0)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        with pytest.raises(NetworkFailure, match="timeout"):
            await fetch_html(session, http_server.url("/start"), max_body_bytes=100,
                             resolver=public_resolver, chain_timeout_seconds=1.0)
    assert len(visited) >= 2
