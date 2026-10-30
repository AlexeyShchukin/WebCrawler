"""SSRF-safe, bounded HTTP retrieval primitives for Fetcher."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

import aiohttp
from yarl import URL

if TYPE_CHECKING:
    from crawler_fetcher.settings import FetcherSettings

MAX_REDIRECTS = 10
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class Resolver(Protocol):
    """Resolve a hostname to every candidate address before connecting."""

    async def resolve(self, host: str) -> Sequence[str]: ...


class PublicResolver(aiohttp.abc.AbstractResolver):
    """An aiohttp resolver that fails closed on non-public DNS answers."""

    def __init__(self, resolver: aiohttp.abc.AbstractResolver) -> None:
        self._resolver = resolver

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: socket.AddressFamily = socket.AF_UNSPEC,
    ) -> list[aiohttp.abc.ResolveResult]:
        try:
            results = await self._resolver.resolve(host, port, family)
        except NetworkFailure:
            raise
        except Exception as error:
            raise NetworkFailure("dns_failure") from error
        addresses: list[str] = []
        for result in results:
            if not isinstance(result, Mapping):
                raise NetworkFailure("dns_failure")
            address = result.get("host")
            if not isinstance(address, str) or not address:
                raise NetworkFailure("dns_failure")
            addresses.append(address)
        _require_public_addresses(addresses)
        return results

    async def close(self) -> None:
        await self._resolver.close()


@dataclass(frozen=True, slots=True)
class FetchedResponse:
    """An HTML response or an HTTP error with its retry metadata."""

    final_url: str
    status: int
    content_type: str
    body: bytes
    retry_after: str | None = None


class NetworkFailure(Exception):
    """A stable, safe-to-classify network failure."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        super().__init__(kind)


def create_http_session(
        settings: FetcherSettings,
        resolver: PublicResolver
) -> aiohttp.ClientSession:
    """Create the process-owned session with connector-level SSRF protection."""
    connector = aiohttp.TCPConnector(
        limit=settings.fetch_concurrency,
        resolver=resolver,
        use_dns_cache=False,
    )
    timeout = aiohttp.ClientTimeout(
        total=settings.http_total_timeout_seconds,
        connect=settings.http_connect_timeout_seconds,
        sock_read=settings.http_read_timeout_seconds,
    )
    return aiohttp.ClientSession(connector=connector, timeout=timeout)


def _validated_port(url: URL) -> int | None:
    try:
        return url.port
    except ValueError as error:
        raise NetworkFailure("invalid_url") from error


async def _resolve_addresses(host: str, resolver: Resolver) -> Sequence[str]:
    try:
        answers = await resolver.resolve(host)
    except NetworkFailure:
        raise
    except Exception as error:
        raise NetworkFailure("dns_failure") from error
    if not answers:
        raise NetworkFailure("dns_failure")
    addresses: list[str] = []
    for answer in answers:
        if isinstance(answer, str):
            addresses.append(answer)
        elif isinstance(answer, dict) and isinstance(answer.get("host"), str):
            addresses.append(cast(str, answer["host"]))
        else:
            raise NetworkFailure("dns_failure")
    return addresses


def _require_public_addresses(addresses: Iterable[str]) -> None:
    candidates = list(addresses)
    if not candidates:
        raise NetworkFailure("dns_failure")
    try:
        if any(not ipaddress.ip_address(address).is_global for address in candidates):
            raise NetworkFailure("ssrf_blocked")
    except (ValueError, TypeError) as error:
        raise NetworkFailure("dns_failure") from error


async def validate_public_target(url: URL, resolver: Resolver) -> URL:
    """Validate a single request target and reject every non-public address."""
    if url.scheme not in {"http", "https"}:
        raise NetworkFailure("invalid_url")
    if url.user is not None or url.password is not None:
        raise NetworkFailure("invalid_url")
    host = url.host
    if host is None or not host:
        raise NetworkFailure("invalid_url")
    port = _validated_port(url)
    if port is not None and not 1 <= port <= 65535:
        raise NetworkFailure("invalid_url")

    try:
        addresses = [str(ipaddress.ip_address(host))]
    except ValueError:
        addresses = await _resolve_addresses(host, resolver)

    _require_public_addresses(addresses)
    return url


def _redirect_target(current_url: URL, location: str | None) -> URL:
    if not location:
        raise NetworkFailure("invalid_redirect")
    try:
        return current_url.join(URL(location))
    except ValueError as error:
        raise NetworkFailure("invalid_redirect") from error


async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_body_bytes: int,
    resolver: Resolver,
    admit_redirect: Callable[[str], Awaitable[None]] | None = None,
    chain_timeout_seconds: float,
) -> FetchedResponse:
    """Fetch within a deadline shared by DNS, redirects, and response reading."""
    try:
        async with asyncio.timeout(chain_timeout_seconds):
            return await _fetch_html(
                session,
                url,
                max_body_bytes=max_body_bytes,
                resolver=resolver,
                admit_redirect=admit_redirect,
            )
    except TimeoutError as error:
        raise NetworkFailure("timeout") from error


async def _fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_body_bytes: int,
    resolver: Resolver,
    admit_redirect: Callable[[str], Awaitable[None]] | None = None,
) -> FetchedResponse:
    """Fetch one validated HTML resource while manually validating redirects."""
    if max_body_bytes < 1:
        raise NetworkFailure("invalid_response_limit")
    try:
        current_url = URL(url)
    except ValueError as error:
        raise NetworkFailure("invalid_url") from error

    redirects = 0
    while True:
        await validate_public_target(current_url, resolver)
        if redirects and admit_redirect is not None:
            await admit_redirect(str(current_url.origin()))
        response: aiohttp.ClientResponse | None = None
        try:
            response = await session.get(str(current_url), allow_redirects=False)
            if response.status in _REDIRECT_STATUSES:
                if redirects >= MAX_REDIRECTS:
                    raise NetworkFailure("too_many_redirects")
                current_url = _redirect_target(
                    current_url, response.headers.get("Location")
                )
                redirects += 1
                continue

            content_type = response.content_type.lower()
            if response.status >= 400:
                return FetchedResponse(
                    final_url=str(current_url),
                    status=response.status,
                    content_type=content_type,
                    body=b"",
                    retry_after=response.headers.get("Retry-After"),
                )
            if content_type != "text/html":
                raise NetworkFailure("unsupported_mime")

            chunks: list[bytes] = []
            body_size = 0
            async for chunk in response.content.iter_chunked(64 * 1024):
                body_size += len(chunk)
                if body_size > max_body_bytes:
                    raise NetworkFailure("response_too_large")
                chunks.append(chunk)
            return FetchedResponse(
                final_url=str(current_url),
                status=response.status,
                content_type=content_type,
                body=b"".join(chunks),
            )
        except NetworkFailure:
            raise
        except (TimeoutError, aiohttp.ServerTimeoutError) as error:
            raise NetworkFailure("timeout") from error
        except aiohttp.ClientError as error:
            raise NetworkFailure("connection_failed") from error
        finally:
            if response is not None:
                response.release()
