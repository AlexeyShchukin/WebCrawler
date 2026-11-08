"""Fetcher client for Frontier's fenced fetch-lease API."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Literal, TypedDict, overload
from uuid import UUID, uuid4

import aiohttp
from crawler_contracts import (
    FetchRetryRequestedEvent,
    PageFailedEvent,
    PageFetchedEvent,
)


class LeaseLostError(Exception):
    """Frontier rejected an operation because this worker no longer owns it."""


class LeaseResponse(TypedDict):
    owner_token: str
    lease_until: str


@dataclass(frozen=True, slots=True)
class LeaseGrant:
    owner_token: UUID
    lease_until: datetime


class FrontierLeaseClient:
    """Manage fetch leases through Frontier's HTTP API with bounded retries."""

    _MAX_ATTEMPTS = 3
    _INITIAL_RETRY_SECONDS = 0.05
    _MAX_RETRY_AFTER_SECONDS = 120.0
    _RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

    def __init__(
            self,
            session: aiohttp.ClientSession,
            base_url: str,
            *,
            sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._sleep = sleep

    async def claim(
            self,
            *,
            crawl_id: UUID,
            url_id: UUID,
            fetch_attempt: int
    ) -> LeaseGrant:
        owner_token = self._new_owner_token()
        data = await self._post_with_retry("claim", {
            "crawl_id": str(crawl_id),
            "url_id": str(url_id),
            "fetch_attempt": fetch_attempt,
            "owner_token": str(owner_token),
        })
        return LeaseGrant(
            UUID(data["owner_token"]),
            datetime.fromisoformat(data["lease_until"])
        )

    async def renew(
            self,
            *,
            crawl_id: UUID,
            url_id: UUID,
            fetch_attempt: int,
            owner_token: UUID
    ) -> LeaseGrant:
        data = await self._post_with_retry("renew", {
            "crawl_id": str(crawl_id),
            "url_id": str(url_id),
            "fetch_attempt": fetch_attempt,
            "owner_token": str(owner_token),
        })
        return LeaseGrant(
            UUID(data["owner_token"]),
            datetime.fromisoformat(data["lease_until"])
        )

    async def submit(
            self,
            event: PageFetchedEvent | PageFailedEvent | FetchRetryRequestedEvent,
            *, owner_token: UUID
    ) -> None:
        payload = {
            "crawl_id": str(event.crawl_id),
            "url_id": str(event.url_id),
            "fetch_attempt": event.fetch_attempt,
            "owner_token": str(owner_token),
            "event": event.model_dump(mode="json"),
        }
        await self._post_with_retry("submit", payload, expect_json=False)

    @staticmethod
    def _new_owner_token() -> UUID:
        return uuid4()

    @overload
    async def _post_with_retry(
            self, operation: str, payload: dict[str, object], *,
            expect_json: Literal[True] = True,
    ) -> LeaseResponse: ...

    @overload
    async def _post_with_retry(
            self, operation: str, payload: dict[str, object], *,
            expect_json: Literal[False],
    ) -> None: ...

    async def _post_with_retry(
            self,
            operation: str,
            payload: dict[str, object],
            *,
            expect_json: bool = True
    ) -> LeaseResponse | None:
        last_error: Exception | None = None
        for attempt in range(self._MAX_ATTEMPTS):
            retry_after = None
            try:
                return await self._post(operation, payload, expect_json=expect_json)
            except (aiohttp.ClientConnectionError, TimeoutError) as error:
                last_error = error
            except aiohttp.ClientResponseError as error:
                if error.status not in self._RETRYABLE_HTTP_STATUSES:
                    raise
                last_error = error
                if error.status in {429, 503} and error.headers:
                    retry_after = _parse_retry_after(error.headers.get("Retry-After"))
                    if retry_after is not None and retry_after > self._MAX_RETRY_AFTER_SECONDS:
                        retry_after = None
            if attempt == self._MAX_ATTEMPTS - 1:
                assert last_error is not None
                raise last_error
            delay = self._INITIAL_RETRY_SECONDS * (2 ** attempt)
            await self._sleep(delay if retry_after is None else retry_after)
        raise AssertionError("retry loop exhausted without returning or raising")

    async def _post(
            self,
            operation: str,
            payload: dict[str, object],
            *,
            expect_json: bool = True
    ) -> LeaseResponse | None:
        async with self._session.post(
                f"{self._base_url}/internal/fetch-leases/{operation}",
                json=payload
        ) as response:
            if response.status == 409:
                raise LeaseLostError()
            response.raise_for_status()
            if not expect_json:
                return None
            data: object = await response.json()
            if not isinstance(data, dict):
                raise ValueError("Invalid Frontier lease response: expected an object")  # noqa: TRY004
            owner_token = data.get("owner_token")
            lease_until = data.get("lease_until")
            if not isinstance(owner_token, str) or not isinstance(lease_until, str):
                raise ValueError(  # noqa: TRY004
                    "Invalid Frontier lease response: expected owner_token and lease_until strings"
                )
            return {"owner_token": owner_token, "lease_until": lease_until}


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdecimal():
        try:
            return float(value)
        except (ValueError, OverflowError):
            return None
    try:
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            return None
        return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError, OverflowError, IndexError):
        return None
