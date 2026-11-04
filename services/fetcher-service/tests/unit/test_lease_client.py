from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import aiohttp
import pytest
from crawler_contracts import PageFetchedEvent

from crawler_fetcher.lease_client import FrontierLeaseClient


def _grant_payload(owner_token):
    return {
        "owner_token": str(owner_token),
        "lease_until": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
    }


@pytest.mark.asyncio
async def test_claim_retries_timeout_with_the_same_owner_token() -> None:
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier:8000", sleep=sleep)
    owner_token = uuid4()
    client._new_owner_token = lambda: owner_token
    client._post = AsyncMock(side_effect=[TimeoutError(), _grant_payload(owner_token)])

    grant = await client.claim(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)

    assert grant.owner_token == owner_token
    assert client._post.await_count == 2
    assert client._post.await_args_list[0].args[1]["owner_token"] == str(owner_token)
    assert client._post.await_args_list[1].args[1]["owner_token"] == str(owner_token)
    sleep.assert_awaited_once_with(0.05)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [aiohttp.ClientConnectionError(), TimeoutError()])
async def test_renew_retries_transient_transport_errors(error) -> None:
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier:8000", sleep=sleep)
    owner_token = uuid4()
    client._post = AsyncMock(side_effect=[error, _grant_payload(owner_token)])

    grant = await client.renew(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1, owner_token=owner_token)

    assert grant.owner_token == owner_token
    assert client._post.await_count == 2
    sleep.assert_awaited_once_with(0.05)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
async def test_claim_retries_temporary_http_status_with_the_same_token(status) -> None:
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier:8000", sleep=sleep)
    owner_token = uuid4()
    client._new_owner_token = lambda: owner_token
    temporary = aiohttp.ClientResponseError(None, (), status=status)
    client._post = AsyncMock(side_effect=[temporary, _grant_payload(owner_token)])

    await client.claim(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)

    assert client._post.await_count == 2
    assert client._post.await_args_list[0].args[1]["owner_token"] == str(owner_token)
    assert client._post.await_args_list[1].args[1]["owner_token"] == str(owner_token)
    sleep.assert_awaited_once_with(0.05)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
async def test_claim_does_not_retry_non_temporary_http_status(status) -> None:
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier:8000", sleep=sleep)
    client._post = AsyncMock(side_effect=aiohttp.ClientResponseError(None, (), status=status))

    with pytest.raises(aiohttp.ClientResponseError):
        await client.claim(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)

    assert client._post.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_stops_after_three_temporary_http_responses() -> None:
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier:8000", sleep=sleep)
    temporary = aiohttp.ClientResponseError(None, (), status=503)
    client._post = AsyncMock(side_effect=[temporary, temporary, temporary])
    event = PageFetchedEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                             final_url="https://example.com", http_status=200,
                             content_type="text/html", content_ref="snapshot")

    with pytest.raises(aiohttp.ClientResponseError):
        await client.submit(event, owner_token=uuid4())

    assert client._post.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [0.05, 0.1]


@pytest.mark.asyncio
async def test_submit_retries_transport_failure_with_same_event_identity() -> None:
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier:8000", sleep=sleep)
    client._post = AsyncMock(side_effect=[aiohttp.ClientConnectionError("response lost"), TimeoutError(), {}])
    event = PageFetchedEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                             final_url="https://example.com", http_status=200,
                             content_type="text/html", content_ref="snapshot")

    await client.submit(event, owner_token=uuid4())

    assert client._post.await_count == 3
    first, second, third = (call.args[1]["event"] for call in client._post.await_args_list)
    assert first["event_id"] == second["event_id"] == third["event_id"] == str(event.event_id)
    assert [call.args[0] for call in sleep.await_args_list] == [0.05, 0.1]
