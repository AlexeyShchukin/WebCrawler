from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import AsyncMock

import aiohttp
import pytest

from crawler_fetcher.lease_client import FrontierLeaseClient


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("header", ["120", "date"])
async def test_retry_after_is_respected(status, header):
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier", sleep=sleep)
    value = (
        format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True)
        if header == "date" else header
    )
    error = aiohttp.ClientResponseError(None, (), status=status, headers={"Retry-After": value})
    client._post = AsyncMock(side_effect=[error, {}])

    await client._post_with_retry("submit", {}, expect_json=False)

    delay = sleep.await_args.args[0]
    assert 118 <= delay <= 120 if header == "date" else delay == 120


@pytest.mark.asyncio
@pytest.mark.parametrize("header", [None, "", "-1", "1.5", "tomorrow"])
async def test_invalid_or_missing_retry_after_uses_backoff(header):
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier", sleep=sleep)
    headers = {} if header is None else {"Retry-After": header}
    error = aiohttp.ClientResponseError(None, (), status=429, headers=headers)
    client._post = AsyncMock(side_effect=[error, error, {}])

    await client._post_with_retry("submit", {}, expect_json=False)

    assert [call.args[0] for call in sleep.await_args_list] == [0.05, 0.1]


@pytest.mark.asyncio
async def test_retry_after_is_ignored_for_other_retryable_statuses():
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier", sleep=sleep)
    error = aiohttp.ClientResponseError(None, (), status=500, headers={"Retry-After": "120"})
    client._post = AsyncMock(side_effect=[error, {}])

    await client._post_with_retry("submit", {}, expect_json=False)

    sleep.assert_awaited_once_with(0.05)


@pytest.mark.asyncio
async def test_past_retry_after_date_does_not_delay():
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier", sleep=sleep)
    error = aiohttp.ClientResponseError(
        None, (), status=503, headers={"Retry-After": "Mon, 02 Nov 2020 12:00:00 GMT"}
    )
    client._post = AsyncMock(side_effect=[error, {}])

    await client._post_with_retry("submit", {}, expect_json=False)

    sleep.assert_awaited_once_with(0)
