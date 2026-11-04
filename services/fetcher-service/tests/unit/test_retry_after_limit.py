from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from crawler_fetcher.lease_client import FrontierLeaseClient


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("value", ["86400", "9" * 5000, "date"])
async def test_long_retry_after_uses_exponential_backoff(status, value):
    if value == "date":
        value = format_datetime(datetime.now(UTC) + timedelta(days=1), usegmt=True)
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier", sleep=sleep)
    error = aiohttp.ClientResponseError(None, (), status=status, headers={"Retry-After": value})
    client._post = AsyncMock(side_effect=[error, error, {}])

    await client._post_with_retry("submit", {}, expect_json=False)

    assert [call.args[0] for call in sleep.await_args_list] == [0.05, 0.1]
    assert client._post.await_count == 3


@pytest.mark.asyncio
async def test_date_parser_index_error_uses_exponential_backoff():
    sleep = AsyncMock()
    client = FrontierLeaseClient(AsyncMock(), "http://frontier", sleep=sleep)
    error = aiohttp.ClientResponseError(None, (), status=429, headers={"Retry-After": "invalid"})
    client._post = AsyncMock(side_effect=[error, {}])

    with patch("crawler_fetcher.lease_client.parsedate_to_datetime", side_effect=IndexError):
        await client._post_with_retry("submit", {}, expect_json=False)

    sleep.assert_awaited_once_with(0.05)
