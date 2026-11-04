from unittest.mock import AsyncMock, MagicMock

import pytest

from crawler_fetcher.lease_client import FrontierLeaseClient


@pytest.mark.asyncio
async def test_post_without_json_returns_none():
    response = MagicMock(status=204)
    response.json = AsyncMock()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post.return_value = context

    result = await FrontierLeaseClient(session, "http://frontier")._post(
        "submit", {}, expect_json=False
    )

    assert result is None
    response.json.assert_not_awaited()
