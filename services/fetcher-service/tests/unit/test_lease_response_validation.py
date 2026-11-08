from unittest.mock import AsyncMock, MagicMock

import pytest

from crawler_fetcher.lease_client import FrontierLeaseClient


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    {"owner_token": "abc123"},
    {"lease_until": "2026-11-02T12:00:00+00:00"},
    {"owner_token": 1, "lease_until": "2026-11-02T12:00:00+00:00"},
    {"owner_token": "abc123", "lease_until": None},
    [],
    None,
])
async def test_post_rejects_invalid_lease_response(payload):
    response = MagicMock(status=200)
    response.json = AsyncMock(return_value=payload)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post.return_value = context

    with pytest.raises(ValueError, match="Invalid Frontier lease response"):
        await FrontierLeaseClient(session, "http://frontier")._post("claim", {})


@pytest.mark.asyncio
async def test_post_accepts_valid_lease_response():
    payload = {"owner_token": "abc123", "lease_until": "2026-11-02T12:00:00+00:00"}
    response = MagicMock(status=200)
    response.json = AsyncMock(return_value=payload)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post.return_value = context

    assert await FrontierLeaseClient(session, "http://frontier")._post("claim", {}) == payload
