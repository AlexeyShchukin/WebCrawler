from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from crawler_fetcher.lease_client import FrontierLeaseClient, LeaseLostError


def _client(status, payload):
    response = MagicMock(status=status)
    response.json = AsyncMock(return_value=payload)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post.return_value = context
    sleep = AsyncMock()
    return FrontierLeaseClient(session, "http://frontier", sleep=sleep), session, sleep


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["claim", "renew"])
@pytest.mark.parametrize("token,date", [
    ("invalid-uuid", "2026-11-02T12:00:00+00:00"),
    ("e0edfa41-892b-43d8-b2cc-432fe438b67e", "not-a-date"),
])
async def test_lease_rejects_invalid_uuid_or_date(operation, token, date):
    client, _, _ = _client(200, {"owner_token": token, "lease_until": date})
    identity = {"crawl_id": uuid4(), "url_id": uuid4(), "fetch_attempt": 1}

    with pytest.raises(ValueError):
        if operation == "claim":
            await client.claim(**identity)
        else:
            await client.renew(**identity, owner_token=uuid4())


@pytest.mark.asyncio
async def test_lease_lost_is_not_retried():
    client, session, sleep = _client(409, None)

    with pytest.raises(LeaseLostError):
        await client.claim(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)

    assert session.post.call_count == 1
    sleep.assert_not_awaited()
