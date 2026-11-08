from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from crawler_fetcher.lease_client import FrontierLeaseClient


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["claim", "renew"])
async def test_lease_operation_parses_valid_json_response(operation):
    token = UUID("e0edfa41-892b-43d8-b2cc-432fe438b67e")
    response = MagicMock(status=200)
    response.json = AsyncMock(return_value={
        "owner_token": str(token), "lease_until": "2026-11-02T12:00:00+00:00"
    })
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post.return_value = context
    client = FrontierLeaseClient(session, "http://frontier")
    identity = {"crawl_id": uuid4(), "url_id": uuid4(), "fetch_attempt": 1}

    if operation == "claim":
        grant = await client.claim(**identity)
    else:
        grant = await client.renew(**identity, owner_token=token)

    assert grant.owner_token == token
    assert grant.lease_until == datetime(2026, 11, 2, 12, tzinfo=UTC)
