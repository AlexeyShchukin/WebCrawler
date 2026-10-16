"""Each integration case gets an empty disposable database, including reliability rows."""

from uuid import uuid4

import pytest
from sqlalchemy import func, select

from crawler_frontier.models import OutboxEvent, ProcessedEvent


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["first", "second"])
async def test_reliability_rows_do_not_leak_between_cases(session_factory, case):
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0
        session.add(OutboxEvent(event_id=uuid4(), exchange_name="crawler.topic", routing_key="fetch.url", payload={"case": case}))
        session.add(ProcessedEvent(event_id=uuid4(), consumer_name="isolation.test"))
        await session.commit()
