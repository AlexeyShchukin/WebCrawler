"""Worker recovery and lifecycle regressions."""

import asyncio

import pytest

from crawler_frontier.messaging.outbox import OutboxPublisher


@pytest.mark.integration
@pytest.mark.asyncio
async def test_permanent_outbox_configuration_error_terminates_unhealthy(session_factory):
    from uuid import uuid4

    from crawler_frontier.models import OutboxEvent

    event_id = uuid4()
    async with session_factory() as session, session.begin():
        session.add(OutboxEvent(event_id=event_id, exchange_name="unknown.exchange", routing_key="fetch.url", payload={}))
    worker = OutboxPublisher(session_factory, {})
    with pytest.raises(KeyError):
        await asyncio.wait_for(worker.run(asyncio.Event(), 0.001), 1)
    assert not worker.health.healthy
    async with session_factory() as session:
        row = await session.get(OutboxEvent, event_id)
        assert row.published_at is None and row.last_publish_error.startswith("KeyError:")
