from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.database import create_engine, create_session_factory
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.models import OutboxEvent
from crawler_frontier.outbox import OutboxPublisher
from crawler_frontier.settings import FrontierSettings


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(
        FrontierSettings(
            database_url=database_url_from_environment(),
            rabbitmq_url="amqp://crawler:secret@localhost/",
        )
    )
    yield create_session_factory(engine)
    await engine.dispose()


async def _create_pending_event(session_factory: async_sessionmaker[AsyncSession]) -> OutboxEvent:
    event = OutboxEvent(
        event_id=uuid4(),
        exchange_name="crawler.topic",
        routing_key="fetch.url",
        payload={"event_id": str(uuid4())},
        created_at=datetime(1970, 1, 1, tzinfo=UTC),
    )
    async with session_factory() as session, session.begin():
        session.add(event)
    return event


async def _delete_event(session_factory: async_sessionmaker[AsyncSession], event_id) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(delete(OutboxEvent).where(OutboxEvent.event_id == event_id))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_marks_event_published_after_confirm(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class ConfirmingExchange:
        def __init__(self) -> None:
            self.published: list[tuple[bytes, str]] = []

        async def publish(self, message, routing_key: str) -> None:
            self.published.append((message.body, routing_key))

    event = await _create_pending_event(session_factory)
    exchange = ConfirmingExchange()

    try:
        assert await OutboxPublisher(session_factory, {"crawler.topic": exchange}).publish_pending_once()

        async with session_factory() as session:
            stored = await session.get(OutboxEvent, event.event_id)

        assert stored is not None
        assert stored.published_at is not None
        assert stored.publish_attempts == 1
        assert stored.last_publish_error is None
        assert exchange.published == [(b'{"event_id":"' + str(event.payload["event_id"]).encode() + b'"}', "fetch.url")]
    finally:
        await _delete_event(session_factory, event.event_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_records_broker_error_and_leaves_event_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class FailingExchange:
        async def publish(self, message, routing_key: str) -> None:
            raise ConnectionError("broker unavailable")

    event = await _create_pending_event(session_factory)

    try:
        assert not await OutboxPublisher(
            session_factory, {"crawler.topic": FailingExchange()}
        ).publish_pending_once()

        async with session_factory() as session:
            stored = await session.get(OutboxEvent, event.event_id)

        assert stored is not None
        assert stored.published_at is None
        assert stored.publish_attempts == 1
        assert stored.last_publish_error == "ConnectionError: broker unavailable"
    finally:
        await _delete_event(session_factory, event.event_id)
