import asyncio
from uuid import uuid4

import pytest
from aio_pika.exceptions import ChannelInvalidStateError
from pamqp.commands import Basic
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.messaging.outbox import OutboxPublisher
from crawler_frontier.models import OutboxEvent
from tests.integration.helpers import _create_pending_event


async def _delete_event(session_factory: async_sessionmaker[AsyncSession], event_id) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(delete(OutboxEvent).where(OutboxEvent.event_id == event_id))


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("exchange_name,routing_key", [
    ("crawler.topic", "fetch.url"), ("crawler.retry", "fetch.url.retry.30s"),
])
async def test_publisher_marks_event_published_after_confirm(
    session_factory: async_sessionmaker[AsyncSession],
    exchange_name: str,
    routing_key: str,
) -> None:
    class ConfirmingExchange:
        def __init__(self) -> None:
            self.published: list[tuple[bytes, str]] = []

        async def publish(self, message, routing_key: str, **kwargs):
            assert kwargs == {"mandatory": True, "timeout": 5.0}
            self.published.append((message.body, routing_key))
            return Basic.Ack(delivery_tag=1)

    event = await _create_pending_event(session_factory, exchange_name=exchange_name, routing_key=routing_key)
    exchange = ConfirmingExchange()

    try:
        assert await OutboxPublisher(session_factory, {exchange_name: exchange}).publish_pending_once()

        async with session_factory() as session:
            stored = await session.get(OutboxEvent, event.event_id)

        assert stored is not None
        assert stored.exchange_name == exchange_name
        assert stored.routing_key == routing_key
        assert stored.published_at is not None
        assert stored.publish_attempts == 1
        assert stored.last_publish_error is None
        assert exchange.published == [(b'{"event_id":"' + str(event.payload["event_id"]).encode() + b'"}', routing_key)]
    finally:
        await _delete_event(session_factory, event.event_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_records_broker_error_and_leaves_event_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class FailingExchange:
        async def publish(self, message, routing_key: str, **kwargs) -> None:
            raise ConnectionError("broker unavailable")

    event = await _create_pending_event(session_factory)

    try:
        with pytest.raises(ConnectionError, match="broker unavailable"):
            await OutboxPublisher(
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


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", [
    pytest.param(None, id="missing"),
    pytest.param(False, id="false"),
    pytest.param(Basic.Nack(delivery_tag=1), id="nack"),
])
async def test_missing_positive_confirmation_never_marks_published(session_factory, confirmation):
    class UnconfirmedExchange:
        async def publish(self, message, routing_key, **kwargs):
            return confirmation

    event = await _create_pending_event(session_factory)
    try:
        with pytest.raises(ConnectionError, match="did not positively confirm publication"):
            await OutboxPublisher(
                session_factory, {"crawler.topic": UnconfirmedExchange()}
            ).publish_pending_once()
        async with session_factory() as session:
            stored = await session.get(OutboxEvent, event.event_id)
            assert stored.published_at is None
            assert stored.last_publish_error == "ConnectionError: RabbitMQ did not positively confirm publication"
    finally:
        await _delete_event(session_factory, event.event_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_recovers_from_closed_channel_and_confirms_the_same_event(session_factory):
    event = await _create_pending_event(session_factory)
    stop = asyncio.Event()
    deliveries = []

    class RecoveringExchange:
        async def publish(self, message, routing_key, **kwargs):
            deliveries.append((message.message_id, message.body, routing_key))
            assert kwargs == {"mandatory": True, "timeout": 5.0}
            if len(deliveries) <= 2:
                raise ChannelInvalidStateError("channel is recovering")

            # Earlier failures committed their diagnostics, but never published_at.
            async with session_factory() as session:
                pending = await session.get(OutboxEvent, event.event_id)
                assert pending.published_at is None
                assert pending.publish_attempts == 2
                assert pending.last_publish_error.startswith("ChannelInvalidStateError:")
            assert not worker.health.healthy
            assert worker.health.consecutive_failures == 2
            stop.set()
            return Basic.Ack(delivery_tag=1)

    worker = OutboxPublisher(session_factory, {"crawler.topic": RecoveringExchange()})
    try:
        await asyncio.wait_for(worker.run(stop, 0.001), timeout=3)

        async with session_factory() as session:
            published = await session.get(OutboxEvent, event.event_id)
            assert published.published_at is not None
            assert published.publish_attempts == 3
            assert published.last_publish_error is None
        assert len(deliveries) == 3
        assert {message_id for message_id, _, _ in deliveries} == {str(event.event_id)}
        assert all(delivery == deliveries[0] for delivery in deliveries)
        assert worker.health.healthy
        assert worker.health.consecutive_failures == 0
    finally:
        await _delete_event(session_factory, event.event_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_stops_on_permanent_runtime_error_without_repeated_publication(session_factory):
    event = await _create_pending_event(session_factory)

    class BrokenExchange:
        async def publish(self, message, routing_key, **kwargs):
            raise RuntimeError("invalid publisher configuration")

    worker = OutboxPublisher(session_factory, {"crawler.topic": BrokenExchange()})
    try:
        with pytest.raises(RuntimeError, match="invalid publisher configuration"):
            await asyncio.wait_for(worker.run(asyncio.Event(), 0.001), timeout=3)
        async with session_factory() as session:
            pending = await session.get(OutboxEvent, event.event_id)
            assert pending.published_at is None
            assert pending.publish_attempts == 1
            assert pending.last_publish_error.startswith("RuntimeError:")
        assert not worker.health.healthy
    finally:
        await _delete_event(session_factory, event.event_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_outbox_insert_requires_an_explicit_exchange(session_factory):
    async with session_factory() as session:
        with pytest.raises(IntegrityError) as caught:
            async with session.begin():
                session.add(OutboxEvent(event_id=uuid4(), routing_key="fetch.url", payload={}))
        assert caught.value.orig.sqlstate == "23502"
        assert "exchange_name" in str(caught.value.orig)
