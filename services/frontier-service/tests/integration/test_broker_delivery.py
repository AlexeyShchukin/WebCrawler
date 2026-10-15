"""Delivery guarantees exercised against the local RabbitMQ, with private queues."""

import os
from urllib.parse import quote
from uuid import uuid4

import aio_pika
import pytest
import pytest_asyncio
from crawler_contracts import FetchStartedEvent
from dotenv import load_dotenv

from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.messaging.consumers import consumer_callback
from crawler_frontier.messaging.outbox import OutboxPublisher
from crawler_frontier.migration_settings import LOCAL_ENV_FILE
from crawler_frontier.models import OutboxEvent
from tests.integration.helpers import _create_pending_event
from tests.support.event_payloads import malformed_event_body


@pytest_asyncio.fixture
async def broker_channel():
    load_dotenv(LOCAL_ENV_FILE, override=False)
    broker_url = os.environ.get("FRONTIER_TEST_RABBITMQ_URL")
    if broker_url is None:
        user = quote(os.environ["RABBITMQ_USER"], safe="")
        password = quote(os.environ["RABBITMQ_PASSWORD"], safe="")
        broker_url = f"amqp://{user}:{password}@127.0.0.1/"
    connection = await aio_pika.connect_robust(broker_url)
    try:
        yield await connection.channel(publisher_confirms=True, on_return_raises=True)
    finally:
        await connection.close()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("routed", [True, False])
@pytest.mark.parametrize("kind", [
    "invalid_json", "missing_event_id", "missing_created_at", "missing_both",
    "duplicate_event_id", "duplicate_created_at",
])
async def test_invalid_delivery_is_retained_until_dlq_confirm(broker_channel, routed, kind):
    channel = broker_channel
    source = await channel.declare_queue(exclusive=True, auto_delete=True)
    dlq = await channel.declare_queue(exclusive=True, auto_delete=True)
    exchange = await channel.declare_exchange("test.dlx." + uuid4().hex, auto_delete=True)
    body = malformed_event_body(kind)
    sessions = []

    def unexpected_session():
        sessions.append(True)
        raise AssertionError("Invalid delivery must be rejected before DB access")

    try:
        if routed:
            await dlq.bind(exchange, "page.fetched.frontier")
        await channel.default_exchange.publish(aio_pika.Message(body), source.name)
        incoming = await source.get(timeout=3)
        callback = consumer_callback(
            FetchStartedEvent, "page.fetched", "handle_fetch_started", unexpected_session, FrontierPolicy(),
            dead_letter_exchange=exchange, dead_letter_routing_key="page.fetched.frontier",
        )
        await callback(incoming)
        assert sessions == []
        if routed:
            retained = await dlq.get(timeout=3)
            assert retained.body == body
            assert retained.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
            await retained.ack()
            assert await source.get(fail=False) is None
        else:
            redelivery = await source.get(timeout=3)
            assert redelivery.body == body
            assert redelivery.redelivered
            await redelivery.ack()
            assert await dlq.get(fail=False) is None
    finally:
        await exchange.delete()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_malformed_redelivery_reaches_dlq_once_its_route_recovers(broker_channel):
    source = await broker_channel.declare_queue(exclusive=True, auto_delete=True)
    dlq = await broker_channel.declare_queue(exclusive=True, auto_delete=True)
    exchange = await broker_channel.declare_exchange("test.dlx." + uuid4().hex, auto_delete=True)
    body = malformed_event_body("missing_both")
    sessions = []

    def unexpected_session():
        sessions.append(True)
        raise AssertionError("Invalid delivery must not open a DB session")

    callback = consumer_callback(
        FetchStartedEvent, "fetch.started", "handle_fetch_started", unexpected_session,
        FrontierPolicy(), dead_letter_exchange=exchange,
    )
    try:
        await broker_channel.default_exchange.publish(aio_pika.Message(body), source.name)
        original = await source.get(timeout=3)
        await callback(original)
        assert await dlq.get(fail=False) is None

        redelivery = await source.get(timeout=3)
        assert redelivery.redelivered and redelivery.body == body
        await dlq.bind(exchange, "fetch.started")
        await callback(redelivery)

        retained = await dlq.get(timeout=3)
        assert retained.body == body
        assert retained.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        await retained.ack()
        assert await source.get(fail=False) is None
        assert await dlq.get(fail=False) is None
        assert sessions == []
    finally:
        await exchange.delete()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("routed", [True, False])
async def test_outbox_requires_a_real_confirm_and_route(broker_channel, session_factory, routed):
    exchange = await broker_channel.declare_exchange("test.outbox." + uuid4().hex, auto_delete=True)
    queue = await broker_channel.declare_queue(exclusive=True, auto_delete=True)
    if routed:
        await queue.bind(exchange, "fetch.url")
    event = await _create_pending_event(session_factory)
    try:
        publisher = OutboxPublisher(session_factory, {"crawler.topic": exchange})
        if routed:
            assert await publisher.publish_pending_once()
        else:
            with pytest.raises(aio_pika.exceptions.DeliveryError):
                await publisher.publish_pending_once()
        async with session_factory() as session:
            stored = await session.get(OutboxEvent, event.event_id)
            assert (stored.published_at is not None) is routed
            assert bool(stored.last_publish_error) is not routed
        if routed:
            received = await queue.get(timeout=3)
            assert received.message_id == str(event.event_id)
            await received.ack()
    finally:
        await exchange.delete()
