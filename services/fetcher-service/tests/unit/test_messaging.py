"""RabbitMQ adapter confirms durable metadata-only publications."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aio_pika import DeliveryMode, ExchangeType
from crawler_contracts import FetchUrlEvent, PageFetchedEvent
from pamqp.commands import Basic

from crawler_fetcher.messaging import FetcherPublisher
from crawler_fetcher.settings import FetcherSettings


@pytest.fixture
def settings() -> FetcherSettings:
    return FetcherSettings(
        rabbitmq_url="amqp://worker:password@broker:5672/",
        redis_url="redis://redis:6379/0",
        object_storage_endpoint="http://objects:9000",
        object_storage_access_key="test-access",
        object_storage_secret_key="test-secret",
        object_storage_bucket="html-bodies",
        fetch_concurrency=7,
    )


@pytest.fixture
def event() -> FetchUrlEvent:
    return FetchUrlEvent(
        crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=2,
        url="https://example.com/", depth=0,
    )


@pytest.fixture
def broker(monkeypatch):
    topic = SimpleNamespace(publish=AsyncMock(return_value=Basic.Ack()))
    retry = SimpleNamespace(publish=AsyncMock(return_value=Basic.Ack()))
    queue = object()
    channel = SimpleNamespace(
        set_qos=AsyncMock(), declare_exchange=AsyncMock(side_effect=[topic, retry]),
        get_queue=AsyncMock(return_value=queue), close=AsyncMock(),
    )
    connection = SimpleNamespace(channel=AsyncMock(return_value=channel), close=AsyncMock())
    connect = AsyncMock(return_value=connection)
    monkeypatch.setattr("crawler_fetcher.messaging.connect_robust", connect)
    return SimpleNamespace(
        topic=topic, retry=retry, queue=queue, channel=channel,
        connection=connection, connect=connect,
    )


@pytest.mark.asyncio
async def test_worker_connection_uses_confirms_qos_and_existing_queue(settings, broker) -> None:
    async with FetcherPublisher(settings) as publisher:
        assert publisher.queue is broker.queue
        assert publisher.channel is broker.channel
        assert publisher.connection is broker.connection
        broker.connect.assert_awaited_once_with(str(settings.rabbitmq_url))
        broker.connection.channel.assert_awaited_once_with(
            publisher_confirms=True, on_return_raises=True,
        )
        broker.channel.set_qos.assert_awaited_once_with(prefetch_count=7)
        assert [call.args for call in broker.channel.declare_exchange.await_args_list] == [
            ("crawler.topic", ExchangeType.TOPIC), ("crawler.retry", ExchangeType.TOPIC),
        ]
        assert all(call.kwargs == {"durable": True} for call in broker.channel.declare_exchange.await_args_list)
        broker.channel.get_queue.assert_awaited_once_with("fetch.url.queue", ensure=True)
    broker.channel.close.assert_awaited_once()
    broker.connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_publisher_waits_for_broker_confirmation(settings, broker, event) -> None:
    started, confirmed = asyncio.Event(), asyncio.Event()

    async def confirm(*args, **kwargs):
        started.set()
        await confirmed.wait()
        return Basic.Ack()

    broker.topic.publish.side_effect = confirm
    async with FetcherPublisher(settings) as publisher:
        pending = asyncio.create_task(publisher.publish(
            event, exchange_name="crawler.topic", routing_key="fetch.url",
        ))
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not pending.done()
        confirmed.set()
        await pending
    assert broker.topic.publish.await_count == 1


@pytest.mark.asyncio
async def test_publisher_serializes_contract_metadata_as_persistent_json(settings, broker, event) -> None:
    fetched = PageFetchedEvent(
        crawl_id=event.crawl_id, url_id=event.url_id, fetch_attempt=2,
        final_url="https://example.com/", http_status=200, content_type="text/html",
        content_ref="crawls/opaque/fetches/2.html",
    )
    async with FetcherPublisher(settings) as publisher:
        await publisher.publish(fetched, exchange_name="crawler.topic", routing_key="page.fetched")
        await publisher.publish(event, exchange_name="crawler.retry", routing_key="fetch.url.retry.5s")

    for exchange, contract, route in (
        (broker.topic, fetched, "page.fetched"), (broker.retry, event, "fetch.url.retry.5s"),
    ):
        message = exchange.publish.await_args.args[0]
        assert message.body == contract.model_dump_json().encode("utf-8")
        assert message.delivery_mode == DeliveryMode.PERSISTENT
        assert message.message_id == str(contract.event_id)
        assert message.content_type == "application/json"
        assert exchange.publish.await_args.kwargs["routing_key"] == route
        assert exchange.publish.await_args.kwargs["mandatory"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", [None, False, Basic.Nack(), Basic.Reject()])
async def test_missing_positive_confirmation_is_an_error(settings, broker, event, confirmation) -> None:
    broker.topic.publish.return_value = confirmation
    async with FetcherPublisher(settings) as publisher:
        with pytest.raises(ConnectionError, match="confirm"):
            await publisher.publish(event, exchange_name="crawler.topic", routing_key="fetch.url")


@pytest.mark.asyncio
async def test_startup_failure_closes_channel_and_connection(settings, broker) -> None:
    broker.channel.get_queue.side_effect = ConnectionError("queue unavailable")
    with pytest.raises(ConnectionError, match="queue unavailable"):
        async with FetcherPublisher(settings):
            pytest.fail("Failed startup must not enter the worker lifetime")
    broker.channel.close.assert_awaited_once()
    broker.connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_connection_closes_even_when_channel_close_fails(settings, broker) -> None:
    broker.channel.close.side_effect = OSError("channel close failed")
    with pytest.raises(OSError, match="channel close failed"):
        async with FetcherPublisher(settings):
            pass
    broker.connection.close.assert_awaited_once()
