"""SDK cancellation regressions without network connections or a database."""

import asyncio
from contextlib import asynccontextmanager
from copy import copy
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from aio_pika import ExchangeType
from aio_pika.exchange import Exchange
from aio_pika.robust_connection import RobustConnection
from aiormq.channel import Channel
from aiormq.connection import Connection
from pamqp.commands import Basic
from pamqp.commands import Connection as ConnectionFrames

from crawler_frontier.outbox import OutboxPublisher
from crawler_frontier.subscriptions import ConsumerSubscriptions


class MemoryOutbox:
    """Separate transactional changes from committed state; no PostgreSQL I/O."""

    def __init__(self):
        event_id = uuid4()
        self.row = SimpleNamespace(
            event_id=event_id, exchange_name="crawler.topic", routing_key="fetch.url",
            payload={"event_id": str(event_id)}, publish_attempts=0,
            published_at=None, last_publish_error=None,
        )
        self.commits = []
        self.rollbacks = 0
        self.before_commit = asyncio.Event()
        self.allow_commit = asyncio.Event()
        self.allow_commit.set()
        self.published = asyncio.Event()

    @asynccontextmanager
    async def session(self):
        store = self

        class Session:
            selected = None

            async def scalar(self, statement):
                self.selected = copy(store.row) if store.row.published_at is None else None
                return self.selected

            @asynccontextmanager
            async def begin(self):
                try:
                    yield
                    if self.selected is not None:
                        if self.selected.published_at is not None:
                            store.before_commit.set()
                            await store.allow_commit.wait()
                        store.row = copy(self.selected)
                        store.commits.append(copy(self.selected))
                        if store.row.published_at is not None:
                            store.published.set()
                except BaseException:
                    store.rollbacks += 1
                    raise

        yield Session()


class MemoryBroker:
    """Real SDK reconnect/close/confirmation machinery; only transport I/O is fake.

    This also supplies the thin channel adapter used by aio-pika Exchange. Restore
    negotiation is simulated; subscription restoration itself has separate tests.
    """

    reconnect = RobustConnection.reconnect

    def __init__(self):
        self.tags = {f"required-{index}" for index in range(6)}
        self.connections = []
        self.sent = asyncio.Queue()
        self._replace_transport()

    def _replace_transport(self):
        # Construct SDK objects, but never connect them to a broker.
        connection = Connection("amqp://localhost/unused")
        connection.server_properties = {"capabilities": {"publisher_confirms": True}}
        connection.connection_tune = ConnectionFrames.Tune(frame_max=131072)
        channel = Channel(connection, number=1, publisher_confirms=True)
        channel.consumers.update(dict.fromkeys(self.tags))

        async def writer():
            while True:
                frame = await connection.write_queue.get()
                frame.drain_future.set_result(None)
                self.sent.put_nowait(channel.confirmations[channel.delivery_tag])

        connection._reader_task = asyncio.create_task(asyncio.Event().wait())
        connection._writer_task = asyncio.create_task(writer())
        self.connections.append(connection)
        self.transport = SimpleNamespace(connection=connection)
        self.underlay = channel

    async def connect(self):
        self._replace_transport()

    async def reconnect_callbacks(self):
        pass

    @property
    def is_closed(self):
        return self.underlay.is_closed

    async def ready(self):
        pass

    async def get_underlay_channel(self):
        return self.underlay

    async def close(self):
        for connection in self.connections:
            await connection.close()


class RecordingExchange(Exchange):
    def __init__(self, broker):
        super().__init__(
            broker, "crawler.topic", ExchangeType.TOPIC, durable=True,
            auto_delete=False, internal=False, passive=False, arguments={},
        )
        self.deliveries = []

    async def publish(self, message, routing_key, **kwargs):
        assert kwargs == {"mandatory": True, "timeout": 5.0}
        self.deliveries.append((message.message_id, message.body, routing_key))
        return await super().publish(message, routing_key=routing_key, **kwargs)


@pytest_asyncio.fixture
async def broker():
    broker = MemoryBroker()
    try:
        yield broker
    finally:
        await broker.close()


@pytest.mark.asyncio
async def test_subscription_reconnect_preserves_publisher_and_republishes_pending_event(broker):
    store = MemoryOutbox()
    store.allow_commit.clear()
    exchange = RecordingExchange(broker)
    worker = OutboxPublisher(store.session, {"crawler.topic": exchange})
    stop = asyncio.Event()
    publisher = asyncio.create_task(worker.run(stop, 0.001))
    tasks = [publisher]
    try:
        first_confirmation = await asyncio.wait_for(broker.sent.get(), 2)
        assert not first_confirmation.done()
        assert store.row.published_at is None and not store.commits

        await broker.underlay._on_cancel_frame(Basic.Cancel(consumer_tag=next(iter(broker.tags))))
        monitor = ConsumerSubscriptions(broker, broker, broker.tags)
        assert not monitor.is_registered(await monitor.get_ready_channel())
        tasks.append(asyncio.create_task(monitor.run(stop, 0.001)))
        next_publication = asyncio.create_task(broker.sent.get())
        tasks.append(next_publication)
        await asyncio.wait({publisher, next_publication}, timeout=2, return_when=asyncio.FIRST_COMPLETED)

        assert first_confirmation.done()
        assert isinstance(first_confirmation.exception(), asyncio.CancelledError)
        assert publisher.cancelling() == 0 and not stop.is_set()
        assert not publisher.done(), "Reconnect killed the publisher without a shutdown request"
        assert next_publication.done(), "The pending outbox event was not retried"
        assert not next_publication.result().done()
        assert store.row.published_at is None
        assert store.rollbacks == 1
        assert all(row.published_at is None for row in store.commits)
        assert not worker.health.healthy and worker.health.consecutive_failures == 1
        assert monitor.is_registered(await monitor.get_ready_channel()) and monitor.health.healthy
        assert len(exchange.deliveries) == 2
        assert exchange.deliveries[0] == exchange.deliveries[1]

        await broker.underlay._on_confirm_frame(Basic.Ack(delivery_tag=1))
        await asyncio.wait_for(store.before_commit.wait(), 2)
        assert store.row.published_at is None  # Confirmation alone is not a committed publication.
        store.allow_commit.set()
        await asyncio.wait_for(store.published.wait(), 2)
        assert store.row.published_at is not None
        assert store.row.last_publish_error is None
        assert worker.health.healthy and worker.health.consecutive_failures == 0
        stop.set()
        await asyncio.wait_for(publisher, 2)
        assert not publisher.cancelled()
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_stop", [False, True])
@pytest.mark.parametrize("phase", ["confirmation", "commit"])
async def test_explicit_publisher_cancellation_does_not_retry_or_commit(broker, signal_stop, phase):
    store = MemoryOutbox()
    store.allow_commit.clear()
    exchange = RecordingExchange(broker)
    worker = OutboxPublisher(store.session, {"crawler.topic": exchange})
    stop = asyncio.Event()
    publisher = asyncio.create_task(worker.run(stop, 0.001))
    try:
        await asyncio.wait_for(broker.sent.get(), 2)
        if phase == "commit":
            await broker.underlay._on_confirm_frame(Basic.Ack(delivery_tag=1))
            await asyncio.wait_for(store.before_commit.wait(), 2)
        assert store.row.published_at is None
        if signal_stop:
            stop.set()
        publisher.cancel()
        done, _ = await asyncio.wait({publisher}, timeout=1)
        assert publisher in done and publisher.cancelled()
        assert publisher.cancelling() == 1
        assert len(exchange.deliveries) == 1
        assert store.row.published_at is None and not store.commits
        assert store.rollbacks == 1
    finally:
        stop.set()
        publisher.cancel()
        await asyncio.gather(publisher, return_exceptions=True)


@pytest.mark.asyncio
async def test_permanent_publish_error_still_terminates_worker_without_retry():
    store = MemoryOutbox()
    deliveries = []

    class BrokenExchange:
        async def publish(self, message, **kwargs):
            deliveries.append(message.message_id)
            raise RuntimeError("invalid publisher configuration")

    worker = OutboxPublisher(store.session, {"crawler.topic": BrokenExchange()})
    with pytest.raises(RuntimeError, match="invalid publisher configuration"):
        await asyncio.wait_for(worker.run(asyncio.Event(), 0.001), 2)
    assert deliveries == [str(store.row.event_id)]
    assert store.row.published_at is None
    assert not worker.health.healthy


@pytest.mark.asyncio
async def test_publish_pending_once_commits_broker_diagnostic_before_reraising():
    store = MemoryOutbox()

    class BrokenExchange:
        async def publish(self, message, **kwargs):
            raise RuntimeError("invalid publisher configuration")

    publisher = OutboxPublisher(store.session, {"crawler.topic": BrokenExchange()})

    with pytest.raises(RuntimeError, match="invalid publisher configuration"):
        await publisher.publish_pending_once()

    assert len(store.commits) == 1
    assert store.row.published_at is None
    assert store.row.publish_attempts == 1
    assert store.row.last_publish_error == "RuntimeError: invalid publisher configuration"
