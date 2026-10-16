"""Subscription lifecycle tests with real client callbacks and no runtime broker."""

import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio
from aio_pika.robust_connection import RobustConnection
from aio_pika.robust_queue import RobustQueue
from aiormq.channel import Channel
from fastapi import HTTPException
from pamqp.commands import Basic, Queue

from crawler_frontier import app as application
from crawler_frontier.messaging.consumers import CONSUMERS
from crawler_frontier.workers.runner import WorkerHealth


async def until(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout=3)


@pytest_asyncio.fixture
async def subscription_app(monkeypatch):
    class Underlay:
        number = 1
        is_closed = False
        basic_consume = Channel.basic_consume
        _on_cancel_frame = Channel._on_cancel_frame

        def __init__(self):
            self.consumers = {}

        async def rpc(self, frame, **kwargs):
            if isinstance(frame, Basic.Consume):
                if frame.consumer_tag == connection.failed_consume_tag:
                    raise ConnectionError("consume confirmation unavailable")
                return Basic.ConsumeOk(consumer_tag=frame.consumer_tag)
            assert isinstance(frame, Basic.Cancel)
            await self._on_cancel_frame(frame)
            return Basic.CancelOk(consumer_tag=frame.consumer_tag)

        async def basic_cancel(self, consumer_tag, **kwargs):
            return await self.rpc(Basic.Cancel(consumer_tag=consumer_tag))

        async def queue_declare(self, queue, **kwargs):
            assert kwargs["passive"]
            return Queue.DeclareOk(queue=queue)

    class BrokerChannel:
        def __init__(self):
            self.underlay = Underlay()
            self.restored = asyncio.Event()
            self.restored.set()
            self.queues = {}

        @property
        def is_closed(self):
            return self.underlay.is_closed

        async def ready(self):
            await self.restored.wait()

        async def get_underlay_channel(self):
            return self.underlay

        async def set_qos(self, **kwargs):
            pass

        async def declare_exchange(self, *args, **kwargs):
            return object()

        async def get_queue(self, name, *, ensure):
            assert ensure
            queue = RobustQueue(self, name, passive=True)
            await queue.declare()
            self.queues[name] = queue
            return queue

    class Transport:
        async def close(self):
            connection.reconnect_calls += 1
            if connection.close_failures:
                connection.close_failures -= 1
                raise ConnectionError("transport close unavailable")
            await connection.restore_allowed.wait()
            connection.connected.clear()
            channel.restored.clear()
            channel.underlay.is_closed = True
            channel.underlay.consumers.clear()

    class Connection:
        is_closed = False
        reconnect = RobustConnection.reconnect

        def __init__(self):
            self.connected = asyncio.Event()
            self.connected.set()
            self.restore_allowed = asyncio.Event()
            self.restore_allowed.set()
            self.reconnect_calls = 0
            self.close_failures = 0
            self.failed_consume_tag = None
            self.transport = SimpleNamespace(connection=Transport())

        async def channel(self, **kwargs):
            return channel

        async def connect(self):
            channel.underlay = Underlay()
            self.connected.set()
            for queue in channel.queues.values():
                await queue.restore()
            channel.restored.set()

        async def reconnect_callbacks(self):
            pass

        async def close(self):
            self.is_closed = True

    class Worker:
        def __init__(self, *args):
            self.health = WorkerHealth(healthy=True)

        async def run(self, stop, interval):
            await stop.wait()

    channel = BrokerChannel()
    connection = Connection()
    engine = SimpleNamespace(disposed=False)

    async def dispose():
        engine.disposed = True

    engine.dispose = dispose

    async def connect(url):
        return connection

    async def database_ok(engine):
        pass

    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://localhost/unused")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://localhost/")
    monkeypatch.setattr(application, "create_engine", lambda settings: engine)
    monkeypatch.setattr(application, "create_session_factory", lambda engine: object())
    monkeypatch.setattr(application, "connect_robust", connect)
    monkeypatch.setattr(application, "database_healthcheck", database_ok)
    monkeypatch.setattr(application, "OutboxPublisher", Worker)
    monkeypatch.setattr(application, "LeaseRecovery", Worker)
    # Shorten polling only; preserve real client consume/cancel/restore behavior.
    monkeypatch.setattr(application, "_CONSUMER_POLL_INTERVAL_SECONDS", 0.001, raising=False)
    app = application.create_app()
    health = next(route.endpoint for route in app.routes if route.path == "/health")
    async with application.lifespan(app):
        try:
            yield app, connection, channel, health
        finally:
            connection.restore_allowed.set()
    assert engine.disposed


@pytest.mark.asyncio
async def test_all_six_consumers_are_registered_and_ready(subscription_app):
    app, connection, channel, health = subscription_app
    assert len(channel.queues) == len(CONSUMERS) == 6
    assert len(channel.underlay.consumers) == 6
    assert await health() == {"status": "ok"}
    assert connection.reconnect_calls == 0
    assert all(not task.done() for task in app.state.worker_tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("queue_name", list(CONSUMERS))
async def test_basic_cancel_is_unready_until_original_subscriptions_are_restored(subscription_app, queue_name):
    app, connection, channel, health = subscription_app
    original_tags = set(channel.underlay.consumers)
    tag = next(iter(channel.queues[queue_name]._consumers))
    connection.restore_allowed.clear()
    await channel.underlay._on_cancel_frame(Basic.Cancel(consumer_tag=tag))
    assert connection.connected.is_set() and not channel.is_closed
    with pytest.raises(HTTPException) as caught:
        await health()
    assert caught.value.status_code == 503
    await until(lambda: connection.reconnect_calls == 1)
    connection.restore_allowed.set()
    await until(lambda: channel.restored.is_set() and set(channel.underlay.consumers) == original_tags)
    await until(lambda: all(worker.health.healthy for worker in app.state.workers))
    assert await health() == {"status": "ok"}
    assert connection.reconnect_calls == 1
    for _ in range(3):
        assert await health() == {"status": "ok"}
    assert set(channel.underlay.consumers) == original_tags


@pytest.mark.asyncio
async def test_recovery_errors_stay_unready_and_retry_without_duplicate_consumers(subscription_app):
    app, connection, channel, health = subscription_app
    original_tags = set(channel.underlay.consumers)
    connection.close_failures = 2
    connection.restore_allowed.clear()
    await channel.underlay._on_cancel_frame(Basic.Cancel(consumer_tag=next(iter(original_tags))))
    with pytest.raises(HTTPException) as caught:
        await health()
    assert caught.value.status_code == 503
    await until(lambda: connection.reconnect_calls == 3)
    assert len(channel.underlay.consumers) == 5
    assert all(not task.done() for task in app.state.worker_tasks)
    with pytest.raises(HTTPException) as caught:
        await health()
    assert caught.value.status_code == 503
    connection.restore_allowed.set()
    await until(lambda: channel.restored.is_set() and set(channel.underlay.consumers) == original_tags)
    await until(lambda: all(worker.health.healthy for worker in app.state.workers))
    assert await health() == {"status": "ok"}
    assert connection.reconnect_calls == 3


@pytest.mark.asyncio
async def test_unconfirmed_consume_registration_is_not_reported_ready(subscription_app):
    app, connection, channel, health = subscription_app
    original_tags = set(channel.underlay.consumers)
    last_queue = channel.queues[next(reversed(CONSUMERS))]
    tag = next(iter(last_queue._consumers))
    connection.failed_consume_tag = tag
    await channel.underlay._on_cancel_frame(Basic.Cancel(consumer_tag=tag))
    with pytest.raises(HTTPException) as caught:
        await health()
    assert caught.value.status_code == 503
    await until(lambda: connection.reconnect_calls == 1 and not channel.restored.is_set())
    await until(lambda: set(channel.underlay.consumers) == original_tags)
    with pytest.raises(HTTPException) as caught:
        await health()
    assert caught.value.status_code == 503
    assert connection.reconnect_calls == 1
    connection.failed_consume_tag = None
    # Emulate the robust connection factory completing its next transport restore.
    await connection.reconnect()
    await until(lambda: all(worker.health.healthy for worker in app.state.workers))
    assert await health() == {"status": "ok"}
    assert set(channel.underlay.consumers) == original_tags


@pytest.mark.asyncio
async def test_cancel_during_database_probe_is_not_reported_ready(subscription_app, monkeypatch):
    _, connection, channel, health = subscription_app
    connection.restore_allowed.clear()
    tag = next(iter(channel.underlay.consumers))

    async def cancel_during_probe(engine):
        await channel.underlay._on_cancel_frame(Basic.Cancel(consumer_tag=tag))

    monkeypatch.setattr(application, "database_healthcheck", cancel_during_probe)
    with pytest.raises(HTTPException) as caught:
        await health()
    assert caught.value.status_code == 503
