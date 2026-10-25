"""Worker recovery and lifecycle regressions."""

import asyncio

import pytest
from sqlalchemy.exc import OperationalError

from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.messaging.outbox import OutboxPublisher
from crawler_frontier.workers.lease_recovery import LeaseRecovery


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["publisher", "recovery"])
async def test_background_worker_survives_repeated_database_errors(kind, monkeypatch):
    stop = asyncio.Event()
    worker = OutboxPublisher(None, {}) if kind == "publisher" else LeaseRecovery(None, FrontierPolicy())
    method = "publish_pending_once" if kind == "publisher" else "recover_expired_once"
    calls = 0

    async def operation():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OperationalError("SELECT", {}, ConnectionError("database offline"))
        stop.set()
        return 0

    monkeypatch.setattr(worker, method, operation)
    await asyncio.wait_for(worker.run(stop, 0.001), 3)
    assert calls == 3
    assert worker.health.healthy


@pytest.mark.asyncio
async def test_startup_failure_still_releases_database_engine(monkeypatch):
    from crawler_frontier import app as application

    disposed = []

    class Engine:
        async def dispose(self):
            disposed.append(True)

    async def failed_connect(url):
        raise ConnectionError("broker offline")

    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://localhost/frontier")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://localhost/")
    monkeypatch.setattr(application, "create_engine", lambda settings: Engine())
    monkeypatch.setattr(application, "create_session_factory", lambda engine: None)
    monkeypatch.setattr(application, "connect_robust", failed_connect)
    with pytest.raises(ConnectionError):
        async with application.lifespan(application.create_app()):
            pass
    assert disposed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["worker_iteration", "worker_stopped", "broker_disconnected", "channel_closed", "startup_incomplete", "subscriptions_restoring"])
async def test_readiness_does_not_report_failed_dependencies_healthy(monkeypatch, failure):
    from types import SimpleNamespace

    from fastapi import HTTPException

    from crawler_frontier import app as application
    from crawler_frontier.messaging.subscriptions import ConsumerSubscriptions
    from crawler_frontier.workers.runner import WorkerHealth

    app = application.create_app()
    connected = asyncio.Event()
    connected.set()
    stop = asyncio.Event()
    task = asyncio.create_task(stop.wait())
    worker = SimpleNamespace(health=WorkerHealth(healthy=True))
    subscriptions_ready = asyncio.Event()
    subscriptions_ready.set()

    async def channel_ready():
        await subscriptions_ready.wait()
    app.state.ready = True
    app.state.database_engine = object()
    app.state.broker_connection = SimpleNamespace(is_closed=False, connected=connected)
    app.state.broker_channel = SimpleNamespace(is_closed=False, ready=channel_ready)
    tags = {f"consumer-{index}" for index in range(3)}
    underlay = SimpleNamespace(is_closed=False, consumers=dict.fromkeys(tags))

    async def get_underlay_channel():
        return underlay

    app.state.broker_channel.get_underlay_channel = get_underlay_channel
    monitor = ConsumerSubscriptions(app.state.broker_connection, app.state.broker_channel, tags)
    monitor.health.healthy = True
    app.state.consumer_subscriptions = monitor
    app.state.workers, app.state.worker_tasks = [worker, monitor], [task]

    async def database_ok(engine):
        pass

    monkeypatch.setattr(application, "database_healthcheck", database_ok)
    endpoint = next(
        route.endpoint for route in app.routes if getattr(route, "path", None) == "/health"
    )
    try:
        assert await endpoint() == {"status": "ok"}
        if failure == "worker_iteration":
            worker.health.healthy = False
        elif failure == "worker_stopped":
            stop.set()
            await task
        elif failure == "broker_disconnected":
            connected.clear()
        elif failure == "channel_closed":
            app.state.broker_channel.is_closed = True
        elif failure == "subscriptions_restoring":
            subscriptions_ready.clear()
        else:
            app.state.ready = False
        with pytest.raises(HTTPException) as caught:
            await endpoint()
        assert caught.value.status_code == 503
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
