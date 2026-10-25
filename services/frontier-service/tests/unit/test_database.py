import asyncio

import pytest
from aio_pika.connection import Connection
from pydantic import ValidationError
from sqlalchemy import make_url

from crawler_frontier.settings import FrontierSettings


def test_settings_require_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "DATABASE_URL",
        "RABBITMQ_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "RABBITMQ_USER",
        "RABBITMQ_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValidationError):
        FrontierSettings()


def test_settings_read_database_pool_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://crawler:secret@rabbitmq/")
    monkeypatch.setenv("DATABASE_POOL_SIZE", "8")
    monkeypatch.setenv("DATABASE_MAX_OVERFLOW", "12")
    monkeypatch.setenv("DATABASE_POOL_TIMEOUT_SECONDS", "7")
    monkeypatch.setenv("MAX_FETCH_ATTEMPTS", "4")
    monkeypatch.setenv("FETCH_LEASE_SECONDS", "180")
    monkeypatch.setenv("MAX_URL_LENGTH", "2048")
    monkeypatch.setenv("MAX_CRAWL_DEPTH", "4")
    monkeypatch.setenv("MAX_URLS_PER_CRAWL", "500")
    monkeypatch.setenv("MAX_URLS_PER_ORIGIN", "50")
    monkeypatch.setenv("LEASE_RECOVERY_POLL_INTERVAL_SECONDS", "2.5")

    settings = FrontierSettings()

    assert settings.database_url == "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db"
    assert settings.database_pool_size == 8
    assert settings.database_max_overflow == 12
    assert settings.database_pool_timeout_seconds == 7
    assert settings.max_fetch_attempts == 4
    assert settings.fetch_lease_seconds == 180
    assert settings.max_url_length == 2048
    assert settings.max_crawl_depth == 4
    assert settings.max_urls_per_crawl == 500
    assert settings.max_urls_per_origin == 50
    assert settings.lease_recovery_poll_interval_seconds == 2.5


def test_settings_build_connection_urls_from_raw_compose_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "crawler")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@/#%:")
    monkeypatch.setenv("FRONTIER_DATABASE_HOST", "postgres")
    monkeypatch.setenv("FRONTIER_DATABASE_PORT", "5432")
    monkeypatch.setenv("FRONTIER_DATABASE_NAME", "frontier_db")
    monkeypatch.setenv("RABBITMQ_USER", "crawler")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "p@/#%:")
    monkeypatch.setenv("RABBITMQ_HOST", "rabbitmq")
    monkeypatch.setenv("RABBITMQ_PORT", "5672")

    settings = FrontierSettings()

    database_url = make_url(settings.database_url)
    rabbitmq_url = Connection(settings.rabbitmq_url).url

    assert (database_url.username, database_url.password, database_url.host) == (
        "crawler",
        "p@/#%:",
        "postgres",
    )
    assert (rabbitmq_url.user, rabbitmq_url.password, rabbitmq_url.host) == (
        "crawler",
        "p@/#%:",
        "rabbitmq",
    )


@pytest.fixture
def infrastructure_environment(monkeypatch):
    for name in (
        "DATABASE_POOL_SIZE", "DATABASE_MAX_OVERFLOW", "DATABASE_POOL_TIMEOUT_SECONDS",
        "OUTBOX_POLL_INTERVAL_SECONDS", "LEASE_RECOVERY_POLL_INTERVAL_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    return {
        "database_url": "postgresql+asyncpg://localhost/unused",
        "rabbitmq_url": "amqp://localhost/",
    }


def test_infrastructure_defaults_remain_valid(infrastructure_environment):
    settings = FrontierSettings(**infrastructure_environment)
    assert settings.database_pool_size == 5
    assert settings.database_max_overflow == 10
    assert settings.database_pool_timeout_seconds == 30
    assert settings.outbox_poll_interval_seconds == 1.0
    assert settings.lease_recovery_poll_interval_seconds == 5.0


@pytest.mark.parametrize("source", ["constructor", "environment"])
@pytest.mark.parametrize("field,value", [
    ("database_pool_size", 0),
    ("database_pool_size", -1),
    ("database_max_overflow", -1),
    ("outbox_poll_interval_seconds", 0),
    ("outbox_poll_interval_seconds", -0.5),
    ("outbox_poll_interval_seconds", float("nan")),
    ("outbox_poll_interval_seconds", float("inf")),
    ("outbox_poll_interval_seconds", float("-inf")),
    ("lease_recovery_poll_interval_seconds", 0),
    ("lease_recovery_poll_interval_seconds", -0.5),
    ("lease_recovery_poll_interval_seconds", float("nan")),
    ("lease_recovery_poll_interval_seconds", float("inf")),
    ("lease_recovery_poll_interval_seconds", float("-inf")),
])
def test_invalid_infrastructure_settings_fail_at_construction(
    infrastructure_environment, monkeypatch, source, field, value,
):
    overrides = {}
    if source == "environment":
        monkeypatch.setenv(field.upper(), str(value))
    else:
        overrides[field] = value
    with pytest.raises(ValidationError) as caught:
        FrontierSettings(**infrastructure_environment, **overrides)
    assert {error["loc"] for error in caught.value.errors()} == {(field,)}
    assert field in str(caught.value)


@pytest.mark.parametrize("pool_size,max_overflow,outbox_poll,lease_poll", [
    (1, 0, 0.001, 0.002),
    (8, 12, 0.5, 2.5),
    (100, 100, 60.0, 600.0),
])
def test_valid_infrastructure_values_include_zero_overflow(
    infrastructure_environment, pool_size, max_overflow, outbox_poll, lease_poll,
):
    settings = FrontierSettings(
        **infrastructure_environment,
        database_pool_size=pool_size, database_max_overflow=max_overflow,
        outbox_poll_interval_seconds=outbox_poll,
        lease_recovery_poll_interval_seconds=lease_poll,
    )
    assert settings.database_pool_size == pool_size
    assert settings.database_max_overflow == max_overflow
    assert settings.outbox_poll_interval_seconds == outbox_poll
    assert settings.lease_recovery_poll_interval_seconds == lease_poll


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_fails, ignores_cancellation", [
    pytest.param(False, False, id="normal"),
    pytest.param(True, False, id="failed_worker"),
    pytest.param(False, True, id="worker_ignores_cancellation"),
])
async def test_application_disposes_its_database_engine_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    worker_fails: bool,
    ignores_cancellation: bool,
) -> None:
    from crawler_frontier import app as application

    release_worker = asyncio.Event()
    cancellation_seen = asyncio.Event()

    async def short_wait(tasks, *, timeout):
        # Use real asyncio waiting, shortening only the lifespan's grace periods.
        return await asyncio.wait(tasks, timeout=min(timeout, 0.02))

    monkeypatch.setattr(application, "wait", short_wait)

    class FakeEngine:
        disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    engine = FakeEngine()
    class FakeConnection:
        is_closed = False
        from asyncio import Event
        connected = Event()
        connected.set()

        def __init__(self):
            self.consumers = {}

        async def channel(self, *, publisher_confirms: bool, on_return_raises: bool):
            assert publisher_confirms and on_return_raises
            return self

        async def set_qos(self, *, prefetch_count: int) -> None:
            return None

        async def ready(self) -> None:
            pass

        async def get_underlay_channel(self):
            return self

        async def declare_exchange(self, *args, **kwargs):
            return object()

        async def get_queue(self, *args, **kwargs):
            return self

        async def consume(self, callback) -> str:
            tag = f"consumer-{len(self.consumers) + 1}"
            self.consumers[tag] = callback
            return tag

        async def close(self) -> None:
            self.is_closed = True

        async def cancel(self, tag) -> None:
            self.consumers.pop(tag, None)

    class FakePublisher:
        def __init__(self, *args) -> None:
            from crawler_frontier.workers.runner import WorkerHealth
            self.health = WorkerHealth(healthy=True)

        async def run(self, stop_event, poll_interval_seconds: float) -> None:
            if worker_fails:
                raise RuntimeError("unexpected worker failure")
            if ignores_cancellation:
                while not release_worker.is_set():
                    try:
                        await release_worker.wait()
                    except asyncio.CancelledError:
                        cancellation_seen.set()
                return
            await stop_event.wait()

    recovery_policies = []

    class FakeLeaseRecovery:
        def __init__(self, session_factory, policy) -> None:
            recovery_policies.append(policy)
            from crawler_frontier.workers.runner import WorkerHealth
            self.health = WorkerHealth(healthy=True)

        async def run(self, stop_event, poll_interval_seconds: float) -> None:
            assert poll_interval_seconds == 2.5
            await stop_event.wait()

    connection = FakeConnection()

    async def connect(url: str) -> FakeConnection:
        return connection

    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://crawler:secret@rabbitmq/")
    monkeypatch.setenv("MAX_FETCH_ATTEMPTS", "4")
    monkeypatch.setenv("FETCH_LEASE_SECONDS", "180")
    monkeypatch.setenv("MAX_URL_LENGTH", "2048")
    monkeypatch.setenv("MAX_CRAWL_DEPTH", "4")
    monkeypatch.setenv("MAX_URLS_PER_CRAWL", "500")
    monkeypatch.setenv("MAX_URLS_PER_ORIGIN", "50")
    monkeypatch.setenv("LEASE_RECOVERY_POLL_INTERVAL_SECONDS", "2.5")
    monkeypatch.setattr(application, "create_engine", lambda settings: engine)
    monkeypatch.setattr(application, "create_session_factory", lambda database_engine: object())
    monkeypatch.setattr(application, "connect_robust", connect)
    monkeypatch.setattr(application, "OutboxPublisher", FakePublisher)
    monkeypatch.setattr(application, "LeaseRecovery", FakeLeaseRecovery)
    consumer_policies = []

    def callback(event_type, consumer_name, handler_name, session_factory, policy, **kwargs):
        consumer_policies.append(policy)
        return object()

    monkeypatch.setattr(application, "consumer_callback", callback)

    async def healthy(database_engine: FakeEngine) -> None:
        assert database_engine is engine

    monkeypatch.setattr(application, "database_healthcheck", healthy)

    app = application.create_app()
    healthcheck = next(
        route.endpoint for route in app.routes if getattr(route, "path", None) == "/health"
    )

    async def run_lifecycle():
        async with application.lifespan(app):
            if worker_fails:
                publisher_task = app.state.worker_tasks[0]
                done, _ = await asyncio.wait({publisher_task}, timeout=1)
                assert publisher_task in done, "The failing publisher did not terminate"
                from fastapi import HTTPException
                with pytest.raises(HTTPException) as caught:
                    await healthcheck()
                assert caught.value.status_code == 503
            else:
                assert await healthcheck() == {"status": "ok"}
            assert app.state.frontier_policy.max_fetch_attempts == 4
            assert app.state.frontier_policy.fetch_lease_seconds == 180
            assert app.state.frontier_policy.max_url_length == 2048
            assert app.state.frontier_policy.max_crawl_depth == 4
            assert app.state.frontier_policy.max_urls_per_crawl == 500
            assert app.state.frontier_policy.max_urls_per_origin == 50
            assert consumer_policies
            assert all(policy is app.state.frontier_policy for policy in consumer_policies)
            assert recovery_policies == [app.state.frontier_policy]

    lifecycle_task = asyncio.create_task(run_lifecycle())
    try:
        done, _ = await asyncio.wait({lifecycle_task}, timeout=1)
        assert lifecycle_task in done, "Shutdown is blocked by a worker ignoring cancellation"
        await lifecycle_task
        if ignores_cancellation:
            assert cancellation_seen.is_set()
            assert not app.state.worker_tasks[0].done()
        assert connection.is_closed is True
        assert engine.disposed is True
    finally:
        # Release the stubborn test worker so it cannot leak into pytest teardown.
        release_worker.set()
        await asyncio.gather(lifecycle_task, *app.state.worker_tasks, return_exceptions=True)
