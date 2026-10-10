import pytest
from pydantic import ValidationError
from sqlalchemy import text

from crawler_frontier.database import (
    create_engine,
    create_session_factory,
    database_healthcheck,
)
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.settings import FrontierSettings


def test_settings_require_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

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
    monkeypatch.setenv("LEASE_RECOVERY_POLL_INTERVAL_SECONDS", "2.5")

    settings = FrontierSettings()

    assert settings.database_url == "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db"
    assert settings.database_pool_size == 8
    assert settings.database_max_overflow == 12
    assert settings.database_pool_timeout_seconds == 7
    assert settings.max_fetch_attempts == 4
    assert settings.fetch_lease_seconds == 180
    assert settings.max_url_length == 2048
    assert settings.lease_recovery_poll_interval_seconds == 2.5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_factory_creates_a_usable_session() -> None:
    settings = FrontierSettings(
        database_url=database_url_from_environment(),
        rabbitmq_url="amqp://crawler:secret@localhost/",
        database_pool_size=1,
        database_max_overflow=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    try:
        async with session_factory() as session:
            assert await session.scalar(text("SELECT 1")) == 1

        await database_healthcheck(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_application_disposes_its_database_engine_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from crawler_frontier import app as application

    class FakeEngine:
        disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    engine = FakeEngine()
    class FakeConnection:
        async def channel(self, *, publisher_confirms: bool):
            return self

        async def set_qos(self, *, prefetch_count: int) -> None:
            return None

        async def declare_exchange(self, *args, **kwargs):
            return object()

        async def get_queue(self, *args, **kwargs):
            return self

        async def consume(self, callback) -> None:
            return None

        async def close(self) -> None:
            return None

    class FakePublisher:
        def __init__(self, *args) -> None:
            pass

        async def run(self, stop_event, poll_interval_seconds: float) -> None:
            await stop_event.wait()

    recovery_policies = []

    class FakeLeaseRecovery:
        def __init__(self, session_factory, policy) -> None:
            recovery_policies.append(policy)

        async def run(self, stop_event, poll_interval_seconds: float) -> None:
            assert poll_interval_seconds == 2.5
            await stop_event.wait()

    async def connect(url: str) -> FakeConnection:
        return FakeConnection()

    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://crawler:secret@rabbitmq/")
    monkeypatch.setenv("MAX_FETCH_ATTEMPTS", "4")
    monkeypatch.setenv("FETCH_LEASE_SECONDS", "180")
    monkeypatch.setenv("MAX_URL_LENGTH", "2048")
    monkeypatch.setenv("LEASE_RECOVERY_POLL_INTERVAL_SECONDS", "2.5")
    monkeypatch.setattr(application, "create_engine", lambda settings: engine)
    monkeypatch.setattr(application, "create_session_factory", lambda database_engine: object())
    monkeypatch.setattr(application, "connect_robust", connect)
    monkeypatch.setattr(application, "OutboxPublisher", FakePublisher)
    monkeypatch.setattr(application, "LeaseRecovery", FakeLeaseRecovery)
    consumer_policies = []

    def callback(event_type, consumer_name, handler_name, session_factory, policy):
        consumer_policies.append(policy)
        return object()

    monkeypatch.setattr(application, "consumer_callback", callback)

    async def healthy(database_engine: FakeEngine) -> None:
        assert database_engine is engine

    monkeypatch.setattr(application, "database_healthcheck", healthy)

    app = application.create_app()
    healthcheck = next(route.endpoint for route in app.routes if route.path == "/health")

    async with application.lifespan(app):
        assert await healthcheck() == {"status": "ok"}
        assert app.state.frontier_policy.max_fetch_attempts == 4
        assert app.state.frontier_policy.fetch_lease_seconds == 180
        assert app.state.frontier_policy.max_url_length == 2048
        assert consumer_policies
        assert all(policy is app.state.frontier_policy for policy in consumer_policies)
        assert recovery_policies == [app.state.frontier_policy]

    assert engine.disposed is True
