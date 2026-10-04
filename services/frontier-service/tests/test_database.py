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
    monkeypatch.setenv("DATABASE_POOL_SIZE", "8")
    monkeypatch.setenv("DATABASE_MAX_OVERFLOW", "12")
    monkeypatch.setenv("DATABASE_POOL_TIMEOUT_SECONDS", "7")

    settings = FrontierSettings()

    assert settings.database_url == "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db"
    assert settings.database_pool_size == 8
    assert settings.database_max_overflow == 12
    assert settings.database_pool_timeout_seconds == 7


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_factory_creates_a_usable_session() -> None:
    settings = FrontierSettings(
        database_url=database_url_from_environment(),
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
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:secret@postgres:5432/frontier_db")
    monkeypatch.setattr(application, "create_engine", lambda settings: engine)
    monkeypatch.setattr(application, "create_session_factory", lambda database_engine: object())

    async def healthy(database_engine: FakeEngine) -> None:
        assert database_engine is engine

    monkeypatch.setattr(application, "database_healthcheck", healthy)

    app = application.create_app()
    healthcheck = next(route.endpoint for route in app.routes if route.path == "/health")

    async with application.lifespan(app):
        assert await healthcheck() == {"status": "ok"}

    assert engine.disposed is True
