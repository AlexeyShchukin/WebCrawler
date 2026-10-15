import pytest
from sqlalchemy import text

from crawler_frontier.database import (
    create_engine,
    create_session_factory,
    database_healthcheck,
)
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.settings import FrontierSettings


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
