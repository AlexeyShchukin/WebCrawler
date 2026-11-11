"""Content migrations and constraints against a disposable PostgreSQL database."""
import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import insert, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command
from crawler_content.database import create_session_factory, database_healthcheck
from crawler_content.models import Base, Keyword, Page, ProcessedEvent

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def engine(isolated_database):
    db = create_async_engine(isolated_database, pool_size=2, max_overflow=1, pool_timeout=3)
    try:
        yield db
    finally:
        await db.dispose()


async def test_migration_round_trip_and_metadata(engine):
    assert ScriptDirectory.from_config(Config("alembic.ini")).get_heads() == ["0001_content_initial_schema"]
    await database_healthcheck(engine)
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT current_database()")) != "frontier_db"
        assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "0001_content_initial_schema"
        differences = await connection.run_sync(
            lambda sync: compare_metadata(MigrationContext.configure(sync, opts={"compare_server_default": True}), Base.metadata)
        )
        assert differences == []
    await asyncio.to_thread(command.downgrade, Config("alembic.ini"), "base")
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT to_regclass('public.pages')")) is None
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT to_regclass('public.pages')")) is not None


async def test_unique_constraints_and_rollback(engine):
    factory = create_session_factory(engine)
    now = datetime.now(UTC)
    url_id, crawl_id, event_id = uuid4(), uuid4(), uuid4()
    page = {"id": uuid4(), "url_id": url_id, "crawl_id": crawl_id, "fetch_attempt": 1,
            "source_event_id": event_id, "url": "https://example.test/", "http_status": 200,
            "content_type": "text/html", "content_ref": "test", "fetched_at": now,
            "processing_state": "indexing_pending", "title": "", "text": "", "extracted_links": []}
    async with factory() as session, session.begin():
        await session.execute(insert(Page).values(**page))
        await session.execute(insert(Keyword).values(id=uuid4(), normalized_term="example"))
        await session.execute(insert(ProcessedEvent).values(id=uuid4(), consumer_name="content", event_id=event_id, received_event={}))
    for statement in (
        insert(Page).values(**{**page, "id": uuid4()}),
        insert(Keyword).values(id=uuid4(), normalized_term="example"),
        insert(ProcessedEvent).values(id=uuid4(), consumer_name="content", event_id=event_id, received_event={}),
    ):
        with pytest.raises(IntegrityError):
            async with factory() as session, session.begin():
                await session.execute(statement)
    async with factory() as session:
        session.add(Keyword(id=uuid4(), normalized_term="rolledback"))
        await session.flush()
        await session.rollback()
    async with factory() as session:
        assert await session.scalar(select(Keyword.id).where(Keyword.normalized_term == "rolledback")) is None
    assert engine.pool.size() == 2 and engine.pool._max_overflow == 1
