"""Frontier's squashed initial-schema migration regressions."""

import asyncio
import hashlib

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import select, text

from alembic import command
from crawler_frontier.models import Base, CrawlUrl
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.services import CrawlService

INITIAL_REVISION = "0001_frontier_initial_schema"


async def migrate(action, revision: str) -> None:
    """Run Alembic's synchronous command outside the active event loop."""
    await asyncio.to_thread(action, Config("alembic.ini"), revision)


def test_initial_schema_is_the_only_alembic_head() -> None:
    """Catch reintroduction of an incremental chain after history was squashed."""
    script = ScriptDirectory.from_config(Config("alembic.ini"))

    assert tuple(script.get_heads()) == (INITIAL_REVISION,)
    revision = script.get_revision(INITIAL_REVISION)
    assert revision is not None and revision.down_revision is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_initial_schema_upgrade_creates_the_current_frontier_contract(session_factory) -> None:
    """A new database has the complete final schema after upgrade head."""
    async with session_factory() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == INITIAL_REVISION
        tables = set(await session.scalars(text(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
        )))
        assert {
            "alembic_version",
            "crawls",
            "crawl_urls",
            "links",
            "outbox_events",
            "processed_events",
        } <= tables
        assert "deferred_events" not in tables
        assert await session.scalar(text("SELECT to_regprocedure('public.frontier_set_url_digest()')")) is not None
        assert await session.scalar(text(
            "SELECT count(*) FROM pg_trigger WHERE tgrelid='crawl_urls'::regclass "
            "AND tgname='crawl_urls_set_digest' AND NOT tgisinternal"
        )) == 1
        constraints = set(await session.scalars(text(
            "SELECT conname FROM pg_constraint WHERE conrelid='crawl_urls'::regclass"
        )))
        assert {
            "uq_crawl_urls_crawl_id_url_digest",
            "uq_crawl_urls_crawl_id_id",
            "ck_crawl_urls_digest_length",
            "ck_crawl_urls_digest_matches",
            "ck_crawl_urls_depth_nonnegative",
            "ck_crawl_urls_fetch_attempt_positive",
        } <= constraints
        defaults = dict((await session.execute(text(
            "SELECT column_name, column_default FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='outbox_events' "
            "AND column_name='exchange_name'"
        ))).all())
        assert defaults == {"exchange_name": None}

        def schema_differences(connection):
            context = MigrationContext.configure(connection, opts={"compare_server_default": True})
            return compare_metadata(context, Base.metadata)

        connection = await session.connection()
        assert await connection.run_sync(schema_differences) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_initial_schema_trigger_derives_digest_and_base_round_trip(session_factory) -> None:
    """The initial migration retains database-derived URL identity across a round trip."""
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session, session.begin():
        await session.execute(text(
            "UPDATE crawl_urls SET normalized_url=:url, url_digest=:wrong WHERE crawl_id=:crawl_id"
        ), {
            "url": "https://example.test/changed",
            "wrong": b"x" * 32,
            "crawl_id": created.crawl_id,
        })
    async with session_factory() as session:
        row = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        assert row is not None
        assert row.url_digest == hashlib.sha256(row.normalized_url.encode()).digest()

    await migrate(command.downgrade, "base")
    try:
        async with session_factory() as session:
            assert await session.scalar(text("SELECT to_regclass('public.crawl_urls')")) is None
            assert await session.scalar(text("SELECT to_regprocedure('public.frontier_set_url_digest()')")) is None
            assert await session.scalar(text("SELECT to_regtype('public.url_status')")) is None
        await migrate(command.upgrade, "head")
        async with session_factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == INITIAL_REVISION
    finally:
        await migrate(command.upgrade, "head")
