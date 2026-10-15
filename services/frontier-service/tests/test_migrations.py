"""Nonempty PostgreSQL migration and preflight regressions."""

import asyncio
import hashlib
import json
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from crawler_contracts import PageProcessedEvent
from sqlalchemy import event, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import command
from crawler_frontier.event_handlers import FrontierEventHandler
from crawler_frontier.models import (
    Base,
    Crawl,
    CrawlUrl,
    OutboxEvent,
    ProcessedEvent,
)
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.services import CrawlService


async def migrate(action, revision):
    await asyncio.to_thread(action, Config("alembic.ini"), revision)


async def insert_legacy_deferred(session, *, crawl_id, url_id, event_id, fetch_attempt=1,
                                 event_type="page.processed", payload=None):
    await session.execute(text(
        "INSERT INTO deferred_events(id,consumer_name,event_id,crawl_id,url_id,fetch_attempt,event_type,payload) "
        "VALUES(:id,:consumer,:event,:crawl,:url,:attempt,:type,CAST(:payload AS jsonb))"
    ), {"id": uuid4(), "consumer": event_type, "event": event_id, "crawl": crawl_id,
        "url": url_id, "attempt": fetch_attempt, "type": event_type, "payload": json.dumps(payload or {})})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_latest_schema_does_not_require_a_deferred_event_ledger(session_factory):
    async with session_factory() as session:
        assert await session.scalar(text("SELECT to_regclass('public.deferred_events')")) is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upgrade_chain_backfills_exchange_without_a_persistent_default(session_factory):
    event_id = uuid4()
    payload = {"event_id": str(event_id), "legacy": True}
    await migrate(command.downgrade, "0002_link_crawl_integrity")
    try:
        async with session_factory() as session, session.begin():
            await session.execute(text(
                "INSERT INTO outbox_events(event_id,routing_key,payload,created_at,publish_attempts) "
                "VALUES(:id,'fetch.url',CAST(:payload AS jsonb),now(),0)"
            ), {"id": event_id, "payload": json.dumps(payload)})
        await migrate(command.upgrade, "head")

        async with session_factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0008_remove_deferred_events"
            row = await session.get(OutboxEvent, event_id)
            assert row.exchange_name == "crawler.topic"
            assert row.routing_key == "fetch.url"
            assert row.payload == payload and row.published_at is None
            column = (await session.execute(text(
                "SELECT column_default,is_nullable FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='outbox_events' AND column_name='exchange_name'"
            ))).one()
            assert column.column_default is None
            assert column.is_nullable == "NO"

            def schema_differences(connection):
                context = MigrationContext.configure(connection, opts={"compare_server_default": True})
                return compare_metadata(context, Base.metadata)

            connection = await session.connection()
            assert await connection.run_sync(schema_differences) == []
    finally:
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upgrade_preserves_nonempty_graph_attempts_and_pending_work(session_factory):
    crawl_id, source_id, target_id, event_id = (uuid4() for _ in range(4))
    source, target = "https://bücher.test/", "https://bücher.test/café"
    payload = {"crawl_id": str(crawl_id), "url_id": str(source_id), "fetch_attempt": 2,
               "url": "https://xn--bcher-kva.test/", "depth": 0}
    await migrate(command.downgrade, "0005_deferred_events")
    try:
        async with session_factory() as session, session.begin():
            await session.execute(text("INSERT INTO crawls(id,seed_urls,allowed_hosts,created_at) VALUES (:id,CAST(:seeds AS jsonb),CAST(:hosts AS jsonb),now())"),
                                  {"id": crawl_id, "seeds": json.dumps([source]), "hosts": json.dumps(["bücher.test"])})
            for url_id, url, depth in ((source_id, source, 0), (target_id, target, 1)):
                await session.execute(text("INSERT INTO crawl_urls(id,crawl_id,normalized_url,origin,depth,status,fetch_attempt,lease_until,created_at,updated_at) VALUES(:id,:crawl,:url,:origin,:depth,'fetching',2,now()+interval '1 minute',now(),now())"),
                                      {"id": url_id, "crawl": crawl_id, "url": url, "origin": "https://bücher.test", "depth": depth})
            await session.execute(text("INSERT INTO links(crawl_id,source_url_id,target_url_id,created_at) VALUES(:crawl,:source,:target,now())"),
                                  {"crawl": crawl_id, "source": source_id, "target": target_id})
            session.add(OutboxEvent(event_id=event_id, exchange_name="crawler.topic", routing_key="fetch.url", payload=payload))
        await migrate(command.upgrade, "head")
        async with session_factory() as session:
            row = await session.get(CrawlUrl, source_id)
            assert row.normalized_url == "https://xn--bcher-kva.test/"
            assert row.origin == "https://xn--bcher-kva.test"
            assert row.fetch_attempt == 2 and row.lease_until is not None
            assert row.url_digest == hashlib.sha256(row.normalized_url.encode()).digest()
            crawl = await session.get(Crawl, crawl_id)
            assert crawl.allowed_hosts == ["xn--bcher-kva.test"]
            assert await session.scalar(text("SELECT count(*) FROM links WHERE crawl_id=:id"), {"id": crawl_id}) == 1
            assert (await session.get(OutboxEvent, event_id)).payload == payload
    finally:
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_canonical_collision_preflight_aborts_without_merging(session_factory):
    await migrate(command.downgrade, "0006_url_digest")
    crawl_id = uuid4()
    urls = ["https://bücher.test/", "https://xn--bcher-kva.test/"]
    try:
        async with session_factory() as session, session.begin():
            session.add(Crawl(id=crawl_id, seed_urls=[], allowed_hosts=[]))
            await session.flush()
            for url in urls:
                session.add(CrawlUrl(crawl_id=crawl_id, normalized_url=url, origin="https://example.test", depth=0))
        with pytest.raises(RuntimeError, match="Canonical URL preflight"):
            await migrate(command.upgrade, "head")
        async with session_factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0006_url_digest"
            stored = list(await session.scalars(select(CrawlUrl.normalized_url).where(CrawlUrl.crawl_id == crawl_id)))
            assert sorted(stored) == sorted(urls)
    finally:
        async with session_factory() as session, session.begin():
            await session.execute(text("DELETE FROM crawls WHERE id=:id"), {"id": crawl_id})
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_trigger_recomputes_the_digest_on_every_update(session_factory):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session, session.begin():
        await session.execute(text("UPDATE crawl_urls SET normalized_url=:url, url_digest=:wrong WHERE crawl_id=:id"),
                              {"url": "https://example.test/changed", "wrong": b"x" * 32, "id": created.crawl_id})
    async with session_factory() as session:
        row = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        assert row.url_digest == hashlib.sha256(row.normalized_url.encode()).digest()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_downgrade_and_reupgrade_preserve_short_url_graph_and_delivery_state(session_factory):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    async with session_factory() as session:
        target = await CrawlService(session, FrontierPolicy()).admit_url(
            crawl_id=created.crawl_id, source_url_id=source.id, url="https://example.test/child", depth=1,
        )
    pending_event_id, processed_event_id = uuid4(), uuid4()
    await migrate(command.downgrade, "0007_canonical_urls")
    async with session_factory() as session, session.begin():
        await insert_legacy_deferred(session,
            event_id=pending_event_id,
            crawl_id=created.crawl_id, url_id=source.id, fetch_attempt=1,
            event_type="page.processed", payload={},
        )
        session.add(ProcessedEvent(consumer_name="migration.test", event_id=processed_event_id))

    try:
        await migrate(command.downgrade, "0004_crawl_url_origin")
        async with session_factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0004_crawl_url_origin"
            assert await session.scalar(text("SELECT to_regclass('public.deferred_events')")) is None
            assert await session.scalar(text("SELECT count(*) FROM pg_constraint WHERE conrelid='crawl_urls'::regclass AND conname='uq_crawl_urls_crawl_id_normalized_url'")) == 1
            assert await session.scalar(text("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='crawl_urls' AND column_name='url_digest'")) == 0
            assert await session.scalar(text("SELECT character_maximum_length FROM information_schema.columns WHERE table_schema='public' AND table_name='crawl_urls' AND column_name='normalized_url'")) == 4096
            assert await session.scalar(text("SELECT to_regprocedure('public.frontier_set_url_digest()')")) is None
            assert list((await session.execute(text("SELECT source_url_id,target_url_id FROM links"))).tuples()) == [(source.id, target.url_id)]
            assert await session.scalar(text("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")) == 2
            assert await session.scalar(text("SELECT count(*) FROM processed_events WHERE event_id=:id"), {"id": processed_event_id}) == 1
        await migrate(command.upgrade, "head")
        async with session_factory() as session:
            assert (await session.get(CrawlUrl, source.id)).normalized_url == "https://example.test/"
            assert (await session.get(CrawlUrl, target.url_id)).depth == 1
            # Historical 0005 downgrade drops rows; latest schema removes the ledger entirely.
            assert await session.scalar(text("SELECT to_regclass('public.deferred_events')")) is None
        await asyncio.to_thread(command.check, Config("alembic.ini"))
    finally:
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_digest_downgrade_refuses_long_url_without_losing_data(session_factory):
    value = "https://example.test/" + "x" * 5000
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy(max_url_length=6000)).create_crawl([value])
    try:
        await migrate(command.downgrade, "0006_url_digest")
        with pytest.raises(DBAPIError):
            await migrate(command.downgrade, "0005_deferred_events")
        async with session_factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0006_url_digest"
            row = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
            assert row.normalized_url == value
            assert row.url_digest == hashlib.sha256(value.encode()).digest()
            assert await session.scalar(text("SELECT count(*) FROM pg_constraint WHERE conrelid='crawl_urls'::regclass AND conname='uq_crawl_urls_crawl_id_url_digest'")) == 1
            assert await session.scalar(text("SELECT count(*) FROM pg_constraint WHERE conrelid='crawl_urls'::regclass AND conname='uq_crawl_urls_crawl_id_normalized_url'")) == 0
    finally:
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["attempt", "type", "crawl"])
async def test_deferred_migration_constraints_reject_invalid_rows(session_factory, invalid):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    await migrate(command.downgrade, "0007_canonical_urls")
    try:
        async with session_factory() as session:
            with pytest.raises(IntegrityError) as caught:
                async with session.begin():
                    await insert_legacy_deferred(session,
                        event_id=uuid4(),
                        crawl_id=(uuid4() if invalid == "crawl" else created.crawl_id),
                        url_id=source.id, fetch_attempt=(0 if invalid == "attempt" else 1),
                        event_type=("fetch.started" if invalid == "type" else "page.processed"), payload={},
                    )
            assert caught.value.orig.sqlstate == ("23503" if invalid == "crawl" else "23514")
    finally:
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_removal_migration_preserves_graph_and_restores_historical_schema(session_factory):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    async with session_factory() as session:
        target = await CrawlService(session, FrontierPolicy()).admit_url(
            crawl_id=created.crawl_id, source_url_id=source.id, url="https://example.test/child", depth=1,
        )
    marker_id = uuid4()
    async with session_factory() as session, session.begin():
        session.add(ProcessedEvent(consumer_name="migration.test", event_id=marker_id))
    await migrate(command.downgrade, "0007_canonical_urls")
    try:
        async with session_factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM deferred_events")) == 0
            constraints = set(await session.scalars(text(
                "SELECT conname FROM pg_constraint WHERE conrelid='deferred_events'::regclass"
            )))
            assert {"uq_deferred_events_consumer_event", "ck_deferred_events_attempt", "ck_deferred_events_type"} <= constraints
            assert await session.scalar(text("SELECT to_regclass('public.ix_deferred_events_url_receipt')")) is not None
            assert await session.scalar(text(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_name='deferred_events' AND column_name='received_at'"
            )) == "clock_timestamp()"
        await migrate(command.upgrade, "head")
        async with session_factory() as session:
            assert await session.scalar(text("SELECT to_regclass('public.deferred_events')")) is None
            assert (await session.get(CrawlUrl, source.id)).fetch_attempt == 1
            assert (await session.get(CrawlUrl, target.url_id)).depth == 1
            assert await session.scalar(text("SELECT count(*) FROM links")) == 1
            assert await session.scalar(text("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")) == 2
            assert await session.scalar(text("SELECT count(*) FROM processed_events WHERE event_id=:id"), {"id": marker_id}) == 1
        await asyncio.to_thread(command.check, Config("alembic.ini"))
    finally:
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_removal_migration_refuses_pending_events_until_reconciled(session_factory):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    pending = PageProcessedEvent(crawl_id=created.crawl_id, url_id=source.id, fetch_attempt=1, page_id=uuid4())
    payload = pending.model_dump(mode="json")
    await migrate(command.downgrade, "0007_canonical_urls")
    try:
        async with session_factory() as session, session.begin():
            await insert_legacy_deferred(session, crawl_id=created.crawl_id, url_id=source.id,
                                         event_id=pending.event_id, payload=payload)
        with pytest.raises(RuntimeError, match="pending deferred events"):
            await migrate(command.upgrade, "head")
        async with session_factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0007_canonical_urls"
            assert await session.scalar(text("SELECT payload FROM deferred_events")) == payload
            assert await session.scalar(text("SELECT count(*) FROM processed_events")) == 0
        # New handlers can reconcile the old envelope before the table is removed.
        async with session_factory() as session:
            await FrontierEventHandler(session, FrontierPolicy()).handle_page_processed(pending, "page.processed")
        async with session_factory() as session, session.begin():
            assert (await session.get(CrawlUrl, source.id)).status.value == "fetched"
            assert await session.scalar(text("SELECT count(*) FROM processed_events WHERE event_id=:id"), {"id": pending.event_id}) == 1
            await session.execute(text("DELETE FROM deferred_events WHERE event_id=:id"), {"id": pending.event_id})
        await migrate(command.upgrade, "head")
        async with session_factory() as session:
            assert (await session.get(CrawlUrl, source.id)).status.value == "fetched"
            assert await session.scalar(text("SELECT count(*) FROM outbox_events")) == 1
    finally:
        # Test-owned data only; allow fixture cleanup even after an assertion failure.
        async with session_factory() as session, session.begin():
            await session.execute(text("DELETE FROM crawls WHERE id=:id"), {"id": created.crawl_id})
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_removal_preflight_waits_for_legacy_writer_and_preserves_its_event(session_factory):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    await migrate(command.downgrade, "0007_canonical_urls")
    event_id = uuid4()
    migration_task = None
    try:
        async with session_factory() as writer, writer.begin():
            await insert_legacy_deferred(writer, crawl_id=created.crawl_id, url_id=source.id, event_id=event_id)
            migration_task = asyncio.create_task(migrate(command.upgrade, "head"))

            async def wait_for_exclusive_lock():
                async with session_factory() as observer:
                    while not await observer.scalar(text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks "
                        "WHERE relation='deferred_events'::regclass AND mode='AccessExclusiveLock' "
                        "AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND NOT granted)"
                    )):
                        if migration_task.done():
                            await migration_task
                            pytest.fail("Removal did not wait for the legacy writer")
                        await asyncio.sleep(0.01)

            await asyncio.wait_for(wait_for_exclusive_lock(), 5)
            assert not migration_task.done()
        # The writer commits before removal gets its lock; preflight must see it.
        with pytest.raises(RuntimeError, match="pending deferred events"):
            await asyncio.wait_for(migration_task, 5)
        async with session_factory() as session:
            assert await session.scalar(text("SELECT event_id FROM deferred_events")) == event_id
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0007_canonical_urls"
    finally:
        if migration_task is not None:
            await asyncio.wait_for(asyncio.gather(migration_task, return_exceptions=True), 5)
        async with session_factory() as session, session.begin():
            await session.execute(text("DELETE FROM crawls WHERE id=:id"), {"id": created.crawl_id})
        await migrate(command.upgrade, "head")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_operator_timeout_wrapper_sets_actual_alembic_connection_limits():
    observed_settings = []

    def timed_engine(configuration, **kwargs):
        kwargs["connect_args"] = {"server_settings": {"lock_timeout": "5s", "statement_timeout": "10min"}}
        engine = async_engine_from_config(configuration, **kwargs)

        @event.listens_for(engine.sync_engine, "connect")
        def observe_limits(dbapi_connection, _connection_record):
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("SELECT current_setting('lock_timeout'), current_setting('statement_timeout')")
                observed_settings.append(cursor.fetchone())
            finally:
                cursor.close()

        return engine

    # env.py imports the patched factory; no production environment/code is edited.
    with patch("sqlalchemy.ext.asyncio.async_engine_from_config", timed_engine):
        await migrate(command.upgrade, "0007_canonical_urls")
    assert observed_settings
    assert all(settings == ("5s", "10min") for settings in observed_settings)
