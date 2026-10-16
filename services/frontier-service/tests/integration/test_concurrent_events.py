"""Race regressions with separate PostgreSQL transactions and real row locks."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from crawler_contracts import (
    FetchRetryRequestedEvent,
    LinksExtractedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy import delete, func, select, text

from crawler_frontier.application.crawl_service import (
    CrawlService,
    SourceUrlNotFoundError,
)
from crawler_frontier.application.event_handlers import FrontierEventHandler
from crawler_frontier.database import create_engine, create_session_factory
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent
from crawler_frontier.settings import FrontierSettings
from crawler_frontier.workers.lease_recovery import LeaseRecovery
from tests.integration.helpers import _create_url


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["two_retries", "processed_then_fetched", "recovery_then_fetched", "links_then_recovery"])
async def test_different_events_cannot_overwrite_a_locked_attempt(scenario: str) -> None:
    engine = create_engine(FrontierSettings(
        database_url=database_url_from_environment(), rabbitmq_url="amqp://localhost/",
    ))
    factory = create_session_factory(engine)
    crawl_id, url_id = uuid4(), uuid4()
    now = datetime.now(UTC)
    async with factory() as session, session.begin():
        session.add(Crawl(id=crawl_id, seed_urls=[], allowed_hosts=[]))
        await session.flush()
        session.add(CrawlUrl(
            id=url_id, crawl_id=crawl_id, normalized_url="https://race.example/",
            origin="https://race.example", depth=0, status=UrlStatus.FETCHING,
            fetch_attempt=1, lease_until=now - timedelta(seconds=1),
        ))

    read_complete = asyncio.Event()
    release_read = asyncio.Event()

    class PausingHandler(FrontierEventHandler):
        async def _claim(self, event, consumer_name):
            row = await super()._claim(event, consumer_name)
            read_complete.set()
            await release_read.wait()
            return row

    fetched = PageFetchedEvent(
        crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, final_url="https://race.example/",
        http_status=200, content_type="text/html", content_ref="race.html",
    )
    retry = FetchRetryRequestedEvent(
        crawl_id=crawl_id, url_id=url_id, fetch_attempt=1,
        category="connection", detail="reset", suggested_delay_seconds=30,
    )

    async def paused():
        async with factory() as session:
            handler = PausingHandler(session, FrontierPolicy())
            if scenario == "two_retries":
                await handler.handle_fetch_retry_requested(retry, "fetch.retry_requested")
            elif scenario == "links_then_recovery":
                await handler.handle_links_extracted(LinksExtractedEvent(
                    crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, links=[],
                ), "links.extracted")
            else:
                await handler.handle_page_fetched(fetched, "page.fetched")

    async def other():
        async with factory() as session:
            # A second connection must block on the existing crawl lock, not read stale state.
            await session.execute(text("SET lock_timeout = '300ms'"))
            await session.commit()
            handler = FrontierEventHandler(session, FrontierPolicy())
            if scenario == "two_retries":
                event = retry.model_copy(update={"event_id": uuid4()})
                await handler.handle_fetch_retry_requested(event, "fetch.retry_requested")
            elif scenario == "processed_then_fetched":
                await handler.handle_page_processed(PageProcessedEvent(
                    crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, page_id=uuid4(),
                ), "page.processed")
            else:
                return await LeaseRecovery(factory, FrontierPolicy()).recover_expired_once(now=now)

    task = asyncio.create_task(paused())
    try:
        await asyncio.wait_for(read_complete.wait(), 5)
        if scenario in {"recovery_then_fetched", "links_then_recovery"}:
            assert await asyncio.wait_for(other(), 5) == 0
        else:
            from sqlalchemy.exc import DBAPIError

            with pytest.raises(DBAPIError, match="lock timeout"):
                await asyncio.wait_for(other(), 5)
        release_read.set()
        await asyncio.wait_for(task, 5)
        await other()
        async with factory() as session:
            row = await session.get(CrawlUrl, url_id)
            count = await session.scalar(select(func.count()).select_from(OutboxEvent).where(
                OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id),
            ))
            if scenario == "two_retries":
                assert (row.status, row.fetch_attempt, count) == (UrlStatus.QUEUED, 2, 1)
            elif scenario == "processed_then_fetched":
                assert row.status is UrlStatus.FETCHED
            else:
                assert (row.status, row.fetch_attempt, count) == (UrlStatus.DOWNLOADED, 1, 0)
    finally:
        release_read.set()
        await asyncio.gather(task, return_exceptions=True)
        async with factory() as session, session.begin():
            await session.execute(delete(OutboxEvent).where(
                OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id),
            ))
            await session.execute(delete(Crawl).where(Crawl.id == crawl_id))
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admission_never_locks_a_source_from_another_crawl(session_factory):
    crawl_a, _ = await _create_url(session_factory, status=UrlStatus.QUEUED)
    _, source_b = await _create_url(session_factory, status=UrlStatus.QUEUED)
    async with session_factory() as locker, locker.begin():
        await locker.scalar(select(CrawlUrl).where(CrawlUrl.id == source_b).with_for_update())
        async with session_factory() as caller:
            await caller.execute(text("SET lock_timeout='300ms'"))
            await caller.commit()
            with pytest.raises(SourceUrlNotFoundError):
                await asyncio.wait_for(CrawlService(caller, FrontierPolicy()).admit_url(
                    crawl_id=crawl_a, source_url_id=source_b, url="https://example.test/", depth=1,
                ), 3)
