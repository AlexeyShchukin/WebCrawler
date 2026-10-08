"""Integration tests for Frontier's idempotent pipeline-event consumers."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from crawler_contracts import (
    FailureCategory,
    FailureStage,
    FetchRetryCategory,
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    FetchUrlEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.database import create_engine, create_session_factory
from crawler_frontier.event_handlers import FrontierEventHandler
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent, ProcessedEvent
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.settings import FrontierSettings
from crawler_frontier.state_machine import UrlStatus


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(
        FrontierSettings(
            database_url=database_url_from_environment(),
            rabbitmq_url="amqp://crawler:secret@localhost/",
        )
    )
    yield create_session_factory(engine)
    await engine.dispose()


async def _create_url(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: UrlStatus,
    fetch_attempt: int = 1,
    lease_until: datetime | None = None,
) -> tuple[UUID, UUID]:
    async with session_factory() as session, session.begin():
        crawl = Crawl(seed_urls=[], allowed_hosts=[])
        session.add(crawl)
        await session.flush()
        url = CrawlUrl(
            crawl_id=crawl.id,
            normalized_url=f"https://{uuid4().hex}.example.test/page",
            depth=0,
            status=status,
            fetch_attempt=fetch_attempt,
            lease_until=lease_until,
        )
        session.add(url)
        await session.flush()
        return crawl.id, url.id


async def _remove_crawl(session_factory: async_sessionmaker[AsyncSession], crawl_id: UUID) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(delete(Crawl).where(Crawl.id == crawl_id))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_started_claims_matching_queued_attempt_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    fetcher_timestamp = datetime(2020, 1, 1, tzinfo=UTC)
    event = FetchStartedEvent(
        crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, created_at=fetcher_timestamp
    )

    try:
        async with session_factory() as session:
            handler = FrontierEventHandler(session, FrontierPolicy(fetch_lease_seconds=7))
            before_start = datetime.now(UTC)
            await handler.handle_fetch_started(event, "fetch.started")
            after_start = datetime.now(UTC)
            await handler.handle_fetch_started(event, "fetch.started")

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            processed = list(
                await session.scalars(
                    select(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
                )
            )

        assert url is not None
        assert url.status is UrlStatus.FETCHING
        assert before_start + timedelta(seconds=7) <= url.lease_until <= after_start + timedelta(seconds=7)
        assert len(processed) == 1
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_page_fetched_releases_matching_fetch_lease_and_enters_downloaded(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    crawl_id, url_id = await _create_url(
        session_factory,
        status=UrlStatus.FETCHING,
        lease_until=datetime(2026, 10, 6, 12, 2, tzinfo=UTC),
    )
    event = PageFetchedEvent(
        crawl_id=crawl_id,
        url_id=url_id,
        fetch_attempt=1,
        final_url="https://example.test/final",
        http_status=200,
        content_type="text/html",
        content_ref="crawls/example/page.html",
    )

    try:
        async with session_factory() as session:
            await FrontierEventHandler(session, FrontierPolicy()).handle_page_fetched(event, "page.fetched")

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)

        assert url is not None
        assert url.status is UrlStatus.DOWNLOADED
        assert url.lease_until is None
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_page_processed_accepts_fetched_result_before_fetched_delivery(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.FETCHING)
    event = PageProcessedEvent(crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, page_id=uuid4())

    try:
        async with session_factory() as session:
            await FrontierEventHandler(session, FrontierPolicy()).handle_page_processed(event, "page.processed")

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)

        assert url is not None
        assert url.status is UrlStatus.FETCHED
        assert url.lease_until is None
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [FailureStage.FETCH, FailureStage.CONTENT])
async def test_page_failed_does_not_fail_a_queued_attempt(
    session_factory: async_sessionmaker[AsyncSession],
    stage: FailureStage,
) -> None:
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    event = PageFailedEvent(
        crawl_id=crawl_id,
        url_id=url_id,
        fetch_attempt=1,
        stage=stage,
        category=FailureCategory.TIMEOUT,
        detail="late failure from an older execution",
    )

    try:
        async with session_factory() as session:
            await FrontierEventHandler(session, FrontierPolicy()).handle_page_failed(event, "page.failed")

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)

        assert url is not None
        assert url.status is UrlStatus.QUEUED
        assert url.last_error_category is None
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_request_allocates_one_new_attempt_and_delays_its_fetch_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.FETCHING)
    event = FetchRetryRequestedEvent(
        crawl_id=crawl_id,
        url_id=url_id,
        fetch_attempt=1,
        category=FetchRetryCategory.CONNECTION,
        detail="connection reset",
        suggested_delay_seconds=31,
    )

    try:
        async with session_factory() as session:
            handler = FrontierEventHandler(session, FrontierPolicy())
            await handler.handle_fetch_retry_requested(event, "fetch.retry_requested")
            await handler.handle_fetch_retry_requested(event, "fetch.retry_requested")

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id))
                )
            )

        assert url is not None
        assert url.status is UrlStatus.QUEUED
        assert url.fetch_attempt == 2
        assert url.lease_until is None
        assert len(outbox_events) == 1
        assert outbox_events[0].exchange_name == "crawler.retry"
        assert outbox_events[0].routing_key == "fetch.url.retry.2m"
        assert FetchUrlEvent.model_validate(outbox_events[0].payload).fetch_attempt == 2
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_duplicate_delivery_is_acknowledgeable_without_unique_constraint_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    event = FetchStartedEvent(crawl_id=crawl_id, url_id=url_id, fetch_attempt=1)

    async def consume_once() -> None:
        async with session_factory() as session:
            await FrontierEventHandler(session, FrontierPolicy()).handle_fetch_started(event, "fetch.started")

    try:
        await asyncio.gather(consume_once(), consume_once())

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            processed = list(
                await session.scalars(
                    select(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
                )
            )

        assert url is not None
        assert url.status is UrlStatus.FETCHING
        assert len(processed) == 1
    finally:
        await _remove_crawl(session_factory, crawl_id)
