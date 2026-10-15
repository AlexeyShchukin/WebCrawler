"""Integration tests for Frontier's idempotent pipeline-event consumers."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
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

from crawler_frontier.application.event_handlers import FrontierEventHandler
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent, ProcessedEvent
from tests.integration.helpers import _create_url


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
async def test_page_failed_finishes_current_queued_attempt_without_started(
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
        detail="failure of the allocated current execution",
    )

    try:
        async with session_factory() as session:
            await FrontierEventHandler(session, FrontierPolicy()).handle_page_failed(event, "page.failed")

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)

        assert url is not None
        assert url.status is UrlStatus.FAILED
        assert url.lease_until is None
        assert url.last_error_category == "timeout"
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


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["older", "future", "unknown", "other_crawl"])
async def test_invalid_attempt_or_identity_never_changes_state_or_schedules_work(session_factory, identity):
    from crawler_frontier.application.event_handlers import EventIdentityError

    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.FETCHING, fetch_attempt=2)
    event = PageProcessedEvent(crawl_id=(uuid4() if identity == "other_crawl" else crawl_id),
                               url_id=(uuid4() if identity == "unknown" else url_id),
                               fetch_attempt=(1 if identity == "older" else 3), page_id=uuid4())
    async with session_factory() as session:
        if identity == "older":
            await FrontierEventHandler(session, FrontierPolicy()).handle_page_processed(event, "page.processed")
        else:
            with pytest.raises(EventIdentityError):
                await FrontierEventHandler(session, FrontierPolicy()).handle_page_processed(event, "page.processed")
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is UrlStatus.FETCHING and row.fetch_attempt == 2
        assert list(await session.scalars(select(OutboxEvent))) == []
        assert len(list(await session.scalars(select(ProcessedEvent)))) == (1 if identity == "older" else 0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_on_final_attempt_fails_without_outbox(session_factory):
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.FETCHING, fetch_attempt=3)
    event = FetchRetryRequestedEvent(crawl_id=crawl_id, url_id=url_id, fetch_attempt=3,
                                    category="timeout", detail="timed out", suggested_delay_seconds=30)
    async with session_factory() as session:
        handler = FrontierEventHandler(session, FrontierPolicy(max_fetch_attempts=3))
        await handler.handle_fetch_retry_requested(event, "fetch.retry_requested")
        await handler.handle_fetch_retry_requested(event, "fetch.retry_requested")
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is UrlStatus.FAILED and row.fetch_attempt == 3 and row.lease_until is None
        assert list(await session.scalars(select(OutboxEvent))) == []
