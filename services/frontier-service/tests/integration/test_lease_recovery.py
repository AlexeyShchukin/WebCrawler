"""Integration tests for Frontier recovery of abandoned fetch leases."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from crawler_contracts import FetchUrlEvent
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent
from crawler_frontier.workers.lease_recovery import LeaseRecovery


async def _create_fetching_url(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    fetch_attempt: int = 1,
    lease_until: datetime,
) -> tuple[UUID, UUID]:
    async with session_factory() as session, session.begin():
        crawl = Crawl(seed_urls=[], allowed_hosts=[])
        session.add(crawl)
        await session.flush()
        url = CrawlUrl(
            crawl_id=crawl.id,
            normalized_url=f"https://{uuid4().hex}.example.test/page",
            origin="https://example.test",
            depth=2,
            status=UrlStatus.FETCHING,
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
async def test_expired_lease_allocates_next_attempt_through_the_frontier_retry_queue(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime(2026, 10, 10, 12, tzinfo=UTC)
    crawl_id, url_id = await _create_fetching_url(
        session_factory,
        lease_until=now - timedelta(seconds=1),
    )

    try:
        recovery = LeaseRecovery(session_factory, FrontierPolicy(max_fetch_attempts=3))

        assert await recovery.recover_expired_once(now=now) == 1
        assert await recovery.recover_expired_once(now=now) == 0

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id)
                    )
                )
            )

        assert url is not None
        assert url.status is UrlStatus.QUEUED
        assert url.fetch_attempt == 2
        assert url.lease_until is None
        assert len(outbox_events) == 1
        assert outbox_events[0].exchange_name == "crawler.retry"
        assert outbox_events[0].routing_key == "fetch.url.retry.30s"
        scheduled = FetchUrlEvent.model_validate(outbox_events[0].payload)
        assert scheduled.fetch_attempt == 2
        assert scheduled.depth == 2
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expired_final_attempt_is_failed_without_another_scheduled_fetch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime(2026, 10, 10, 12, tzinfo=UTC)
    crawl_id, url_id = await _create_fetching_url(
        session_factory,
        fetch_attempt=3,
        lease_until=now,
    )

    try:
        recovery = LeaseRecovery(session_factory, FrontierPolicy(max_fetch_attempts=3))

        assert await recovery.recover_expired_once(now=now) == 1

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id)
                    )
                )
            )

        assert url is not None
        assert url.status is UrlStatus.FAILED
        assert url.fetch_attempt == 3
        assert url.lease_until is None
        assert url.last_error_category == "lease_expired"
        assert len(outbox_events) == 0
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unexpired_lease_is_not_recovered(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime(2026, 10, 10, 12, tzinfo=UTC)
    crawl_id, url_id = await _create_fetching_url(
        session_factory,
        lease_until=now + timedelta(seconds=1),
    )

    try:
        recovery = LeaseRecovery(session_factory, FrontierPolicy())

        assert await recovery.recover_expired_once(now=now) == 0

        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id)
                    )
                )
            )

        assert url is not None
        assert url.status is UrlStatus.FETCHING
        assert url.fetch_attempt == 1
        assert url.lease_until == now + timedelta(seconds=1)
        assert outbox_events == []
    finally:
        await _remove_crawl(session_factory, crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_recovery_replicas_allocate_only_one_retry(session_factory):
    now = datetime.now(UTC)
    crawl_id, url_id = await _create_fetching_url(session_factory, lease_until=now)
    gate = asyncio.Barrier(2)

    async def recover():
        await gate.wait()
        return await LeaseRecovery(session_factory, FrontierPolicy()).recover_expired_once(now=now)

    assert sorted(await asyncio.wait_for(asyncio.gather(recover(), recover()), 5)) == [0, 1]
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.fetch_attempt == 2 and row.status is UrlStatus.QUEUED
        events = list(await session.scalars(select(OutboxEvent).where(OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id))))
        assert len(events) == 1
