"""PostgreSQL regressions for Fetcher lease ownership."""
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from crawler_contracts import (
    FetchRetryCategory,
    FetchRetryRequestedEvent,
    PageFetchedEvent,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.application.crawl_service import CrawlService
from crawler_frontier.application.fetch_lease_service import (
    FetchLeaseService,
    LeaseLostError,
)
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.models import CrawlUrl, OutboxEvent
from crawler_frontier.workers.lease_recovery import LeaseRecovery


async def _queued(factory):
    async with factory() as session:
        created = await CrawlService(session, FrontierPolicy(fetch_lease_seconds=30)).create_crawl(["https://example.test/"])
    async with factory() as session:
        url = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        return created.crawl_id, url.id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_claim_with_same_owner_token_is_idempotent_after_lost_response(session_factory: async_sessionmaker[AsyncSession]):
    crawl_id, url_id = await _queued(session_factory)
    policy, owner_token = FrontierPolicy(), uuid4()
    async with session_factory() as session:
        first = await FetchLeaseService(session, policy).claim(
            crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, owner_token=owner_token,
        )
    async with session_factory() as session:
        repeated = await FetchLeaseService(session, policy).claim(
            crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, owner_token=owner_token,
        )
    assert repeated == first
    async with session_factory() as session:
        with pytest.raises(LeaseLostError):
            await FetchLeaseService(session, policy).claim(
                crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, owner_token=uuid4(),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recovered_owner_cannot_renew_or_submit_and_new_owner_can_submit(session_factory: async_sessionmaker[AsyncSession]):
    crawl_id, url_id = await _queued(session_factory)
    policy = FrontierPolicy(fetch_lease_seconds=30)
    async with session_factory() as session:
        old = await FetchLeaseService(session, policy).claim(crawl_id=crawl_id, url_id=url_id, fetch_attempt=1)
    async with session_factory() as session, session.begin():
        url = await session.scalar(select(CrawlUrl).where(CrawlUrl.id == url_id).with_for_update())
        url.lease_until = datetime.now(UTC) - timedelta(seconds=1)
    assert await LeaseRecovery(session_factory, policy).recover_expired_once() == 1
    async with session_factory() as session:
        with pytest.raises(LeaseLostError):
            await FetchLeaseService(session, policy).renew(crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, owner_token=old.owner_token)
    async with session_factory() as session:
        new = await FetchLeaseService(session, policy).claim(crawl_id=crawl_id, url_id=url_id, fetch_attempt=2)
        event = PageFetchedEvent(crawl_id=crawl_id, url_id=url_id, fetch_attempt=2, final_url="https://example.test/", http_status=200, content_type="text/html", content_ref="s3://bucket/object")
        await FetchLeaseService(session, policy).submit(event, owner_token=new.owner_token)
    async with session_factory() as session:
        outbox = await session.scalar(select(OutboxEvent).where(OutboxEvent.event_id == event.event_id))
        assert outbox is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_submit_after_lost_response_is_idempotent(session_factory: async_sessionmaker[AsyncSession]):
    crawl_id, url_id = await _queued(session_factory)
    policy = FrontierPolicy()
    async with session_factory() as session:
        grant = await FetchLeaseService(session, policy).claim(crawl_id=crawl_id, url_id=url_id, fetch_attempt=1)
        event = PageFetchedEvent(event_id=uuid4(), crawl_id=crawl_id, url_id=url_id, fetch_attempt=1, final_url="https://example.test/", http_status=200, content_type="text/html", content_ref="s3://bucket/object")
        await FetchLeaseService(session, policy).submit(event, owner_token=grant.owner_token)
    async with session_factory() as session:
        await FetchLeaseService(session, policy).submit(event, owner_token=grant.owner_token)
    async with session_factory() as session:
        assert len(list(await session.scalars(select(OutboxEvent).where(OutboxEvent.event_id == event.event_id)))) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_submit_remains_idempotent_after_allocating_next_attempt(session_factory: async_sessionmaker[AsyncSession]):
    crawl_id, url_id = await _queued(session_factory)
    policy = FrontierPolicy()
    async with session_factory() as session:
        grant = await FetchLeaseService(session, policy).claim(
            crawl_id=crawl_id, url_id=url_id, fetch_attempt=1,
        )
        event = FetchRetryRequestedEvent(
            crawl_id=crawl_id,
            url_id=url_id,
            fetch_attempt=1,
            category=FetchRetryCategory.TIMEOUT,
            detail="timeout",
            suggested_delay_seconds=5,
        )
        await FetchLeaseService(session, policy).submit(event, owner_token=grant.owner_token)
    async with session_factory() as session:
        await FetchLeaseService(session, policy).submit(event, owner_token=grant.owner_token)
    async with session_factory() as session:
        url = await session.get(CrawlUrl, url_id)
        assert (url.fetch_attempt, url.status.value) == (2, "queued")
        assert len(list(await session.scalars(select(OutboxEvent).where(OutboxEvent.event_id == event.event_id)))) == 1
