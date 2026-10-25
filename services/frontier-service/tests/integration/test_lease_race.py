"""PostgreSQL races between fenced lease renewal, recovery, and stale work."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from crawler_contracts import FailureCategory, FailureStage, PageFailedEvent
from sqlalchemy import select

from crawler_frontier.application.fetch_lease_service import (
    FetchLeaseService,
    LeaseLostError,
)
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.models import CrawlUrl, OutboxEvent
from crawler_frontier.workers.lease_recovery import LeaseRecovery
from tests.integration.test_fetch_lease_ownership import _queued


@pytest.mark.integration
@pytest.mark.asyncio
async def test_renewal_lock_serializes_recovery(session_factory) -> None:
    """Recovery cannot take an attempt while its owner is renewing it."""
    crawl_id, url_id = await _queued(session_factory)
    policy = FrontierPolicy(fetch_lease_seconds=30)
    async with session_factory() as session:
        grant = await FetchLeaseService(session, policy).claim(
            crawl_id=crawl_id, url_id=url_id, fetch_attempt=1
        )

    claimed, release = asyncio.Event(), asyncio.Event()

    class PausingLeaseService(FetchLeaseService):
        async def _locked_url(self, crawl_id, url_id):
            url, now = await super()._locked_url(crawl_id, url_id)
            claimed.set()
            await release.wait()
            return url, now

    async def renew() -> None:
        async with session_factory() as session:
            await PausingLeaseService(session, policy).renew(
                crawl_id=crawl_id,
                url_id=url_id,
                fetch_attempt=1,
                owner_token=grant.owner_token,
            )

    task = asyncio.create_task(renew())
    try:
        await asyncio.wait_for(claimed.wait(), 5)
        assert await LeaseRecovery(session_factory, policy).recover_expired_once(
            now=datetime.now(UTC) + timedelta(hours=1)
        ) == 0
        release.set()
        await asyncio.wait_for(task, 5)
        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            assert url.fetch_attempt == 1
            assert url.lease_owner_token == grant.owner_token
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recovery_revokes_old_owner_and_blocks_its_result(session_factory) -> None:
    """A worker noticing loss must cancel instead of submitting its old result."""
    crawl_id, url_id = await _queued(session_factory)
    policy = FrontierPolicy(fetch_lease_seconds=30)
    async with session_factory() as session:
        old = await FetchLeaseService(session, policy).claim(
            crawl_id=crawl_id, url_id=url_id, fetch_attempt=1
        )
    async with session_factory() as session, session.begin():
        url = await session.scalar(select(CrawlUrl).where(CrawlUrl.id == url_id).with_for_update())
        url.lease_until = datetime.now(UTC) - timedelta(seconds=1)

    operation_started, lease_lost, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def old_worker_operation() -> None:
        operation_started.set()
        await lease_lost.wait()
        try:
            async with session_factory() as session:
                await FetchLeaseService(session, policy).renew(
                    crawl_id=crawl_id,
                    url_id=url_id,
                    fetch_attempt=1,
                    owner_token=old.owner_token,
                )
        except LeaseLostError:
            cancelled.set()

    task = asyncio.create_task(old_worker_operation())
    try:
        await asyncio.wait_for(operation_started.wait(), 5)
        assert await LeaseRecovery(session_factory, policy).recover_expired_once() == 1
        lease_lost.set()
        await asyncio.wait_for(cancelled.wait(), 5)

        stale = PageFailedEvent(
            crawl_id=crawl_id,
            url_id=url_id,
            fetch_attempt=1,
            stage=FailureStage.FETCH,
            category=FailureCategory.TIMEOUT,
            detail="old worker must not publish this",
        )
        async with session_factory() as session:
            with pytest.raises(LeaseLostError):
                await FetchLeaseService(session, policy).submit(stale, owner_token=old.owner_token)
        async with session_factory() as session:
            url = await session.get(CrawlUrl, url_id)
            stale_outbox = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.event_id == stale.event_id)
            )
            assert url.fetch_attempt == 2
            assert stale_outbox is None
    finally:
        lease_lost.set()
        await asyncio.gather(task, return_exceptions=True)
