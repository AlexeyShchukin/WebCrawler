"""Recovery of fetch attempts abandoned after their Frontier lease expires."""

from asyncio import Event, wait_for
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.models import CrawlUrl
from crawler_frontier.policy import FrontierPolicy, can_retry
from crawler_frontier.services import _fetch_url_outbox_event
from crawler_frontier.state_machine import UrlStatus


class LeaseRecovery:
    """Return expired fetch leases to Frontier-owned retry scheduling."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        policy: FrontierPolicy,
    ) -> None:
        self._session_factory = session_factory
        self._policy = policy

    async def recover_expired_once(self, *, now: datetime | None = None) -> int:
        """Recover every lease expired at *now*, returning the number claimed."""
        recovery_time = now or datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            expired_urls = list(
                await session.scalars(
                    select(CrawlUrl)
                    .where(
                        CrawlUrl.status == UrlStatus.FETCHING,
                        CrawlUrl.lease_until.is_not(None),
                        CrawlUrl.lease_until <= recovery_time,
                    )
                    .order_by(CrawlUrl.lease_until, CrawlUrl.id)
                    .with_for_update(skip_locked=True)
                )
            )
            for url in expired_urls:
                url.lease_until = None
                if not can_retry(fetch_attempt=url.fetch_attempt, policy=self._policy):
                    url.status = UrlStatus.FAILED
                    url.last_error_category = "lease_expired"
                    url.last_error_detail = "Fetch lease expired before a terminal event"
                    continue

                url.fetch_attempt += 1
                url.status = UrlStatus.QUEUED
                session.add(
                    _fetch_url_outbox_event(
                        crawl_id=url.crawl_id,
                        url_id=url.id,
                        fetch_attempt=url.fetch_attempt,
                        url=url.normalized_url,
                        depth=url.depth,
                        exchange_name="crawler.retry",
                        routing_key="fetch.url.retry.30s",
                    )
                )
            return len(expired_urls)

    async def run(self, stop_event: Event, poll_interval_seconds: float) -> None:
        """Recover expired leases until service shutdown."""
        while not stop_event.is_set():
            if await self.recover_expired_once():
                continue
            try:
                await wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except TimeoutError:
                continue
