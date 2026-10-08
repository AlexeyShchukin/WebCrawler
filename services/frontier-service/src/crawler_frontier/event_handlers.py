"""Idempotent Frontier handlers for crawler pipeline events."""

from datetime import UTC, datetime

from crawler_contracts import (
    FailureStage,
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crawler_frontier.models import CrawlUrl, ProcessedEvent
from crawler_frontier.policy import FrontierPolicy, can_retry
from crawler_frontier.services import _fetch_url_outbox_event
from crawler_frontier.state_machine import UrlStatus, lease_expires_at


class FrontierEventHandler:
    def __init__(self, session: AsyncSession, policy: FrontierPolicy) -> None:
        self.session, self.policy = session, policy

    async def handle_fetch_started(self, event: FetchStartedEvent, consumer_name: str) -> None:
        async with self.session.begin():
            url = await self._claim(event, consumer_name)
            if url and url.status is UrlStatus.QUEUED and url.fetch_attempt == event.fetch_attempt:
                url.status = UrlStatus.FETCHING
                url.lease_until = lease_expires_at(
                    now=datetime.now(UTC),
                    lease_seconds=self.policy.fetch_lease_seconds,
                )

    async def handle_page_fetched(self, event: PageFetchedEvent, consumer_name: str) -> None:
        async with self.session.begin():
            url = await self._claim(event, consumer_name)
            if url and url.status is UrlStatus.FETCHING and url.fetch_attempt == event.fetch_attempt:
                url.status, url.lease_until = UrlStatus.DOWNLOADED, None

    async def handle_page_processed(self, event: PageProcessedEvent, consumer_name: str) -> None:
        async with self.session.begin():
            url = await self._claim(event, consumer_name)
            if url and url.status in {UrlStatus.FETCHING, UrlStatus.DOWNLOADED} and url.fetch_attempt == event.fetch_attempt:
                url.status, url.lease_until = UrlStatus.FETCHED, None

    async def handle_page_failed(self, event: PageFailedEvent, consumer_name: str) -> None:
        async with self.session.begin():
            url = await self._claim(event, consumer_name)
            accepted_statuses = (
                {UrlStatus.FETCHING}
                if event.stage is FailureStage.FETCH
                else {UrlStatus.FETCHING, UrlStatus.DOWNLOADED}
            )
            if url and url.status in accepted_statuses and url.fetch_attempt == event.fetch_attempt:
                url.status, url.lease_until = UrlStatus.FAILED, None
                url.last_error_category, url.last_error_detail = event.category.value, event.detail

    async def handle_fetch_retry_requested(
        self,
        event: FetchRetryRequestedEvent,
        consumer_name: str,
    ) -> None:
        async with self.session.begin():
            url = await self._claim(event, consumer_name)
            if not url or url.status is not UrlStatus.FETCHING or url.fetch_attempt != event.fetch_attempt:
                return
            url.lease_until = None
            if not can_retry(fetch_attempt=url.fetch_attempt, policy=self.policy):
                url.status, url.last_error_category, url.last_error_detail = UrlStatus.FAILED, event.category.value, event.detail
                return
            url.fetch_attempt += 1
            url.status = UrlStatus.QUEUED
            self.session.add(_fetch_url_outbox_event(
                crawl_id=event.crawl_id,
                url_id=url.id,
                fetch_attempt=url.fetch_attempt,
                url=url.normalized_url,
                depth=url.depth,
                exchange_name="crawler.retry",
                routing_key=_retry_routing_key(event.suggested_delay_seconds)
            ))

    async def _claim(self, event, consumer_name: str) -> CrawlUrl | None:
        """Claim an event idempotently for this consumer and return its associated URL."""
        statement = (
            insert(ProcessedEvent)
            .values(consumer_name=consumer_name, event_id=event.event_id)
            .on_conflict_do_nothing(index_elements=["consumer_name", "event_id"])
            .returning(ProcessedEvent.id)
        )
        if await self.session.scalar(statement) is None:
            return None
        url = await self.session.get(CrawlUrl, event.url_id)
        return url if url and url.crawl_id == event.crawl_id else None


def _retry_routing_key(seconds: int) -> str:
    if seconds <= 5: return "fetch.url.retry.5s"
    if seconds <= 30: return "fetch.url.retry.30s"
    if seconds <= 120: return "fetch.url.retry.2m"
    return "fetch.url.retry.10m"
