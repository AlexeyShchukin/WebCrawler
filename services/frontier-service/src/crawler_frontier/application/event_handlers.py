"""Idempotent Frontier handlers for crawler pipeline events."""

from datetime import UTC, datetime

from crawler_contracts import (
    FailureStage,
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    LinksExtractedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crawler_frontier.application.crawl_service import (
    CrawlService,
    _fetch_url_outbox_event,
)
from crawler_frontier.domain.policy import FrontierPolicy, can_retry
from crawler_frontier.domain.state_machine import UrlStatus, lease_expires_at
from crawler_frontier.models import Crawl, CrawlUrl, ProcessedEvent


class EventIdentityError(ValueError):
    """The event refers to an unknown URL or an unallocated attempt."""


class FrontierEventHandler:
    def __init__(self, session: AsyncSession, policy: FrontierPolicy) -> None:
        self.session, self.policy = session, policy

    async def handle_fetch_started(self, event: FetchStartedEvent, consumer_name: str) -> None:
        await self._handle(event, consumer_name)

    async def handle_page_fetched(self, event: PageFetchedEvent, consumer_name: str) -> None:
        await self._handle(event, consumer_name)

    async def handle_page_processed(self, event: PageProcessedEvent, consumer_name: str) -> None:
        await self._handle(event, consumer_name)

    async def handle_page_failed(self, event: PageFailedEvent, consumer_name: str) -> None:
        await self._handle(event, consumer_name)

    async def handle_fetch_retry_requested(self, event: FetchRetryRequestedEvent, consumer_name: str) -> None:
        await self._handle(event, consumer_name)

    async def handle_links_extracted(self, event: LinksExtractedEvent, consumer_name: str) -> None:
        await self._handle(event, consumer_name)

    async def _handle(self, event, consumer_name: str) -> None:
        async with self.session.begin():
            url = await self._claim(event, consumer_name)
            if url is not None:
                await self._apply(url, event)

    async def _apply(self, url: CrawlUrl, event) -> None:
        if url.fetch_attempt != event.fetch_attempt:
            return
        if isinstance(event, LinksExtractedEvent):
            if url.status is not UrlStatus.SKIPPED:
                if url.status in {UrlStatus.QUEUED, UrlStatus.FETCHING}:
                    url.status, url.lease_until = UrlStatus.DOWNLOADED, None
                await CrawlService(self.session, self.policy)._admit_extracted_links(url, event.links)
        elif isinstance(event, FetchStartedEvent):
            if url.status is UrlStatus.QUEUED:
                url.status = UrlStatus.FETCHING
                url.lease_until = lease_expires_at(
                    now=datetime.now(UTC),
                    lease_seconds=self.policy.fetch_lease_seconds,
                )

        elif isinstance(event, PageFetchedEvent):
            if url.status in {UrlStatus.QUEUED, UrlStatus.FETCHING}:
                url.status, url.lease_until = UrlStatus.DOWNLOADED, None

        elif isinstance(event, PageProcessedEvent):
            if url.status in {UrlStatus.QUEUED, UrlStatus.FETCHING, UrlStatus.DOWNLOADED}:
                url.status, url.lease_until = UrlStatus.FETCHED, None

        elif isinstance(event, PageFailedEvent):
            accepted_statuses = (
                {UrlStatus.QUEUED, UrlStatus.FETCHING}
                if event.stage is FailureStage.FETCH
                else {UrlStatus.QUEUED, UrlStatus.FETCHING, UrlStatus.DOWNLOADED}
            )
            if url.status in accepted_statuses:
                url.status, url.lease_until = UrlStatus.FAILED, None
                url.last_error_category, url.last_error_detail = event.category.value, event.detail

        elif isinstance(event, FetchRetryRequestedEvent):
            if url.status not in {UrlStatus.QUEUED, UrlStatus.FETCHING}:
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
        # Admission and recovery use the same order: Crawl -> CrawlUrl -> ledger.
        crawl = await self.session.scalar(
            select(Crawl).where(Crawl.id == event.crawl_id).with_for_update()
        )
        if crawl is None:
            raise EventIdentityError("Unknown event crawl")
        url = await self.session.scalar(
            select(CrawlUrl).where(
                CrawlUrl.id == event.url_id, CrawlUrl.crawl_id == event.crawl_id,
            ).with_for_update()
        )
        if url is None:
            raise EventIdentityError("Event URL does not belong to crawl")
        if await self.session.scalar(select(ProcessedEvent.id).where(
            ProcessedEvent.consumer_name == consumer_name, ProcessedEvent.event_id == event.event_id,
        )) is not None:
            return None
        if event.fetch_attempt > url.fetch_attempt:
            raise EventIdentityError("Event attempt has not been allocated by Frontier")
        if not await self._mark_processed(event.event_id, consumer_name):
            return None
        return url if event.fetch_attempt == url.fetch_attempt else None

    async def _mark_processed(self, event_id, consumer_name: str) -> bool:
        statement = (
            insert(ProcessedEvent)
            .values(consumer_name=consumer_name, event_id=event_id)
            .on_conflict_do_nothing(index_elements=["consumer_name", "event_id"])
            .returning(ProcessedEvent.id)
        )
        return await self.session.scalar(statement) is not None

def _retry_routing_key(seconds: int) -> str:
    if seconds <= 5: return "fetch.url.retry.5s"
    if seconds <= 30: return "fetch.url.retry.30s"
    if seconds <= 120: return "fetch.url.retry.2m"
    return "fetch.url.retry.10m"
