"""Frontier-owned lease ownership for Fetcher attempts."""
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from crawler_contracts import (
    FetchRetryRequestedEvent,
    PageFailedEvent,
    PageFetchedEvent,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from crawler_frontier.application.event_handlers import FrontierEventHandler
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus, lease_expires_at
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent


class LeaseLostError(ValueError):
    """The caller no longer owns the Frontier fetch lease."""


@dataclass(frozen=True, slots=True)
class LeaseGrant:
    owner_token: UUID
    lease_until: datetime


class FetchLeaseService:
    """Atomically claim, renew, and complete a Fetcher-owned attempt."""

    def __init__(self, session: AsyncSession, policy: FrontierPolicy) -> None:
        self.session, self.policy = session, policy

    async def claim(self, *, crawl_id: UUID, url_id: UUID, fetch_attempt: int,
                    owner_token: UUID | None = None) -> LeaseGrant:
        async with self.session.begin():
            url, now = await self._locked_url(crawl_id, url_id)
            if url.fetch_attempt != fetch_attempt:
                raise LeaseLostError("fetch attempt is no longer current")
            if url.status is UrlStatus.QUEUED:
                token = owner_token or uuid4()
                url.status = UrlStatus.FETCHING
                url.lease_owner_token = token
                url.lease_until = lease_expires_at(now=now, lease_seconds=self.policy.fetch_lease_seconds)
                return LeaseGrant(token, url.lease_until)
            if (
                    owner_token is not None
                    and url.status is UrlStatus.FETCHING
                    and url.lease_owner_token == owner_token
                    and url.lease_until is not None
                    and now < url.lease_until
            ):
                return LeaseGrant(owner_token, url.lease_until)
            raise LeaseLostError("fetch attempt is already owned or complete")

    async def renew(
            self,
            *,
            crawl_id: UUID,
            url_id: UUID,
            fetch_attempt: int,
            owner_token: UUID
    ) -> LeaseGrant:
        async with self.session.begin():
            url, now = await self._locked_url(crawl_id, url_id)
            self._require_owner(url, fetch_attempt, owner_token, now)
            url.lease_until = lease_expires_at(now=now, lease_seconds=self.policy.fetch_lease_seconds)
            return LeaseGrant(owner_token, url.lease_until)

    async def submit(self, event: PageFetchedEvent | PageFailedEvent | FetchRetryRequestedEvent, *,
                     owner_token: UUID) -> None:
        async with self.session.begin():
            url, now = await self._locked_url(event.crawl_id, event.url_id)
            # A repeated request after a lost HTTP response is successful only when
            # its exact event was committed by this owner.
            if url.lease_submission_event_id == event.event_id and url.lease_owner_token == owner_token:
                return
            self._require_owner(url, event.fetch_attempt, owner_token, now)
            await FrontierEventHandler(self.session, self.policy).apply_current_attempt_event(url, event)
            # _apply enqueues retry work, but the submitted result itself must be
            # published from Frontier's outbox in the same transaction.
            self.session.add(
                OutboxEvent(
                    event_id=event.event_id,
                    exchange_name="crawler.topic",
                    routing_key=_outbox_routing_key(event),
                    payload=event.model_dump(mode="json")
                )
            )
            url.lease_owner_token = owner_token
            url.lease_submission_event_id = event.event_id

    async def _locked_url(self, crawl_id: UUID, url_id: UUID) -> tuple[CrawlUrl, datetime]:
        crawl = await self.session.scalar(select(Crawl).where(Crawl.id == crawl_id).with_for_update())
        if crawl is None:
            raise LeaseLostError("unknown crawl")
        url = await self.session.scalar(
            select(CrawlUrl).where(
                CrawlUrl.id == url_id,
                CrawlUrl.crawl_id == crawl_id
            ).with_for_update())
        if url is None:
            raise LeaseLostError("unknown crawl URL")
        # PostgreSQL transaction_timestamp is fixed at begin; evaluate the clock
        # only after the row lock has been acquired.
        now = await self.session.scalar(select(func.clock_timestamp()))
        return url, now

    @staticmethod
    def _require_owner(url: CrawlUrl, attempt: int, token: UUID, now: datetime) -> None:
        if (url.status is not UrlStatus.FETCHING or url.fetch_attempt != attempt
                or url.lease_owner_token != token or url.lease_until is None
                or now >= url.lease_until):
            raise LeaseLostError("fetch lease is no longer owned")


def _outbox_routing_key(event: PageFetchedEvent | PageFailedEvent | FetchRetryRequestedEvent) -> str:
    match event:
        case PageFetchedEvent():
            return "page.fetched"
        case PageFailedEvent():
            return "page.failed"
        case FetchRetryRequestedEvent():
            return "fetch.retry_requested"
        case _:
            raise ValueError(f"Unsupported event type: {type(event).__name__}")
