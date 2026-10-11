"""Frontier application services for crawl creation and URL admission."""

from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import UUID

from crawler_contracts import FetchUrlEvent
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crawler_frontier.models import Crawl, CrawlUrl, Link, OutboxEvent
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.state_machine import UrlStatus
from crawler_frontier.url_policy import (
    UrlValidationError,
    normalize_allowed_host,
    normalize_url,
)


class CrawlNotFoundError(LookupError):
    """Raised when an admission request refers to an unknown crawl."""


class SourceUrlNotFoundError(LookupError):
    """Raised when an admission request refers to a URL outside its crawl."""


@dataclass(frozen=True, slots=True)
class CreatedCrawl:
    """The durable identity returned after a crawl and its seed URLs are created."""

    crawl_id: UUID


@dataclass(frozen=True, slots=True)
class CrawlStatus:
    """The current derived scheduling state and URL counts for a crawl."""

    crawl_id: UUID
    url_counts: dict[UrlStatus, int]

    @property
    def state(self) -> str:
        """Return running while Frontier still has active crawl work."""
        active_statuses = (UrlStatus.QUEUED, UrlStatus.FETCHING, UrlStatus.DOWNLOADED)
        return "running" if any(self.url_counts[status] for status in active_statuses) else "completed"


@dataclass(frozen=True, slots=True)
class UrlAdmission:
    """The outcome of admitting one normalized URL into a crawl graph."""

    url_id: UUID | None
    normalized_url: str
    status: UrlStatus
    created: bool


class CrawlService:
    """Create crawls and atomically admit URLs and graph edges into Frontier."""

    def __init__(self, session: AsyncSession, policy: FrontierPolicy) -> None:
        self._session = session
        self._policy = policy

    async def create_crawl(
            self,
            seed_urls: Iterable[str],
            *,
            additional_allowed_hosts: Iterable[str] = (),
    ) -> CreatedCrawl:
        """Create a crawl and queue each distinct normalized seed URL."""
        normalized_seed_urls = _normalized_unique_urls(seed_urls)
        if not normalized_seed_urls:
            raise UrlValidationError("A crawl requires at least one seed URL")

        allowed_hosts = _allowed_hosts(normalized_seed_urls, additional_allowed_hosts)
        allowed_host_set = frozenset(allowed_hosts)

        async with self._session.begin():
            crawl = Crawl(seed_urls=normalized_seed_urls, allowed_hosts=allowed_hosts)
            self._session.add(crawl)
            await self._session.flush()

            for seed_url in normalized_seed_urls:
                await self._admit_normalized_url(
                    crawl_id=crawl.id,
                    allowed_hosts=allowed_host_set,
                    normalized_url=seed_url,
                    depth=0,
                    source_url_id=None,
                )

        return CreatedCrawl(crawl_id=crawl.id)

    async def admit_url(
            self,
            *,
            crawl_id: UUID,
            source_url_id: UUID,
            url: str,
            depth: int,
    ) -> UrlAdmission:
        """Store a discovered URL and its edge, deduplicating within one crawl."""
        if depth < 0:
            raise ValueError("depth must be non-negative")
        normalized_url = normalize_url(url)

        async with self._session.begin():
            crawl = await self._session.scalar(
                select(Crawl).where(Crawl.id == crawl_id).with_for_update()
            )
            if crawl is None:
                raise CrawlNotFoundError(f"Crawl {crawl_id} does not exist")

            source_url = await self._session.get(CrawlUrl, source_url_id)
            if source_url is None or source_url.crawl_id != crawl_id:
                raise SourceUrlNotFoundError(
                    f"Source URL {source_url_id} does not belong to crawl {crawl_id}"
                )

            return await self._admit_normalized_url(
                crawl_id=crawl_id,
                allowed_hosts=frozenset(crawl.allowed_hosts),
                normalized_url=normalized_url,
                depth=depth,
                source_url_id=source_url_id,
            )

    async def get_crawl_status(self, *, crawl_id: UUID) -> CrawlStatus:
        """Return a crawl's derived state and a stable count for every URL status."""
        crawl = await self._session.get(Crawl, crawl_id)
        if crawl is None:
            raise CrawlNotFoundError(f"Crawl {crawl_id} does not exist")

        rows = await self._session.execute(
            select(CrawlUrl.status, func.count())
            .where(CrawlUrl.crawl_id == crawl_id)
            .group_by(CrawlUrl.status)
        )
        url_counts = {status: 0 for status in UrlStatus}
        url_counts.update({status: count for status, count in rows})
        return CrawlStatus(crawl_id=crawl.id, url_counts=url_counts)

    async def _admit_normalized_url(
            self,
            *,
            crawl_id: UUID,
            allowed_hosts: frozenset[str],
            normalized_url: str,
            depth: int,
            source_url_id: UUID | None,
    ) -> UrlAdmission:
        existing = await self._session.scalar(
            select(CrawlUrl).where(
                CrawlUrl.crawl_id == crawl_id,
                CrawlUrl.normalized_url == normalized_url,
            )
        )
        if existing is not None:
            if source_url_id is not None:
                await self._store_link(crawl_id, source_url_id, existing.id)
            return UrlAdmission(
                url_id=existing.id,
                normalized_url=existing.normalized_url,
                status=existing.status,
                created=False,
            )

        origin = _origin(normalized_url)
        if (
            _hostname(normalized_url) not in allowed_hosts
            or depth > self._policy.max_crawl_depth
            or await self._admitted_url_count(crawl_id) >= self._policy.max_urls_per_crawl
            or await self._admitted_origin_url_count(crawl_id, origin) >= self._policy.max_urls_per_origin
        ):
            return UrlAdmission(
                url_id=None,
                normalized_url=normalized_url,
                status=UrlStatus.SKIPPED,
                created=False,
            )

        statement = (
            insert(CrawlUrl)
            .values(
                crawl_id=crawl_id,
                normalized_url=normalized_url,
                origin=origin,
                depth=depth,
                status=UrlStatus.QUEUED,
                fetch_attempt=1,
            )
            .on_conflict_do_nothing(index_elements=["crawl_id", "normalized_url"])
            .returning(CrawlUrl.id, CrawlUrl.status)
        )
        inserted = (await self._session.execute(statement)).one_or_none()

        if inserted is None:
            existing = await self._session.scalar(
                select(CrawlUrl).where(
                    CrawlUrl.crawl_id == crawl_id,
                    CrawlUrl.normalized_url == normalized_url,
                )
            )
            if existing is None:
                raise RuntimeError("A conflicting crawl URL disappeared during admission")
            url_id, stored_status, created = existing.id, existing.status, False
        else:
            url_id, stored_status, created = inserted.id, inserted.status, True

        if created and stored_status is UrlStatus.QUEUED:
            self._session.add(
                _fetch_url_outbox_event(
                    crawl_id=crawl_id,
                    url_id=url_id,
                    fetch_attempt=1,
                    url=normalized_url,
                    depth=depth,
                )
            )

        if source_url_id is not None:
            await self._store_link(crawl_id, source_url_id, url_id)

        return UrlAdmission(
            url_id=url_id,
            normalized_url=normalized_url,
            status=stored_status,
            created=created,
        )

    async def _admitted_url_count(self, crawl_id: UUID) -> int:
        return await self._session.scalar(
            select(func.count())
            .select_from(CrawlUrl)
            .where(CrawlUrl.crawl_id == crawl_id, CrawlUrl.status != UrlStatus.SKIPPED)
        ) or 0

    async def _admitted_origin_url_count(self, crawl_id: UUID, origin: str) -> int:
        return await self._session.scalar(
            select(func.count())
            .select_from(CrawlUrl)
            .where(
                CrawlUrl.crawl_id == crawl_id,
                CrawlUrl.origin == origin,
                CrawlUrl.status != UrlStatus.SKIPPED,
            )
        ) or 0

    async def _store_link(self, crawl_id: UUID, source_url_id: UUID, target_url_id: UUID) -> None:
        await self._session.execute(
            insert(Link)
            .values(
                crawl_id=crawl_id,
                source_url_id=source_url_id,
                target_url_id=target_url_id,
            )
            .on_conflict_do_nothing(index_elements=["source_url_id", "target_url_id"])
        )


def _normalized_unique_urls(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(normalize_url(value) for value in values))


def _allowed_hosts(seed_urls: Iterable[str], additional_hosts: Iterable[str]) -> list[str]:
    seed_hosts = (_hostname(seed_url) for seed_url in seed_urls)
    normalized_hosts = (*seed_hosts, *(normalize_allowed_host(host) for host in additional_hosts))
    return list(dict.fromkeys(normalized_hosts))


def _hostname(normalized_url: str) -> str:
    hostname = urlsplit(normalized_url).hostname
    if hostname is None:
        raise UrlValidationError("URL must include a hostname")
    return hostname.lower()


def _origin(normalized_url: str) -> str:
    parts = urlsplit(normalized_url)
    if not parts.scheme or not parts.netloc:
        raise UrlValidationError("URL must include a scheme and hostname")
    return f"{parts.scheme}://{parts.netloc}"


def _fetch_url_outbox_event(
        *,
        crawl_id: UUID,
        url_id: UUID,
        fetch_attempt: int,
        url: str,
        depth: int,
        exchange_name: str = "crawler.topic",
        routing_key: str = "fetch.url",
) -> OutboxEvent:
    """Build the durable event for one newly admitted queued URL."""
    event = FetchUrlEvent(
        crawl_id=crawl_id,
        url_id=url_id,
        fetch_attempt=fetch_attempt,
        url=url,
        depth=depth,
    )
    return OutboxEvent(
        event_id=event.event_id,
        exchange_name=exchange_name,
        routing_key=routing_key,
        payload=event.model_dump(mode="json"),
    )
