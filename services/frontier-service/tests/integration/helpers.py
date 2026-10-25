from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent


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
            origin="https://example.test",
            depth=0,
            status=status,
            fetch_attempt=fetch_attempt,
            lease_until=(lease_until or datetime.now(UTC) + timedelta(minutes=1))
            if status is UrlStatus.FETCHING else lease_until,
            lease_owner_token=uuid4() if status is UrlStatus.FETCHING else None,
        )
        session.add(url)
        await session.flush()
        return crawl.id, url.id


async def _create_pending_event(
    session_factory: async_sessionmaker[AsyncSession], *,
    exchange_name: str = "crawler.topic", routing_key: str = "fetch.url",
) -> OutboxEvent:
    event = OutboxEvent(
        event_id=uuid4(),
        exchange_name=exchange_name,
        routing_key=routing_key,
        payload={"event_id": str(uuid4())},
        created_at=datetime(1970, 1, 1, tzinfo=UTC),
    )
    async with session_factory() as session, session.begin():
        session.add(event)
    return event
