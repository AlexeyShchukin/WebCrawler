from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from crawler_contracts import FetchUrlEvent
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.database import create_engine, create_session_factory
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.models import Crawl, CrawlUrl, Link, OutboxEvent
from crawler_frontier.services import CrawlService
from crawler_frontier.settings import FrontierSettings
from crawler_frontier.state_machine import UrlStatus


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(
        FrontierSettings(
            database_url=database_url_from_environment(),
            rabbitmq_url="amqp://crawler:secret@localhost/",
        )
    )
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))

    yield create_session_factory(engine)
    await engine.dispose()


async def _remove_crawl(
    session_factory: async_sessionmaker[AsyncSession],
    crawl_id: UUID,
) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(delete(Crawl).where(Crawl.id == crawl_id))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_crawl_normalizes_and_deduplicates_seed_urls(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        created = await CrawlService(session).create_crawl(
            [
                "HTTPS://Example.test:443#first",
                "https://example.test/",
                "https://docs.example.test/guide",
            ]
        )

        try:
            crawl = await session.get(Crawl, created.crawl_id)
            urls = list(
                await session.scalars(
                    select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id).order_by(CrawlUrl.normalized_url)
                )
            )

            assert crawl is not None
            assert crawl.seed_urls == ["https://example.test/", "https://docs.example.test/guide"]
            assert crawl.allowed_hosts == ["example.test", "docs.example.test"]
            assert [(url.normalized_url, url.depth, url.status) for url in urls] == [
                ("https://docs.example.test/guide", 0, UrlStatus.QUEUED),
                ("https://example.test/", 0, UrlStatus.QUEUED),
            ]
        finally:
            await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admission_deduplicates_urls_stores_edges_and_skips_out_of_scope_urls(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"crawl-{uuid4().hex}.example.test"

    async with session_factory() as create_session:
        created = await CrawlService(create_session).create_crawl([f"https://{host}/start"])

    try:
        async with session_factory() as read_session:
            seed = await read_session.scalar(
                select(CrawlUrl).where(
                    CrawlUrl.crawl_id == created.crawl_id,
                    CrawlUrl.normalized_url == f"https://{host}/start",
                )
            )
            assert seed is not None

        async with session_factory() as write_session:
            service = CrawlService(write_session)
            first = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed.id,
                url=f"https://{host}/about#team",
                depth=1,
            )
            repeated = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed.id,
                url=f"https://{host}/about",
                depth=1,
            )
            outside_scope = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed.id,
                url="https://outside.example.test/page",
                depth=1,
            )

            urls = list(
                await write_session.scalars(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
            )
            edges = list(
                await write_session.scalars(select(Link).where(Link.crawl_id == created.crawl_id))
            )

        assert first.created is True
        assert repeated.created is False
        assert outside_scope.status is UrlStatus.SKIPPED
        assert len(urls) == 3
        assert len(edges) == 2
    finally:
        await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admission_stores_a_cycle_once_without_duplicate_nodes_or_edges(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"cycle-{uuid4().hex}.example.test"
    first_url = f"https://{host}/first"
    second_url = f"https://{host}/second"

    async with session_factory() as create_session:
        created = await CrawlService(create_session).create_crawl([first_url])

    try:
        async with session_factory() as read_session:
            first = await read_session.scalar(
                select(CrawlUrl).where(
                    CrawlUrl.crawl_id == created.crawl_id,
                    CrawlUrl.normalized_url == first_url,
                )
            )
            assert first is not None

        async with session_factory() as write_session:
            service = CrawlService(write_session)
            second = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=first.id,
                url=second_url,
                depth=1,
            )
            back_to_first = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=second.url_id,
                url=first_url,
                depth=2,
            )
            repeated_edge = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=first.id,
                url=second_url,
                depth=1,
            )

            urls = list(
                await write_session.scalars(
                    select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)
                )
            )
            links = list(
                await write_session.scalars(select(Link).where(Link.crawl_id == created.crawl_id))
            )

        assert second.created is True
        assert back_to_first.created is False
        assert repeated_edge.created is False
        assert {url.normalized_url for url in urls} == {first_url, second_url}
        assert {(link.source_url_id, link.target_url_id) for link in links} == {
            (first.id, second.url_id),
            (second.url_id, first.id),
        }
    finally:
        await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admission_writes_one_fetch_url_outbox_event_per_new_queued_url(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"outbox-{uuid4().hex}.example.test"
    seed_url = f"https://{host}/start"
    queued_url = f"https://{host}/about"

    async with session_factory() as create_session:
        created = await CrawlService(create_session).create_crawl([seed_url])

    try:
        async with session_factory() as read_session:
            seed = await read_session.scalar(
                select(CrawlUrl).where(
                    CrawlUrl.crawl_id == created.crawl_id,
                    CrawlUrl.normalized_url == seed_url,
                )
            )
            assert seed is not None

        async with session_factory() as write_session:
            service = CrawlService(write_session)
            queued = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed.id,
                url=queued_url,
                depth=1,
            )
            repeated = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed.id,
                url=queued_url,
                depth=1,
            )
            skipped = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed.id,
                url="https://outside.example.test/page",
                depth=1,
            )

            outbox_events = [
                outbox_event
                for outbox_event in await write_session.scalars(select(OutboxEvent))
                if outbox_event.payload["crawl_id"] == str(created.crawl_id)
            ]

        assert queued.created is True
        assert repeated.created is False
        assert skipped.status is UrlStatus.SKIPPED
        assert len(outbox_events) == 2
        assert {event.routing_key for event in outbox_events} == {"fetch.url"}

        fetch_events = [FetchUrlEvent.model_validate(event.payload) for event in outbox_events]
        assert {event.event_id for event in fetch_events} == {event.event_id for event in outbox_events}
        assert {(str(event.url), event.url_id, event.depth, event.fetch_attempt) for event in fetch_events} == {
            (seed_url, seed.id, 0, 1),
            (queued_url, queued.url_id, 1, 1),
        }
    finally:
        await _remove_crawl(session_factory, created.crawl_id)
