import asyncio
from uuid import UUID, uuid4

import pytest
from crawler_contracts import FetchUrlEvent
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.application.crawl_service import CrawlService
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import Crawl, CrawlUrl, Link, OutboxEvent


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
        created = await CrawlService(session, FrontierPolicy()).create_crawl(
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
        created = await CrawlService(create_session, FrontierPolicy()).create_crawl([f"https://{host}/start"])

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
            service = CrawlService(write_session, FrontierPolicy())
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
            outbox_events = list(
                await write_session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(created.crawl_id)
                    )
                )
            )

        assert first.created is True
        assert repeated.created is False
        assert outside_scope.status is UrlStatus.SKIPPED
        assert outside_scope.url_id is None
        assert len(urls) == 2
        assert len(edges) == 1
        assert len(outbox_events) == 2
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
        created = await CrawlService(create_session, FrontierPolicy()).create_crawl([first_url])

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
            service = CrawlService(write_session, FrontierPolicy())
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
        created = await CrawlService(create_session, FrontierPolicy()).create_crawl([seed_url])

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
            service = CrawlService(write_session, FrontierPolicy())
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
        assert skipped.url_id is None
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admission_skips_urls_beyond_depth_without_spending_url_budget(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"depth-{uuid4().hex}.example.test"
    seed_url = f"https://{host}/seed"
    policy = FrontierPolicy(max_crawl_depth=0, max_urls_per_crawl=2, max_urls_per_origin=2)

    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl([seed_url])

    try:
        async with session_factory() as session:
            seed = await session.scalar(select(CrawlUrl).where(CrawlUrl.normalized_url == seed_url))
            assert seed is not None
            seed_id = seed.id

        async with session_factory() as session:
            admission = await CrawlService(session, policy).admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url=f"https://{host}/too-deep",
                depth=1,
            )
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(created.crawl_id)
                    )
                )
            )
            urls = list(
                await session.scalars(
                    select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)
                )
            )
            links = list(
                await session.scalars(select(Link).where(Link.crawl_id == created.crawl_id))
            )
            urls = list(
                await session.scalars(
                    select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)
                )
            )
            links = list(
                await session.scalars(select(Link).where(Link.crawl_id == created.crawl_id))
            )

        assert admission.status is UrlStatus.SKIPPED
        assert admission.url_id is None
        assert len(outbox_events) == 1
        assert len(urls) == 1
        assert links == []
    finally:
        await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_skipped_out_of_scope_url_does_not_spend_the_crawl_url_budget(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"budget-{uuid4().hex}.example.test"
    seed_url = f"https://{host}/seed"
    policy = FrontierPolicy(max_crawl_depth=5, max_urls_per_crawl=2, max_urls_per_origin=2)

    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl([seed_url])

    try:
        async with session_factory() as session:
            seed = await session.scalar(select(CrawlUrl).where(CrawlUrl.normalized_url == seed_url))
            assert seed is not None
            seed_id = seed.id

        async with session_factory() as session:
            service = CrawlService(session, policy)
            outside_scope = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url="https://outside.example.test/page",
                depth=1,
            )
            admitted = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url=f"https://{host}/within-budget",
                depth=1,
            )

        assert outside_scope.status is UrlStatus.SKIPPED
        assert admitted.status is UrlStatus.QUEUED
    finally:
        await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_origin_limit_counts_only_queued_urls_and_preserves_duplicate_admission(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"origin-{uuid4().hex}.example.test"
    seed_url = f"https://{host}/seed"
    policy = FrontierPolicy(max_crawl_depth=5, max_urls_per_crawl=10, max_urls_per_origin=1)

    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl([seed_url])

    try:
        async with session_factory() as session:
            seed = await session.scalar(select(CrawlUrl).where(CrawlUrl.normalized_url == seed_url))
            assert seed is not None
            seed_id = seed.id

        async with session_factory() as session:
            service = CrawlService(session, policy)
            duplicate = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url=seed_url,
                depth=1,
            )
            beyond_origin_limit = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url=f"https://{host}/beyond-origin-limit",
                depth=1,
            )
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(created.crawl_id)
                    )
                )
            )
            urls = list(
                await session.scalars(
                    select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)
                )
            )
            links = list(
                await session.scalars(select(Link).where(Link.crawl_id == created.crawl_id))
            )

        assert duplicate.created is False
        assert duplicate.status is UrlStatus.QUEUED
        assert beyond_origin_limit.status is UrlStatus.SKIPPED
        assert beyond_origin_limit.url_id is None
        assert len(outbox_events) == 1
        assert len(urls) == 1
        assert [(link.source_url_id, link.target_url_id) for link in links] == [(seed_id, seed_id)]
    finally:
        await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_admission_cannot_exceed_the_crawl_url_limit(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"concurrent-{uuid4().hex}.example.test"
    seed_url = f"https://{host}/seed"
    policy = FrontierPolicy(max_crawl_depth=5, max_urls_per_crawl=2, max_urls_per_origin=10)

    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl([seed_url])

    try:
        async with session_factory() as session:
            seed = await session.scalar(select(CrawlUrl).where(CrawlUrl.normalized_url == seed_url))
            assert seed is not None
            seed_id = seed.id

        async def admit(path: str):
            async with session_factory() as session:
                return await CrawlService(session, policy).admit_url(
                    crawl_id=created.crawl_id,
                    source_url_id=seed_id,
                    url=f"https://{host}/{path}",
                    depth=1,
                )

        first, second = await asyncio.gather(admit("first"), admit("second"))

        async with session_factory() as session:
            admitted_count = await session.scalar(
                select(func.count())
                .select_from(CrawlUrl)
                .where(
                    CrawlUrl.crawl_id == created.crawl_id,
                    CrawlUrl.status != UrlStatus.SKIPPED,
                )
            )
            urls = list(
                await session.scalars(
                    select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)
                )
            )
            links = list(
                await session.scalars(select(Link).where(Link.crawl_id == created.crawl_id))
            )
            outbox_events = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["crawl_id"].as_string() == str(created.crawl_id)
                    )
                )
            )

        assert {first.status, second.status} == {UrlStatus.QUEUED, UrlStatus.SKIPPED}
        assert admitted_count == 2
        assert len(urls) == 2
        assert len(links) == 1
        assert len(outbox_events) == 2
    finally:
        await _remove_crawl(session_factory, created.crawl_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_origin_limit_uses_normalized_scheme_host_and_port(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    host = f"origin-normalized-{uuid4().hex}.example.test"
    seed_url = f"HTTPS://{host.upper()}:443/seed"
    policy = FrontierPolicy(max_crawl_depth=5, max_urls_per_crawl=10, max_urls_per_origin=1)

    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl([seed_url])

    try:
        async with session_factory() as session:
            seed = await session.scalar(
                select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)
            )
            assert seed is not None
            seed_id = seed.id
            assert seed.origin == f"https://{host}"

        async with session_factory() as session:
            service = CrawlService(session, policy)
            same_origin = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url=f"https://{host.upper()}:443/other?query=1",
                depth=1,
            )
            different_scheme = await service.admit_url(
                crawl_id=created.crawl_id,
                source_url_id=seed_id,
                url=f"http://{host}/other",
                depth=1,
            )

        assert same_origin.status is UrlStatus.SKIPPED
        assert different_scheme.status is UrlStatus.QUEUED
    finally:
        await _remove_crawl(session_factory, created.crawl_id)
