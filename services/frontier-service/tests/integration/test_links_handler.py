"""Frontier owns admission of Content's extracted links."""
from datetime import UTC, datetime, timedelta

import pytest
from crawler_contracts import LinksExtractedEvent
from sqlalchemy import func, select

from crawler_frontier.application.crawl_service import CrawlService
from crawler_frontier.application.event_handlers import FrontierEventHandler
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.messaging.consumers import CONSUMERS
from crawler_frontier.models import CrawlUrl, Link, OutboxEvent
from crawler_frontier.workers.lease_recovery import LeaseRecovery


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("initial", [UrlStatus.FETCHING, UrlStatus.DOWNLOADED, UrlStatus.FETCHED, UrlStatus.QUEUED, UrlStatus.FAILED])
async def test_links_are_subscribed_admitted_and_deduplicated(session_factory, initial):
    assert CONSUMERS["links.extracted.queue"] == ("links.extracted", LinksExtractedEvent, "handle_links_extracted")
    policy = FrontierPolicy(max_urls_per_crawl=3, max_urls_per_origin=3)
    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl(
            ["https://example.test/"], additional_allowed_hosts=["docs.example.test"],
        )
    async with session_factory() as session, session.begin():
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        source.status = initial
        if initial is UrlStatus.FETCHING:
            source.lease_until = datetime.now(UTC) - timedelta(seconds=1)
        source_id = source.id
    event = LinksExtractedEvent(crawl_id=created.crawl_id, url_id=source_id, fetch_attempt=1, links=(
        "https://example.test/a", "https://example.test/a#fragment", "https://docs.example.test/b",
        "https://external.test/", "https://example.test/over-limit",
    ))
    async with session_factory() as session:
        handler = FrontierEventHandler(session, policy)
        await handler.handle_links_extracted(event, "links.extracted")
        await handler.handle_links_extracted(event, "links.extracted")
    async with session_factory() as session:
        rows = list(await session.scalars(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)))
        assert len(rows) == 3
        source = await session.get(CrawlUrl, source_id)
        assert source.status is (initial if initial in {UrlStatus.FETCHED, UrlStatus.FAILED} else UrlStatus.DOWNLOADED)
        assert source.lease_until is None
        assert sorted(row.depth for row in rows) == [0, 1, 1]
        assert await session.scalar(select(func.count()).select_from(Link).where(Link.crawl_id == created.crawl_id)) == 2
        assert await session.scalar(select(func.count()).select_from(OutboxEvent).where(
            OutboxEvent.payload["crawl_id"].as_string() == str(created.crawl_id),
        )) == 3
    assert await LeaseRecovery(session_factory, policy).recover_expired_once() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unicode_seed_and_content_links_share_allowed_hostname(session_factory):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://BÜCHER.test/"])
    async with session_factory() as session, session.begin():
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        source.status = UrlStatus.FETCHED
        source_id = source.id
    event = LinksExtractedEvent(crawl_id=created.crawl_id, url_id=source_id, fetch_attempt=1,
                               links=["https://bücher.test/café"])
    async with session_factory() as session:
        await FrontierEventHandler(session, FrontierPolicy()).handle_links_extracted(event, "links.extracted")
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)) == 2
