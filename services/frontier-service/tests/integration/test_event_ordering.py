"""Ordering regressions for Content events after an HTTP fetch submit."""

from itertools import permutations
from uuid import uuid4

import pytest
from crawler_contracts import (
    LinksExtractedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy import func, select

from crawler_frontier.application.crawl_service import CrawlService
from crawler_frontier.application.event_handlers import (
    EventIdentityError,
    FrontierEventHandler,
)
from crawler_frontier.application.fetch_lease_service import FetchLeaseService
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import CrawlUrl, Link, OutboxEvent, ProcessedEvent


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("order", list(permutations(("page.processed", "links.extracted"))))
async def test_content_events_after_http_fetch_submit_keep_status_and_graph(session_factory, order):
    policy = FrontierPolicy()
    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    common = {"crawl_id": created.crawl_id, "url_id": source.id, "fetch_attempt": 1}
    async with session_factory() as session:
        lease_service = FetchLeaseService(session, policy)
        grant = await lease_service.claim(**common)
        await lease_service.submit(PageFetchedEvent(
            **common, final_url="https://example.test/", http_status=200,
            content_type="text/html", content_ref="page.html",
        ), owner_token=grant.owner_token)
    events = {
        "page.processed": ("handle_page_processed", PageProcessedEvent(**common, page_id=uuid4())),
        "links.extracted": ("handle_links_extracted", LinksExtractedEvent(**common, links=["https://example.test/child"])),
    }
    for name in order:
        method_name, event = events[name]
        async with session_factory() as session:
            handler = FrontierEventHandler(session, policy)
            await getattr(handler, method_name)(event, name)
            await getattr(handler, method_name)(event, name)
    async with session_factory() as session:
        source = await session.get(CrawlUrl, source.id)
        assert source.status is UrlStatus.FETCHED
        assert await session.scalar(select(func.count()).select_from(Link)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 3
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 2


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 3])
async def test_content_events_ignore_old_attempts_and_reject_future_attempts(session_factory, attempt):
    policy = FrontierPolicy()
    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    event = PageProcessedEvent(crawl_id=created.crawl_id, url_id=source.id, fetch_attempt=attempt, page_id=uuid4())
    async with session_factory() as session:
        handler = FrontierEventHandler(session, policy)
        if attempt > 1:
            with pytest.raises(EventIdentityError, match="not been allocated"):
                await handler.handle_page_processed(event, "page.processed")
        else:
            await handler.handle_page_processed(event, "page.processed")
    async with session_factory() as session:
        row = await session.get(CrawlUrl, source.id)
        assert row.status is (UrlStatus.FETCHED if attempt == 1 else UrlStatus.QUEUED)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["page.failed", "links.extracted"])
async def test_links_and_content_failure_keep_graph_in_both_delivery_orders(session_factory, first):
    policy = FrontierPolicy()
    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    common = {"crawl_id": created.crawl_id, "url_id": source.id, "fetch_attempt": 1}
    events = {
        "page.failed": ("handle_page_failed", PageFailedEvent(**common, stage="content", category="indexing", detail="failed")),
        "links.extracted": ("handle_links_extracted", LinksExtractedEvent(**common, links=["https://example.test/child"])),
    }
    for name in (first, "links.extracted" if first == "page.failed" else "page.failed"):
        method_name, event = events[name]
        async with session_factory() as session:
            await getattr(FrontierEventHandler(session, policy), method_name)(event, name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, source.id)
        assert row.status is UrlStatus.FAILED
        assert await session.scalar(select(func.count()).select_from(Link)) == 1
