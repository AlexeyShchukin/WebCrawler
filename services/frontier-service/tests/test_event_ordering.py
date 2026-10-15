"""Current-attempt outcomes apply independently of start-event delivery."""

import asyncio
from itertools import permutations
from uuid import uuid4

import pytest
from crawler_contracts import (
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    LinksExtractedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy import func, select
from test_event_handlers import _create_url

from crawler_frontier.event_handlers import EventIdentityError, FrontierEventHandler
from crawler_frontier.models import CrawlUrl, Link, OutboxEvent, ProcessedEvent
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.services import CrawlService
from crawler_frontier.state_machine import UrlStatus


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["page_fetched", "page_processed", "fetch_failed", "content_failed", "fetch_retry_requested"])
async def test_outcome_without_started_applies_once_and_late_start_is_ignored(session_factory, kind):
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    common = {"crawl_id": crawl_id, "url_id": url_id, "fetch_attempt": 1}
    events = {
        "page_fetched": PageFetchedEvent(**common, final_url="https://example.test/", http_status=200,
                                       content_type="text/html", content_ref="page.html"),
        "page_processed": PageProcessedEvent(**common, page_id=uuid4()),
        "fetch_failed": PageFailedEvent(**common, stage="fetch", category="timeout", detail="timeout"),
        "content_failed": PageFailedEvent(**common, stage="content", category="indexing", detail="indexing failed"),
        "fetch_retry_requested": FetchRetryRequestedEvent(**common, category="connection", detail="reset",
                                                         suggested_delay_seconds=30),
    }
    event = events[kind]
    method_name = "page_failed" if kind.endswith("_failed") else kind
    consumer_name = method_name.replace("_", ".", 1)
    expected = {"page_fetched": UrlStatus.DOWNLOADED, "page_processed": UrlStatus.FETCHED,
                "fetch_failed": UrlStatus.FAILED, "content_failed": UrlStatus.FAILED,
                "fetch_retry_requested": UrlStatus.QUEUED}[kind]
    async with session_factory() as session:
        handler = FrontierEventHandler(session, FrontierPolicy())
        method = getattr(handler, "handle_" + method_name)
        await method(event, consumer_name)
        await method(event, consumer_name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is expected
        assert row.fetch_attempt == (2 if kind == "fetch_retry_requested" else 1)
        assert row.lease_until is None
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent).where(
            ProcessedEvent.event_id == event.event_id,
        )) == 1
    async with session_factory() as session:
        start = FetchStartedEvent(**common)
        handler = FrontierEventHandler(session, FrontierPolicy())
        await handler.handle_fetch_started(start, "fetch.started")
        await handler.handle_fetch_started(start, "fetch.started")
        await getattr(handler, "handle_" + method_name)(event, consumer_name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is expected
        assert row.fetch_attempt == (2 if kind == "fetch_retry_requested" else 1)
        assert row.lease_until is None
        assert await session.scalar(select(func.count()).select_from(OutboxEvent).where(
            OutboxEvent.payload["crawl_id"].as_string() == str(crawl_id),
        )) == (1 if kind == "fetch_retry_requested" else 0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_start_and_outcome_concurrently_never_miss_each_other(session_factory):
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    common = {"crawl_id": crawl_id, "url_id": url_id, "fetch_attempt": 1}
    event = PageProcessedEvent(**common, page_id=uuid4())
    gate = asyncio.Barrier(2)

    async def deliver(start):
        async with session_factory() as session:
            await gate.wait()
            handler = FrontierEventHandler(session, FrontierPolicy())
            if start:
                await handler.handle_fetch_started(FetchStartedEvent(**common), "fetch.started")
            else:
                await handler.handle_page_processed(event, "page.processed")

    await asyncio.wait_for(asyncio.gather(deliver(True), deliver(False)), 5)
    async with session_factory() as session:
        assert (await session.get(CrawlUrl, url_id)).status is UrlStatus.FETCHED
        assert (await session.get(CrawlUrl, url_id)).lease_until is None


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True])
async def test_early_outcome_rolls_back_state_marker_and_outbox(session_factory, monkeypatch, retry):
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    common = {"crawl_id": crawl_id, "url_id": url_id, "fetch_attempt": 1}
    event = (FetchRetryRequestedEvent(**common, category="timeout", detail="timeout", suggested_delay_seconds=30)
             if retry else PageProcessedEvent(**common, page_id=uuid4()))
    method_name = "handle_fetch_retry_requested" if retry else "handle_page_processed"
    consumer_name = "fetch.retry_requested" if retry else "page.processed"
    original = FrontierEventHandler._apply

    async def fail_after_apply(self, url, received):
        await original(self, url, received)
        raise ConnectionError("injected failure before commit")

    monkeypatch.setattr(FrontierEventHandler, "_apply", fail_after_apply)
    async with session_factory() as session:
        with pytest.raises(ConnectionError):
            await getattr(FrontierEventHandler(session, FrontierPolicy()), method_name)(event, consumer_name)
    async with session_factory() as session:
        assert (await session.get(CrawlUrl, url_id)).status is UrlStatus.QUEUED
        assert (await session.get(CrawlUrl, url_id)).fetch_attempt == 1
        assert list(await session.scalars(select(OutboxEvent))) == []
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)) == 0
    monkeypatch.setattr(FrontierEventHandler, "_apply", original)
    async with session_factory() as session:
        await getattr(FrontierEventHandler(session, FrontierPolicy()), method_name)(event, consumer_name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is (UrlStatus.QUEUED if retry else UrlStatus.FETCHED)
        assert row.fetch_attempt == (2 if retry else 1)
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == int(retry)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("order", list(permutations(("fetch.started", "page.fetched", "page.processed", "links.extracted"))))
async def test_success_pipeline_in_any_delivery_order_keeps_status_and_graph(session_factory, order):
    policy = FrontierPolicy()
    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    common = {"crawl_id": created.crawl_id, "url_id": source.id, "fetch_attempt": 1}
    events = {
        "fetch.started": ("handle_fetch_started", FetchStartedEvent(**common)),
        "page.fetched": ("handle_page_fetched", PageFetchedEvent(**common, final_url="https://example.test/",
                            http_status=200, content_type="text/html", content_ref="page.html")),
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
        assert source.status is UrlStatus.FETCHED and source.lease_until is None
        assert source.fetch_attempt == 1
        assert await session.scalar(select(func.count()).select_from(Link)) == 1
        assert await session.scalar(select(func.count()).select_from(CrawlUrl)) == 2
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 2
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_distinct_concurrent_early_retry_results_allocate_only_one_attempt(session_factory):
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED)
    gate = asyncio.Barrier(2)

    async def deliver():
        event = FetchRetryRequestedEvent(crawl_id=crawl_id, url_id=url_id, fetch_attempt=1,
                                        category="timeout", detail="timeout", suggested_delay_seconds=30)
        async with session_factory() as session:
            await gate.wait()
            await FrontierEventHandler(session, FrontierPolicy()).handle_fetch_retry_requested(event, "fetch.retry_requested")

    await asyncio.wait_for(asyncio.gather(deliver(), deliver()), 5)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.fetch_attempt == 2 and row.status is UrlStatus.QUEUED
        assert row.lease_until is None
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 2


def pipeline_event_cases(common):
    return {
        "fetch.started": ("handle_fetch_started", FetchStartedEvent(**common)),
        "page.fetched": ("handle_page_fetched", PageFetchedEvent(**common, final_url="https://example.test/",
                            http_status=200, content_type="text/html", content_ref="page.html")),
        "page.processed": ("handle_page_processed", PageProcessedEvent(**common, page_id=uuid4())),
        "page.failed.fetch": ("handle_page_failed", PageFailedEvent(**common, stage="fetch", category="timeout", detail="timeout")),
        "page.failed.content": ("handle_page_failed", PageFailedEvent(**common, stage="content", category="indexing", detail="indexing")),
        "fetch.retry_requested": ("handle_fetch_retry_requested", FetchRetryRequestedEvent(**common,
            category="timeout", detail="timeout", suggested_delay_seconds=30)),
        "links.extracted": ("handle_links_extracted", LinksExtractedEvent(**common, links=["https://example.test/child"])),
    }


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["fetch.started", "page.fetched", "page.processed", "page.failed.fetch",
                                  "page.failed.content", "fetch.retry_requested", "links.extracted"])
@pytest.mark.parametrize("attempt", [1, 3])
async def test_queued_url_rejects_future_and_ignores_older_pipeline_events(session_factory, name, attempt):
    crawl_id, url_id = await _create_url(session_factory, status=UrlStatus.QUEUED, fetch_attempt=2)
    method_name, event = pipeline_event_cases({"crawl_id": crawl_id, "url_id": url_id, "fetch_attempt": attempt})[name]
    async with session_factory() as session:
        handler = FrontierEventHandler(session, FrontierPolicy())
        for _ in range(2):
            if attempt > 2:
                with pytest.raises(EventIdentityError, match="not been allocated"):
                    await getattr(handler, method_name)(event, name)
            else:
                await getattr(handler, method_name)(event, name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is UrlStatus.QUEUED and row.fetch_attempt == 2 and row.lease_until is None
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
        assert await session.scalar(select(func.count()).select_from(Link)) == 0
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == (1 if attempt == 1 else 0)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("initial", [UrlStatus.DOWNLOADED, UrlStatus.FETCHED, UrlStatus.FAILED])
@pytest.mark.parametrize("name", ["fetch.started", "page.fetched", "page.processed", "page.failed.fetch",
                                  "page.failed.content", "fetch.retry_requested"])
async def test_late_events_never_regress_completed_fetch_or_terminal_status(session_factory, initial, name):
    crawl_id, url_id = await _create_url(session_factory, status=initial)
    method_name, event = pipeline_event_cases({"crawl_id": crawl_id, "url_id": url_id, "fetch_attempt": 1})[name]
    expected = initial
    if initial is UrlStatus.DOWNLOADED and name == "page.processed":
        expected = UrlStatus.FETCHED
    if initial is UrlStatus.DOWNLOADED and name == "page.failed.content":
        expected = UrlStatus.FAILED
    async with session_factory() as session:
        handler = FrontierEventHandler(session, FrontierPolicy())
        await getattr(handler, method_name)(event, name)
        await getattr(handler, method_name)(event, name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, url_id)
        assert row.status is expected and row.fetch_attempt == 1 and row.lease_until is None
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["page.failed.content", "links.extracted"])
async def test_links_and_content_failure_keep_graph_in_both_delivery_orders(session_factory, first):
    policy = FrontierPolicy()
    async with session_factory() as session:
        created = await CrawlService(session, policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        source = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    events = pipeline_event_cases({"crawl_id": created.crawl_id, "url_id": source.id, "fetch_attempt": 1})
    order = [first, "links.extracted" if first == "page.failed.content" else "page.failed.content"]
    for name in order:
        method_name, event = events[name]
        async with session_factory() as session:
            await getattr(FrontierEventHandler(session, policy), method_name)(event, name)
    async with session_factory() as session:
        row = await session.get(CrawlUrl, source.id)
        assert row.status is UrlStatus.FAILED and row.lease_until is None
        assert await session.scalar(select(func.count()).select_from(Link)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 2
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 2
