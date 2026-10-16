"""Long URL identities, database uniqueness and collision rollback."""

import asyncio
import hashlib
import random
import string

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from crawler_frontier.application import crawl_service as services
from crawler_frontier.application.crawl_service import CrawlService
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.models import Crawl, CrawlUrl, Link, OutboxEvent


@pytest.mark.integration
@pytest.mark.asyncio
async def test_valid_incompressible_long_url_is_admitted(session_factory):
    rng = random.Random(42)
    url = "https://example.test/" + "".join(rng.choices(string.ascii_letters + string.digits, k=3500))
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl([url])
    async with session_factory() as session:
        row = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        assert row.normalized_url == url
        assert row.url_digest == hashlib.sha256(url.encode()).digest()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_digest_conflict_never_merges_different_urls(session_factory, monkeypatch):
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy()).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        seed = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    # Force a hash collision in the application lookup; database constraints stay intact.
    monkeypatch.setattr(services, "url_digest", lambda _: hashlib.sha256(seed.normalized_url.encode()).digest(), raising=False)
    async with session_factory() as session:
        with pytest.raises(ValueError, match="digest collision"):
            await CrawlService(session, FrontierPolicy()).admit_url(
                crawl_id=created.crawl_id, source_url_id=seed.id, url="https://example.test/other", depth=1,
            )
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id)) == 1
        assert await session.scalar(select(func.count()).select_from(Link).where(Link.crawl_id == created.crawl_id)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent).where(OutboxEvent.payload["crawl_id"].as_string() == str(created.crawl_id))) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_unique_identity_with_concurrent_direct_inserts(session_factory):
    async with session_factory() as session, session.begin():
        crawl = Crawl(seed_urls=[], allowed_hosts=[])
        session.add(crawl)
    gate = asyncio.Barrier(2)

    async def insert_one():
        try:
            async with session_factory() as session, session.begin():
                await gate.wait()
                session.add(CrawlUrl(crawl_id=crawl.id, normalized_url="https://example.test/identity",
                                     origin="https://example.test", depth=0))
            return "inserted"
        except IntegrityError:
            return "duplicate"

    assert sorted(await asyncio.wait_for(asyncio.gather(insert_one(), insert_one()), 5)) == ["duplicate", "inserted"]
