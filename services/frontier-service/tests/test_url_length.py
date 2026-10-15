"""Configured input and canonical length limits apply at every admission boundary."""

import pytest
from sqlalchemy import select

from crawler_frontier.models import CrawlUrl
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.services import CrawlService
from crawler_frontier.url_policy import UrlValidationError, normalize_url


def test_canonical_url_expansion_also_obeys_the_length_limit():
    with pytest.raises(UrlValidationError):
        normalize_url("https://example.test/é", max_length=22)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_policy_limit_is_used_for_seeds_and_discovered_urls(session_factory):
    policy = FrontierPolicy(max_url_length=30)
    async with session_factory() as session:
        with pytest.raises(UrlValidationError):
            await CrawlService(session, policy).create_crawl(["https://example.test/" + "x" * 20])
        created = await CrawlService(session, policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        seed = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    async with session_factory() as session:
        with pytest.raises(UrlValidationError):
            await CrawlService(session, policy).admit_url(crawl_id=created.crawl_id, source_url_id=seed.id,
                                                       url="https://example.test/" + "x" * 20, depth=1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_policy_can_admit_up_to_the_existing_storage_ceiling(session_factory):
    value = "https://example.test/" + "x" * 16_000
    async with session_factory() as session:
        created = await CrawlService(session, FrontierPolicy(max_url_length=16384)).create_crawl([value])
    async with session_factory() as session:
        row = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
        assert row.normalized_url == value
