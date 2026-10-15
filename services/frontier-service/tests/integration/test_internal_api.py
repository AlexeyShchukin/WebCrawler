"""HTTP integration tests for Frontier's internal crawl endpoints."""

from uuid import UUID

import httpx
import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.app import create_app
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import Crawl, CrawlUrl


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_crawl_and_report_its_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app()
    app.state.session_factory = session_factory
    app.state.frontier_policy = FrontierPolicy()
    transport = httpx.ASGITransport(app=app)

    crawl_id: UUID | None = None
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://frontier") as client:
            created_response = await client.post(
                "/internal/crawls",
                json={
                    "seed_urls": ["https://example.test/", "https://docs.example.test/guide"],
                    "additional_allowed_hosts": ["assets.example.test"],
                },
            )
            assert created_response.status_code == 201
            crawl_id = UUID(created_response.json()["crawl_id"])

            running_response = await client.get(f"/internal/crawls/{crawl_id}")
            assert running_response.status_code == 200
            assert running_response.json() == {
                "crawl_id": str(crawl_id),
                "state": "running",
                "url_counts": {
                    "queued": 2,
                    "fetching": 0,
                    "downloaded": 0,
                    "fetched": 0,
                    "failed": 0,
                    "skipped": 0,
                },
            }

            async with session_factory() as session, session.begin():
                urls = list(
                    await session.scalars(select(CrawlUrl).where(CrawlUrl.crawl_id == crawl_id))
                )
                for url in urls:
                    url.status = UrlStatus.FETCHED

            completed_response = await client.get(f"/internal/crawls/{crawl_id}")
            assert completed_response.status_code == 200
            assert completed_response.json()["state"] == "completed"
            assert completed_response.json()["url_counts"]["fetched"] == 2
    finally:
        if crawl_id is not None:
            async with session_factory() as session, session.begin():
                await session.execute(delete(Crawl).where(Crawl.id == crawl_id))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_internal_crawl_endpoints_validate_input_and_return_not_found(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app()
    app.state.session_factory = session_factory
    app.state.frontier_policy = FrontierPolicy()
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://frontier") as client:
        invalid_response = await client.post("/internal/crawls", json={"seed_urls": []})
        missing_response = await client.get("/internal/crawls/00000000-0000-0000-0000-000000000000")

    assert invalid_response.status_code == 422
    assert missing_response.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("host", ["exam%ple.test", "a..test", "-invalid.test"])
async def test_bad_hostname_returns_422_and_creates_no_crawl(session_factory, host):
    app = create_app()
    app.state.session_factory = session_factory
    app.state.frontier_policy = FrontierPolicy()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://frontier") as client:
        response = await client.post("/internal/crawls", json={"seed_urls": [f"https://{host}/"]})
        assert response.status_code == 422
