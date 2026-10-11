"""HTTP integration tests for Frontier's internal crawl endpoints."""

from collections.abc import AsyncIterator
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.app import create_app
from crawler_frontier.database import create_engine, create_session_factory
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.models import Crawl, CrawlUrl
from crawler_frontier.policy import FrontierPolicy
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
    yield create_session_factory(engine)
    await engine.dispose()


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
