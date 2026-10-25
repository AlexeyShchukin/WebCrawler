"""HTTP integration tests for Frontier's internal crawl endpoints."""

import asyncio
import socket
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.app import create_app
from crawler_frontier.application.crawl_service import CrawlService
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.domain.state_machine import UrlStatus
from crawler_frontier.models import Crawl, CrawlUrl, OutboxEvent


async def _serve_over_tcp(app):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    for _ in range(100):
        if server.started:
            return server, task, f"http://127.0.0.1:{port}"
        await asyncio.sleep(0.01)
    server.should_exit = True
    await task
    raise RuntimeError("uvicorn did not start")


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
async def test_fetch_lease_routes_claim_renew_submit_and_reject_stale_owner(session_factory) -> None:
    app = create_app()
    app.state.session_factory = session_factory
    app.state.frontier_policy = FrontierPolicy(fetch_lease_seconds=30)
    async with session_factory() as session:
        created = await CrawlService(session, app.state.frontier_policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        url = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))

    identity = {
        "crawl_id": str(created.crawl_id),
        "url_id": str(url.id),
        "fetch_attempt": 1,
        "owner_token": str(uuid4()),
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://frontier") as client:
        claim = await client.post("/internal/fetch-leases/claim", json=identity)
        assert claim.status_code == 200
        owner_token = claim.json()["owner_token"]
        repeated_claim = await client.post("/internal/fetch-leases/claim", json=identity)
        assert repeated_claim.status_code == 200
        assert repeated_claim.json() == claim.json()
        renew = await client.post(
            "/internal/fetch-leases/renew", json={**identity, "owner_token": owner_token},
        )
        assert renew.status_code == 200
        event = {
            **{key: value for key, value in identity.items() if key != "owner_token"},
            "event_id": "00000000-0000-0000-0000-000000000001",
            "created_at": "2026-10-24T00:00:00Z",
            "final_url": "https://example.test/",
            "http_status": 200,
            "content_type": "text/html",
            "content_ref": "snapshot",
        }
        submitted = await client.post(
            "/internal/fetch-leases/submit", json={**identity, "owner_token": owner_token, "event": event},
        )
        assert submitted.status_code == 204
        stale = await client.post(
            "/internal/fetch-leases/renew", json={**identity, "owner_token": owner_token},
        )
        assert stale.status_code == 409


@pytest.mark.integration
@pytest.mark.asyncio
async def test_submit_is_idempotent_when_real_http_client_loses_first_response(session_factory) -> None:
    app = create_app()
    app.state.session_factory = session_factory
    app.state.frontier_policy = FrontierPolicy(fetch_lease_seconds=30)
    async with session_factory() as session:
        created = await CrawlService(session, app.state.frontier_policy).create_crawl(["https://example.test/"])
    async with session_factory() as session:
        url = await session.scalar(select(CrawlUrl).where(CrawlUrl.crawl_id == created.crawl_id))
    identity = {"crawl_id": str(created.crawl_id), "url_id": str(url.id), "fetch_attempt": 1}
    server, task, base_url = await _serve_over_tcp(app)
    try:
        async with httpx.AsyncClient(base_url=base_url) as client:
            owner_token = str(uuid4())
            claimed = await client.post("/internal/fetch-leases/claim", json={**identity, "owner_token": owner_token})
            assert claimed.status_code == 200
            await claimed.aclose()
            event = {
                **identity,
                "event_id": str(uuid4()),
                "created_at": "2026-10-24T00:00:00Z",
                "final_url": "https://example.test/",
                "http_status": 200,
                "content_type": "text/html",
                "content_ref": "snapshot",
            }
            payload = {**identity, "owner_token": owner_token, "event": event}
            lost_response = await client.post("/internal/fetch-leases/submit", json=payload)
            await lost_response.aclose()
            repeated = await client.post("/internal/fetch-leases/submit", json=payload)
            assert repeated.status_code == 204
            await repeated.aclose()
    finally:
        server.should_exit = True
        await task
    async with session_factory() as session:
        outbox = list(await session.scalars(select(OutboxEvent).where(OutboxEvent.event_id == UUID(event["event_id"]))))
        assert len(outbox) == 1


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
