"""FastAPI application lifecycle for Frontier's internal API."""

from asyncio import Event, create_task
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from aio_pika import ExchangeType, connect_robust
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from crawler_frontier.consumers import CONSUMERS, consumer_callback
from crawler_frontier.database import (
    create_engine,
    create_session_factory,
    database_healthcheck,
    get_session,
)
from crawler_frontier.lease_recovery import LeaseRecovery
from crawler_frontier.outbox import OutboxPublisher
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.services import CrawlNotFoundError, CrawlService
from crawler_frontier.settings import FrontierSettings
from crawler_frontier.url_policy import UrlValidationError


class CreateCrawlRequest(BaseModel):
    """Input for Frontier's internal crawl-creation API."""

    model_config = ConfigDict(extra="forbid")

    seed_urls: list[str] = Field(min_length=1)
    additional_allowed_hosts: list[str] = Field(default_factory=list)


class CreatedCrawlResponse(BaseModel):
    """Response returned after Frontier has durably created a crawl."""

    crawl_id: UUID


class CrawlStatusResponse(BaseModel):
    """The derived Frontier status exposed to the API service."""

    crawl_id: UUID
    state: str
    url_counts: dict[str, int]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Create one database engine per process and dispose it on shutdown."""
    settings = FrontierSettings()
    engine = create_engine(settings)
    app.state.database_engine = engine
    app.state.session_factory = create_session_factory(engine)
    app.state.frontier_policy = FrontierPolicy(
        max_fetch_attempts=settings.max_fetch_attempts,
        fetch_lease_seconds=settings.fetch_lease_seconds,
        max_url_length=settings.max_url_length,
        max_crawl_depth=settings.max_crawl_depth,
        max_urls_per_crawl=settings.max_urls_per_crawl,
        max_urls_per_origin=settings.max_urls_per_origin,
    )

    connection = await connect_robust(settings.rabbitmq_url)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=10)
    topic_exchange = await channel.declare_exchange("crawler.topic", ExchangeType.TOPIC, durable=True)
    retry_exchange = await channel.declare_exchange("crawler.retry", ExchangeType.TOPIC, durable=True)
    stop_event = Event()
    publisher_task = create_task(
        OutboxPublisher(
            app.state.session_factory,
            {"crawler.topic": topic_exchange, "crawler.retry": retry_exchange},
        ).run(
            stop_event,
            settings.outbox_poll_interval_seconds,
        )
    )
    lease_recovery_task = create_task(
        LeaseRecovery(app.state.session_factory, app.state.frontier_policy).run(
            stop_event,
            settings.lease_recovery_poll_interval_seconds,
        )
    )
    for queue_name, (consumer_name, event_type, handler_name) in CONSUMERS.items():
        queue = await channel.get_queue(queue_name, ensure=False)
        await queue.consume(
            consumer_callback(
                event_type,
                consumer_name,
                handler_name,
                app.state.session_factory,
                app.state.frontier_policy,
            )
        )
    try:
        yield
    finally:
        stop_event.set()
        await publisher_task
        await lease_recovery_task
        await connection.close()
        await engine.dispose()


def create_app() -> FastAPI:
    """Create the Frontier internal application without starting a server."""
    app = FastAPI(title="Crawler Frontier Service", lifespan=lifespan)

    @app.get("/health", include_in_schema=False)
    async def healthcheck() -> dict[str, str]:
        try:
            await database_healthcheck(app.state.database_engine)
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is unavailable",
            ) from error
        return {"status": "ok"}

    @app.post(
        "/internal/crawls",
        response_model=CreatedCrawlResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_crawl(
        payload: CreateCrawlRequest,
        session: Annotated[AsyncSession, Depends(get_session)],
    ) -> CreatedCrawlResponse:
        """Create a crawl and its initial fetch work inside Frontier's database."""
        try:
            created = await CrawlService(session, app.state.frontier_policy).create_crawl(
                payload.seed_urls,
                additional_allowed_hosts=payload.additional_allowed_hosts,
            )
        except UrlValidationError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=str(error),
            ) from error
        return CreatedCrawlResponse(crawl_id=created.crawl_id)

    @app.get("/internal/crawls/{crawl_id}", response_model=CrawlStatusResponse)
    async def get_crawl_status(
        crawl_id: UUID,
        session: Annotated[AsyncSession, Depends(get_session)],
    ) -> CrawlStatusResponse:
        """Report a crawl's derived scheduling status without mutating it."""
        try:
            crawl_status = await CrawlService(
                session, app.state.frontier_policy
            ).get_crawl_status(crawl_id=crawl_id)
        except CrawlNotFoundError as error:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Crawl not found",
            ) from error
        return CrawlStatusResponse(
            crawl_id=crawl_status.crawl_id,
            state=crawl_status.state,
            url_counts={
                url_status.value: count for url_status, count in crawl_status.url_counts.items()
            },
        )

    return app


app = create_app()
