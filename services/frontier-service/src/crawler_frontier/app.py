"""FastAPI application lifecycle for Frontier's internal API."""

import logging
from asyncio import Event, create_task, current_task, gather, wait, wait_for
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
from crawler_frontier.subscriptions import ConsumerSubscriptions
from crawler_frontier.url_policy import UrlValidationError

logger = logging.getLogger(__name__)
_CONSUMER_POLL_INTERVAL_SECONDS = 1.0


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
    connection = None
    consumer_monitor_task = None
    subscriptions = []
    tasks = []
    inflight = set()
    stop_event = Event()
    app.state.ready = False
    try:
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
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        app.state.broker_connection, app.state.broker_channel = connection, channel
        await channel.set_qos(prefetch_count=10)
        topic = await channel.declare_exchange("crawler.topic", ExchangeType.TOPIC, durable=True)
        retry = await channel.declare_exchange("crawler.retry", ExchangeType.TOPIC, durable=True)
        dlx = await channel.declare_exchange("crawler.dlx", ExchangeType.TOPIC, durable=True)
        publisher = OutboxPublisher(app.state.session_factory, {"crawler.topic": topic, "crawler.retry": retry})
        recovery = LeaseRecovery(app.state.session_factory, app.state.frontier_policy)
        app.state.workers = (publisher, recovery)
        tasks.extend((
            create_task(publisher.run(stop_event, settings.outbox_poll_interval_seconds)),
            create_task(recovery.run(stop_event, settings.lease_recovery_poll_interval_seconds)),
        ))
        app.state.worker_tasks = tasks

        def tracked(callback):
            async def consume(message):
                task = current_task()
                inflight.add(task)
                try:
                    await callback(message)
                finally:
                    inflight.discard(task)
            return consume

        for queue_name, (consumer_name, event_type, handler_name) in CONSUMERS.items():
            queue = await channel.get_queue(queue_name, ensure=True)
            tag = await queue.consume(tracked(consumer_callback(
                event_type, consumer_name, handler_name,
                app.state.session_factory, app.state.frontier_policy,
                dead_letter_exchange=dlx,
                dead_letter_routing_key=queue_name.removesuffix(".queue"),
            )))
            subscriptions.append((queue, tag))
        monitor = ConsumerSubscriptions(connection, channel, (tag for _, tag in subscriptions))
        if not monitor.is_registered(await wait_for(monitor.get_ready_channel(), timeout=1.0)):
            raise ConnectionError("Required RabbitMQ subscriptions were not registered")
        monitor.health.healthy = True
        app.state.consumer_subscriptions = monitor
        app.state.workers = (*app.state.workers, monitor)
        consumer_monitor_task = create_task(monitor.run(stop_event, _CONSUMER_POLL_INTERVAL_SECONDS))
        tasks.append(consumer_monitor_task)
        app.state.ready = True
        yield
    finally:
        app.state.ready = False
        stop_event.set()
        if consumer_monitor_task is not None:
            consumer_monitor_task.cancel()
        try:
            for queue, tag in subscriptions:
                try:
                    await wait_for(queue.cancel(tag), timeout=5)
                except Exception as error:  # noqa: BLE001 - cleanup must continue to resource disposal.
                    logger.warning("Consumer cancellation failed (%s)", type(error).__name__)
            draining = set(tasks) | inflight
            if draining:
                done, pending = await wait(draining, timeout=10)
                for task in pending:
                    task.cancel()
                if pending:
                    cancelled_done, pending = await wait(pending, timeout=2)
                    done.update(cancelled_done)
                    if pending:
                        logger.warning("Shutdown continuing with %d tasks still pending after cancellation", len(pending))
                # cancel() is cooperative: never await cancellation-resistant tasks indefinitely.
                await gather(*done, return_exceptions=True)
        finally:
            try:
                if connection is not None:
                    await wait_for(connection.close(), timeout=5)
            finally:
                await engine.dispose()


def create_app() -> FastAPI:
    """Create the Frontier internal application without starting a server."""
    app = FastAPI(title="Crawler Frontier Service", lifespan=lifespan)

    @app.get("/health", include_in_schema=False)
    async def healthcheck() -> dict[str, str]:
        if (
            not getattr(app.state, "ready", False)
            or app.state.broker_connection.is_closed
            or not app.state.broker_connection.connected.is_set()
            or app.state.broker_channel.is_closed
            or any(task.done() for task in app.state.worker_tasks)
            or any(not worker.health.healthy for worker in app.state.workers)
        ):
            raise HTTPException(status_code=503, detail="Frontier workers or RabbitMQ are unavailable")
        try:
            channel = await wait_for(app.state.consumer_subscriptions.get_ready_channel(), timeout=1.0)
        except Exception as error:
            raise HTTPException(status_code=503, detail="RabbitMQ subscriptions are recovering") from error
        try:
            await database_healthcheck(app.state.database_engine)
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is unavailable",
            ) from error
        # Recheck the live map after the DB probe, without yielding before return.
        if (
            not app.state.consumer_subscriptions.is_registered(channel)
            or any(task.done() for task in app.state.worker_tasks)
            or any(not worker.health.healthy for worker in app.state.workers)
        ):
            raise HTTPException(status_code=503, detail="Required RabbitMQ consumers are unavailable")
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
