"""FastAPI application lifecycle for Frontier's internal API."""

from asyncio import Event, create_task
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aio_pika import ExchangeType, connect_robust
from fastapi import FastAPI, HTTPException, status

from crawler_frontier.consumers import CONSUMERS, consumer_callback
from crawler_frontier.database import (
    create_engine,
    create_session_factory,
    database_healthcheck,
)
from crawler_frontier.outbox import OutboxPublisher
from crawler_frontier.policy import FrontierPolicy
from crawler_frontier.settings import FrontierSettings


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

    return app


app = create_app()
