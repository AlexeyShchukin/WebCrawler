"""RabbitMQ consumers for Frontier pipeline events."""

from collections.abc import Awaitable, Callable

from aio_pika.abc import AbstractIncomingMessage
from crawler_contracts import (
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.event_handlers import FrontierEventHandler
from crawler_frontier.policy import FrontierPolicy


def consumer_callback(
        event_type,
        consumer_name: str,
        handler_name: str,
        session_factory: async_sessionmaker[AsyncSession],
        policy: FrontierPolicy
) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
    """Build a manual-ack callback that commits Frontier state before acknowledging."""
    async def consume(message: AbstractIncomingMessage) -> None:
        event = event_type.model_validate_json(message.body)
        async with session_factory() as session:
            handler = FrontierEventHandler(session, policy)
            await getattr(handler, handler_name)(event, consumer_name)
        await message.ack()
    return consume


CONSUMERS = {
    "fetch.started.queue": ("fetch.started", FetchStartedEvent, "handle_fetch_started"),
    "page.fetched.frontier.queue": ("page.fetched", PageFetchedEvent, "handle_page_fetched"),
    "fetch.retry_requested.queue": (
        "fetch.retry_requested",
        FetchRetryRequestedEvent,
        "handle_fetch_retry_requested",
    ),
    "page.processed.queue": ("page.processed", PageProcessedEvent, "handle_page_processed"),
    "page.failed.queue": ("page.failed", PageFailedEvent, "handle_page_failed"),
}
