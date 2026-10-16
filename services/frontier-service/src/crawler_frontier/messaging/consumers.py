"""RabbitMQ consumers for Frontier pipeline events."""

import logging
from asyncio import sleep
from collections.abc import Awaitable, Callable
from uuid import uuid4

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage
from crawler_contracts import (
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    LinksExtractedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)
from pamqp.commands import Basic
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.application.event_handlers import FrontierEventHandler
from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.failures import is_transient_failure

logger = logging.getLogger(__name__)


def consumer_callback(
        event_type,
        consumer_name: str,
        handler_name: str,
        session_factory: async_sessionmaker[AsyncSession],
        policy: FrontierPolicy,
        *,
        dead_letter_exchange: AbstractExchange | None = None,
        dead_letter_routing_key: str | None = None,
) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
    """Commit before ACK, or confirm a durable DLQ handoff after bounded retries."""
    async def consume(message: AbstractIncomingMessage) -> None:
        for attempt in range(3):
            try:
                event = event_type.from_message_json(message.body)
                async with session_factory() as session:
                    handler = FrontierEventHandler(session, policy)
                    await getattr(handler, handler_name)(event, consumer_name)
            except Exception as error:  # noqa: BLE001 - every failed delivery needs a reliable settlement.
                transient = is_transient_failure(error)
                if transient and attempt < 2:
                    await sleep(0.1 * 2 ** attempt)
                    continue
                logger.warning("Consumer %s failed (%s)", consumer_name, type(error).__name__)
                break
            else:
                await message.ack()
                return

        try:
            if dead_letter_exchange is None:
                raise RuntimeError("No DLQ exchange configured")
            confirmation = await dead_letter_exchange.publish(
                Message(
                    body=message.body, delivery_mode=DeliveryMode.PERSISTENT,
                    content_type="application/json", message_id=str(uuid4()),
                    headers={"original_message_id": message.message_id or "", "consumer": consumer_name},
                ),
                routing_key=dead_letter_routing_key or consumer_name,
                mandatory=True, timeout=5.0,
            )
            if not isinstance(confirmation, Basic.Ack):
                raise ConnectionError("DLQ publication was not positively confirmed")
        except Exception as error:  # noqa: BLE001 - no handoff failure may acknowledge the original.
            logger.warning("DLQ handoff failed for %s (%s)", consumer_name, type(error).__name__)
            await sleep(1.0)
            # A closed transport leaves the message unsettled for broker recovery.
            if not getattr(message, "processed", False):
                try:
                    await message.nack(requeue=True)
                except Exception:  # noqa: BLE001 - broker recovery owns unsettled closed-channel deliveries.
                    logger.warning("Transport unavailable while requeueing %s", consumer_name)
            return
        await message.ack()
    return consume


CONSUMERS = {
    "links.extracted.queue": ("links.extracted", LinksExtractedEvent, "handle_links_extracted"),
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
