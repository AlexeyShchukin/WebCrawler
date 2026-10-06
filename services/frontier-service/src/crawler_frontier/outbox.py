"""Transactional-outbox delivery to RabbitMQ."""

import json
from asyncio import Event, wait_for

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractExchange
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.models import OutboxEvent
from crawler_frontier.models.reliability import utc_now


class OutboxPublisher:
    """Publish pending Frontier events and record broker confirmations."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        exchange: AbstractExchange,
    ) -> None:
        self._session_factory = session_factory
        self._exchange = exchange

    async def publish_pending_once(self) -> bool:
        """Publish one pending event, returning whether RabbitMQ confirmed it."""
        async with self._session_factory() as session, session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at, OutboxEvent.event_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return False

            event.publish_attempts += 1
            try:
                await self._exchange.publish(
                    Message(
                        body=json.dumps(event.payload, separators=(",", ":")).encode(),
                        content_type="application/json",
                        message_id=str(event.event_id),
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=event.routing_key,
                )
            except Exception as error:  # noqa: BLE001 - broker failures remain retryable outbox state.
                event.last_publish_error = f"{type(error).__name__}: {error}"[:1024]
                return False

            event.published_at = utc_now()
            event.last_publish_error = None
            return True

    async def run(self, stop_event: Event, poll_interval_seconds: float) -> None:
        """Continuously publish pending events until the service shuts down."""
        while not stop_event.is_set():
            if await self.publish_pending_once():
                continue
            try:
                await wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except TimeoutError:
                continue
