"""Transactional-outbox delivery to RabbitMQ."""

import json
from asyncio import CancelledError, Event, current_task

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractExchange
from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler_frontier.background import WorkerHealth, run_worker
from crawler_frontier.models import OutboxEvent
from crawler_frontier.models.reliability import utc_now


class OutboxPublisher:
    """Publish pending Frontier events and record broker confirmations."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        exchanges: dict[str, AbstractExchange],
    ) -> None:
        self._session_factory = session_factory
        self._exchanges = exchanges
        self.health = WorkerHealth()

    async def publish_pending_once(self) -> bool:
        """Publish one pending event, re-raising an error after its diagnostic commits."""
        publication_error: Exception | None = None
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
                exchange = self._exchanges[event.exchange_name]
                confirmation = await exchange.publish(
                    Message(
                        body=json.dumps(event.payload, separators=(",", ":")).encode(),
                        content_type="application/json",
                        message_id=str(event.event_id),
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=event.routing_key,
                    mandatory=True,
                    timeout=5.0,
                )
                if not isinstance(confirmation, Basic.Ack):
                    raise ConnectionError("RabbitMQ did not positively confirm publication")
            except CancelledError as error:
                task = current_task()
                if task is None or task.cancelling():
                    raise
                # aiormq closes confirmation futures with CancelledError on reconnect.
                # Only an interrupted publish is retryable; roll back before backoff.
                raise ConnectionError("RabbitMQ publication interrupted by transport recovery") from error
            except Exception as error:  # noqa: BLE001 - broker failures remain retryable outbox state.
                publication_error = error
                event.last_publish_error = f"{type(error).__name__}: {error}"[:1024]
            else:
                event.published_at = utc_now()
                event.last_publish_error = None
                return True

        # The transaction committed publication diagnostics. Do not mask a commit error:
        # this executes only after normal context-manager exit.
        if publication_error is not None:
            raise publication_error
        return False

    async def run(self, stop_event: Event, poll_interval_seconds: float) -> None:
        """Continuously publish pending events until the service shuts down."""
        await run_worker(self.publish_pending_once, stop_event, poll_interval_seconds, self.health)
