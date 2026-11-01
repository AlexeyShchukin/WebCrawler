"""Confirmed RabbitMQ event delivery and the worker's input queue."""

from contextlib import AsyncExitStack
from types import TracebackType
from typing import Literal, Self

from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust
from crawler_contracts import CrawlerEvent
from pamqp.commands import Basic

from crawler_fetcher.settings import FetcherSettings


class FetcherPublisher:
    """Own the broker resources shared by one Fetcher worker."""

    def __init__(self, settings: FetcherSettings) -> None:
        self._settings = settings
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        try:
            self.connection = await connect_robust(str(self._settings.rabbitmq_url))
            self._stack.push_async_callback(self.connection.close)
            self.channel = await self.connection.channel(
                publisher_confirms=True, on_return_raises=True,
            )
            self._stack.push_async_callback(self.channel.close)
            await self.channel.set_qos(prefetch_count=self._settings.fetch_concurrency)
            self._exchanges = {}
            for name in ("crawler.topic", "crawler.retry"):
                self._exchanges[name] = await self.channel.declare_exchange(
                    name, ExchangeType.TOPIC, durable=True,
                )
            # Infrastructure owns queue bindings and dead-letter arguments.
            self.queue = await self.channel.get_queue("fetch.url.queue", ensure=True)
        except BaseException:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
    ) -> None:
        await self._stack.__aexit__(exc_type, exc_value, traceback)

    async def publish(
        self,
        event: CrawlerEvent,
        *,
        exchange_name: Literal["crawler.topic", "crawler.retry"],
        routing_key: str,
    ) -> None:
        """Publish contract metadata and require a positive broker confirmation."""
        confirmation = await self._exchanges[exchange_name].publish(
            Message(
                body=event.model_dump_json().encode("utf-8"),
                content_type="application/json",
                message_id=str(event.event_id),
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
            mandatory=True,
            timeout=5.0,
        )
        if not isinstance(confirmation, Basic.Ack):
            raise ConnectionError("RabbitMQ did not positively confirm publication")
