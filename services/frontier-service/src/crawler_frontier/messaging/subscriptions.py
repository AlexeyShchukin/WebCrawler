"""Observe live consumer tags and restore lost subscriptions through aio-pika."""

import logging
from asyncio import Event, wait_for
from collections.abc import Iterable

from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection
from aiormq.abc import AbstractChannel

from crawler_frontier.messaging.consumers import CONSUMERS
from crawler_frontier.workers.runner import WorkerHealth, run_worker

logger = logging.getLogger(__name__)


class ConsumerSubscriptions:
    """Keep the six confirmed subscriptions observable across transport restores."""

    def __init__(
        self, connection: AbstractRobustConnection, channel: AbstractRobustChannel,
        consumer_tags: Iterable[str],
    ) -> None:
        self._connection, self._channel = connection, channel
        self._tags = frozenset(consumer_tags)
        if len(self._tags) != len(CONSUMERS) or any(not isinstance(tag, str) or not tag for tag in self._tags):
            raise ValueError("Every required consumer must have a distinct non-empty tag")
        self.health = WorkerHealth()

    async def get_ready_channel(self) -> AbstractChannel:
        await self._channel.ready()
        return await self._channel.get_underlay_channel()

    def is_registered(self, channel: AbstractChannel) -> bool:
        # RobustQueue's saved consumers are desired state, not live subscriptions.
        # aiormq removes live tags on Basic.Cancel; use the current transport map.
        return not channel.is_closed and self._tags.issubset(channel.consumers)

    async def run(self, stop: Event, poll_interval_seconds: float = 1.0) -> None:
        async def iteration() -> bool:
            channel = await wait_for(self.get_ready_channel(), timeout=1.0)
            if not self.is_registered(channel) and not stop.is_set():
                self.health.healthy = False
                logger.warning("Required RabbitMQ consumer lost; restoring subscriptions")
                # Closing the old transport before restore prevents duplicate tags.
                # RobustQueue restores the original callbacks/tags with ConsumeOk.
                await wait_for(self._connection.reconnect(), timeout=5.0)
                channel = await wait_for(self.get_ready_channel(), timeout=5.0)
                if not self.is_registered(channel):
                    raise ConnectionError("Required RabbitMQ subscriptions were not restored")
            return False

        await run_worker(iteration, stop, poll_interval_seconds, self.health)
