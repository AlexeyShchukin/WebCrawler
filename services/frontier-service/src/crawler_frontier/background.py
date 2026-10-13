"""Observable, stoppable workers with bounded backoff on infrastructure failures."""

import logging
from asyncio import Event, wait_for
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from crawler_frontier.failures import is_transient_failure

logger = logging.getLogger(__name__)


@dataclass
class WorkerHealth:
    healthy: bool = False
    consecutive_failures: int = 0


async def run_worker(
    operation: Callable[[], Awaitable[int | bool]], stop: Event,
    poll_interval: float, health: WorkerHealth,
) -> None:
    while not stop.is_set():
        try:
            work_done = await operation()
        except Exception as error:
            health.healthy = False
            if not is_transient_failure(error):
                logger.error("Background worker terminated (%s)", type(error).__name__)
                raise
            health.consecutive_failures += 1
            logger.warning("Worker iteration failed (%s)", type(error).__name__)
            delay = min(30.0, poll_interval * 2 ** min(health.consecutive_failures, 10))
        else:
            health.healthy = True
            health.consecutive_failures = 0
            if work_done:
                continue
            delay = poll_interval
        try:
            await wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
