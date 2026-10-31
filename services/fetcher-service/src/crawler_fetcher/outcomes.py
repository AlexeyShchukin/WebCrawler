"""Classification of one Fetcher execution without allocating attempts."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import aiohttp
from crawler_contracts.events import FailureCategory, FetchRetryCategory

from crawler_fetcher.network import NetworkFailure

logger = logging.getLogger(__name__)


class FetchDisposition(StrEnum):
    """Defines how Fetcher handles a request outcome."""

    FINAL_FAILURE = "final_failure"
    RETRY_REQUESTED = "retry_requested"
    PRESTART_RETRY = "prestart_retry"


@dataclass(frozen=True, slots=True)
class FetchOutcome:
    """A classified outcome, deliberately independent of attempt allocation."""

    disposition: FetchDisposition
    category: FailureCategory | FetchRetryCategory | None = None


class ObjectStorageFailure(Exception):
    """A bounded upload adapter reports a failed object-store write."""


def suggest_retry_delay(
    attempt: int, retry_after: str | None, random_seconds: Callable[[], float]
) -> int:
    """Return a bounded server-directed or jittered exponential retry delay."""
    if retry_after is not None and retry_after.isascii() and retry_after.isdecimal():
        parsed = int(retry_after)
        return max(1, min(3600, parsed))
    return min(3600, max(1, int(2 ** min(attempt - 1, 12) + random_seconds())))


def classify_fetch_failure(
    error: BaseException | None = None,
    *,
    http_status: int | None = None,
    started: bool = True,
) -> FetchOutcome:
    """Classify a failure while preserving Frontier's sole attempt ownership."""
    if not started:
        return FetchOutcome(FetchDisposition.PRESTART_RETRY)

    if http_status is not None:
        if http_status in {408, 425, 429} or 500 <= http_status <= 599:
            return FetchOutcome(FetchDisposition.RETRY_REQUESTED, FetchRetryCategory.HTTP_STATUS)
        if 400 <= http_status <= 499:
            return FetchOutcome(FetchDisposition.FINAL_FAILURE, FailureCategory.HTTP_STATUS)

    if isinstance(error, ObjectStorageFailure):
        return FetchOutcome(FetchDisposition.RETRY_REQUESTED, FetchRetryCategory.OBJECT_STORAGE)
    if isinstance(error, (TimeoutError, aiohttp.ServerTimeoutError)):
        return FetchOutcome(FetchDisposition.RETRY_REQUESTED, FetchRetryCategory.TIMEOUT)
    if isinstance(error, aiohttp.ClientConnectionError):
        return FetchOutcome(FetchDisposition.RETRY_REQUESTED, FetchRetryCategory.CONNECTION)
    if isinstance(error, NetworkFailure):
        return _classify_network_failure(error)
    logger.warning("Unclassified fetch failure: %s", type(error).__name__)
    return FetchOutcome(FetchDisposition.FINAL_FAILURE, FailureCategory.INTERNAL)


def _classify_network_failure(error: NetworkFailure) -> FetchOutcome:
    if error.kind == "ssrf_blocked":
        return FetchOutcome(FetchDisposition.FINAL_FAILURE, FailureCategory.SSRF_BLOCKED)
    if error.kind == "unsupported_mime":
        return FetchOutcome(FetchDisposition.FINAL_FAILURE, FailureCategory.UNSUPPORTED_CONTENT)
    if error.kind == "response_too_large":
        return FetchOutcome(FetchDisposition.FINAL_FAILURE, FailureCategory.RESPONSE_TOO_LARGE)
    if error.kind == "timeout":
        return FetchOutcome(FetchDisposition.RETRY_REQUESTED, FetchRetryCategory.TIMEOUT)
    if error.kind in {"connection_failed", "dns_failure"}:
        return FetchOutcome(FetchDisposition.RETRY_REQUESTED, FetchRetryCategory.CONNECTION)
    logger.warning("Unclassified fetch failure: %s", type(error).__name__)
    return FetchOutcome(FetchDisposition.FINAL_FAILURE, FailureCategory.INTERNAL)
