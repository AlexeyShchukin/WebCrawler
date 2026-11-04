from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import patch

import aiohttp
import pytest
from crawler_contracts.events import FailureCategory, FetchRetryCategory

from crawler_fetcher.network import NetworkFailure
from crawler_fetcher.outcomes import (
    FetchDisposition,
    ObjectStorageFailure,
    classify_fetch_failure,
    suggest_retry_delay,
)


@pytest.mark.parametrize(("header", "expected"), [
    ("Wed, 04 Nov 2026 12:02:00 GMT", 120),
    ("Wednesday, 04-Nov-26 12:02:00 GMT", 120),
    ("Wed Nov  4 12:02:00 2026", 120),
    ("Wed, 04 Nov 2026 12:00:00 GMT", 1),
    ("Wed, 04 Nov 2026 11:59:59 GMT", 1),
    ("Wed, 04 Nov 2026 14:00:00 GMT", 3600),
])
def test_retry_after_http_date_is_bounded(header: str, expected: int) -> None:
    now = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
    assert suggest_retry_delay(1, header, lambda: 0.5, now=now) == expected


def test_retry_after_http_date_rounds_fractional_seconds_up() -> None:
    now = datetime(2026, 11, 4, 12, 0, 0, 500000, tzinfo=UTC)
    assert suggest_retry_delay(1, "Wed, 04 Nov 2026 12:00:02 GMT", lambda: 0.5, now=now) == 2


def test_bad_http_date_uses_existing_exponential_fallback() -> None:
    now = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
    assert suggest_retry_delay(4, "Wed, 99 Nov 2026 12:00:00 GMT", lambda: 0.5, now=now) == 8


def test_future_http_date_changes_actual_retry_delay() -> None:
    header = format_datetime(datetime.now(UTC) + timedelta(minutes=2), usegmt=True)
    assert 119 <= suggest_retry_delay(1, header, lambda: 0.5) <= 120


@pytest.mark.parametrize("error", [ValueError(), TypeError(), OverflowError(), IndexError()])
def test_http_date_parser_errors_use_fallback(error) -> None:
    with patch("crawler_fetcher.outcomes.parsedate_to_datetime", side_effect=error):
        assert suggest_retry_delay(4, "bad-date", lambda: 0.5) == 8


def test_retry_after_overrides_exponential_delay() -> None:
    assert suggest_retry_delay(1, "120", lambda: 0.5) == 120


@pytest.mark.parametrize(("header", "expected"), [
    ("9" * 5000, 3600), ("0" * 5000 + "120", 120),
    ("0" * 5000, 1), ("3599", 3599), ("3600", 3600), ("3601", 3600),
])
def test_retry_after_large_decimal_is_bounded(header: str, expected: int) -> None:
    assert suggest_retry_delay(1, header, lambda: 0.5) == expected


def test_retry_after_is_capped_at_the_maximum_delay() -> None:
    assert suggest_retry_delay(1, "3601", lambda: 0.5) == 3600


def test_zero_retry_after_is_clamped_without_using_exponential_fallback() -> None:
    assert suggest_retry_delay(8, "0", lambda: 0.9) == 1


@pytest.mark.parametrize("retry_after", ["-0", "+1", "1_0", "１２"])
def test_non_ascii_or_non_decimal_retry_after_uses_exponential_fallback(retry_after: str) -> None:
    assert suggest_retry_delay(8, retry_after, lambda: 0.9) == 128


@pytest.mark.parametrize("retry_after", ["", "-1", "1.5", "tomorrow"])
def test_malformed_retry_after_uses_bounded_exponential_delay(retry_after: str) -> None:
    assert suggest_retry_delay(20, retry_after, lambda: 0.9) == 3600


def test_private_target_rejection_is_a_final_failure() -> None:
    outcome = classify_fetch_failure(NetworkFailure("ssrf_blocked"))

    assert outcome.disposition is FetchDisposition.FINAL_FAILURE
    assert outcome.category is FailureCategory.SSRF_BLOCKED


@pytest.mark.parametrize("kind", ["unsupported_mime", "response_too_large"])
def test_non_retryable_network_rejections_are_final_failures(kind: str) -> None:
    outcome = classify_fetch_failure(NetworkFailure(kind))

    assert outcome.disposition is FetchDisposition.FINAL_FAILURE
    assert outcome.category in {
        FailureCategory.UNSUPPORTED_CONTENT,
        FailureCategory.RESPONSE_TOO_LARGE,
    }


@pytest.mark.parametrize("status", [400, 404, 451])
def test_non_retryable_http_4xx_is_a_final_failure(status: int) -> None:
    outcome = classify_fetch_failure(http_status=status)

    assert outcome.disposition is FetchDisposition.FINAL_FAILURE
    assert outcome.category is FailureCategory.HTTP_STATUS


@pytest.mark.parametrize("status", [408, 425, 429, 500, 599])
def test_retryable_http_status_after_start_requests_retry(status: int) -> None:
    outcome = classify_fetch_failure(http_status=status)

    assert outcome.disposition is FetchDisposition.RETRY_REQUESTED
    assert outcome.category is FetchRetryCategory.HTTP_STATUS


@pytest.mark.parametrize(
    "error, expected_category",
    [
        (NetworkFailure("connection_failed"), FetchRetryCategory.CONNECTION),
        (NetworkFailure("timeout"), FetchRetryCategory.TIMEOUT),
        (aiohttp.ClientConnectionError(), FetchRetryCategory.CONNECTION),
        (TimeoutError(), FetchRetryCategory.TIMEOUT),
        (ObjectStorageFailure(), FetchRetryCategory.OBJECT_STORAGE),
    ],
)
def test_started_transient_failures_request_retry(
    error: BaseException, expected_category: FetchRetryCategory
) -> None:
    outcome = classify_fetch_failure(error)

    assert outcome.disposition is FetchDisposition.RETRY_REQUESTED
    assert outcome.category is expected_category


def test_prestart_failure_preserves_the_existing_attempt_route() -> None:
    outcome = classify_fetch_failure(NetworkFailure("connection_failed"), started=False)

    assert outcome.disposition is FetchDisposition.PRESTART_RETRY
    assert outcome.category is None
