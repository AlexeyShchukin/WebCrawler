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


def test_retry_after_overrides_exponential_delay() -> None:
    assert suggest_retry_delay(1, "120", lambda: 0.5) == 120


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
