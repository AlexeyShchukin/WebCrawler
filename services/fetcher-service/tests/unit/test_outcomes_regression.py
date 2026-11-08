import logging

import pytest
from crawler_contracts.events import FailureCategory

from crawler_fetcher.network import NetworkFailure
from crawler_fetcher.outcomes import (
    FetchDisposition,
    classify_fetch_failure,
    suggest_retry_delay,
)


def test_large_attempt_is_bounded_before_exponentiation() -> None:
    assert suggest_retry_delay(1_000_000, None, lambda: 0.5) == 3600


@pytest.mark.parametrize("error", [ValueError("sensitive detail"), NetworkFailure("unknown")])
def test_unknown_failure_is_final_and_logged_without_exception_details(error, caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="crawler_fetcher.outcomes"):
        outcome = classify_fetch_failure(error)

    assert outcome.disposition is FetchDisposition.FINAL_FAILURE
    assert outcome.category is FailureCategory.INTERNAL
    assert caplog.records
    assert "sensitive detail" not in caplog.text
