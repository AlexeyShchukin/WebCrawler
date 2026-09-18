from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from crawler_frontier.policy import FrontierPolicy, can_retry
from crawler_frontier.state_machine import (
    InvalidStateTransition,
    UrlStatus,
    is_lease_expired,
    lease_expires_at,
    transition,
)


def test_policy_uses_bounded_defaults() -> None:
    policy = FrontierPolicy()

    assert policy.max_fetch_attempts == 3
    assert policy.fetch_lease_seconds == 120
    assert policy.max_url_length == 4_096


def test_policy_rejects_invalid_retry_limit() -> None:
    with pytest.raises(ValidationError, match="max_fetch_attempts"):
        FrontierPolicy(max_fetch_attempts=0)


def test_retry_is_allowed_only_before_the_final_attempt() -> None:
    policy = FrontierPolicy(max_fetch_attempts=3)

    assert can_retry(fetch_attempt=1, policy=policy)
    assert can_retry(fetch_attempt=2, policy=policy)
    assert not can_retry(fetch_attempt=3, policy=policy)


def test_lease_expiry_is_calculated_from_the_configured_duration() -> None:
    now = datetime(2026, 9, 17, 12, tzinfo=UTC)

    assert lease_expires_at(now=now, lease_seconds=120) == now + timedelta(seconds=120)


def test_lease_is_expired_at_its_deadline() -> None:
    deadline = datetime(2026, 9, 17, 12, 2, tzinfo=UTC)

    assert not is_lease_expired(lease_until=deadline, now=deadline - timedelta(microseconds=1))
    assert is_lease_expired(lease_until=deadline, now=deadline)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (UrlStatus.QUEUED, UrlStatus.FETCHING),
        (UrlStatus.QUEUED, UrlStatus.SKIPPED),
        (UrlStatus.FETCHING, UrlStatus.QUEUED),
        (UrlStatus.FETCHING, UrlStatus.FETCHED),
        (UrlStatus.FETCHING, UrlStatus.FAILED),
    ],
)
def test_state_machine_allows_documented_transitions(current: UrlStatus, target: UrlStatus) -> None:
    assert transition(current=current, target=target) is target


def test_state_machine_rejects_terminal_state_transition() -> None:
    with pytest.raises(InvalidStateTransition):
        transition(current=UrlStatus.FETCHED, target=UrlStatus.QUEUED)
