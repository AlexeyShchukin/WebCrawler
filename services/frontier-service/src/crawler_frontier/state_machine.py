"""State transitions and lease calculations for a crawl URL."""

from datetime import datetime, timedelta
from enum import StrEnum


class UrlStatus(StrEnum):
    """Persistent scheduling states for one normalized URL."""

    QUEUED = "queued"
    FETCHING = "fetching"
    DOWNLOADED = "downloaded"
    FETCHED = "fetched"
    FAILED = "failed"
    SKIPPED = "skipped"


class InvalidStateTransition(ValueError):
    """Raised when a URL would move outside the documented state machine."""


_ALLOWED_TRANSITIONS: dict[UrlStatus, frozenset[UrlStatus]] = {
    UrlStatus.QUEUED: frozenset({UrlStatus.FETCHING, UrlStatus.FAILED, UrlStatus.SKIPPED}),
    UrlStatus.FETCHING: frozenset(
        {UrlStatus.QUEUED, UrlStatus.DOWNLOADED, UrlStatus.FETCHED, UrlStatus.FAILED}
    ),
    UrlStatus.DOWNLOADED: frozenset({UrlStatus.FETCHED, UrlStatus.FAILED}),
    UrlStatus.FETCHED: frozenset(),
    UrlStatus.FAILED: frozenset(),
    UrlStatus.SKIPPED: frozenset(),
}


def transition(*, current: UrlStatus, target: UrlStatus) -> UrlStatus:
    """Validate and return one documented URL-state transition."""
    if target not in _ALLOWED_TRANSITIONS[current]:
        raise InvalidStateTransition(f"Cannot transition URL from {current} to {target}")
    return target


def lease_expires_at(*, now: datetime, lease_seconds: int) -> datetime:
    """Calculate the UTC-aware deadline for a claimed fetch task."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if now.utcoffset() != timedelta(0):
        raise ValueError("now must be in UTC")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    return now + timedelta(seconds=lease_seconds)


def is_lease_expired(*, lease_until: datetime, now: datetime) -> bool:
    """Return whether a fetch lease has reached or passed its deadline."""
    if lease_until.tzinfo is None:
        raise ValueError("lease_until must be timezone-aware")
    if lease_until.utcoffset() != timedelta(0):
        raise ValueError("lease_until must be in UTC")

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if now.utcoffset() != timedelta(0):
        raise ValueError("now must be in UTC")
    return now >= lease_until
