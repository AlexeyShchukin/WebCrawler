"""Validated policy values used by Frontier scheduling rules."""

from pydantic import BaseModel, ConfigDict, Field


class FrontierPolicy(BaseModel):
    """Limits that keep crawl scheduling bounded and predictable."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_fetch_attempts: int = Field(default=3, ge=1, le=10)
    fetch_lease_seconds: int = Field(default=120, ge=1, le=3600)
    max_url_length: int = Field(default=4096, ge=1, le=16384)


def can_retry(*, fetch_attempt: int, policy: FrontierPolicy) -> bool:
    """Return whether another fetch attempt may be scheduled."""
    if fetch_attempt < 1:
        raise ValueError("fetch_attempt must be positive")
    return fetch_attempt < policy.max_fetch_attempts
