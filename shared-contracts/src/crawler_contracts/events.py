"""Versioned Pydantic contracts for RabbitMQ crawler events."""

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(UTC)


class CrawlerEvent(BaseModel):
    """Fields required on every event exchanged by crawler services."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    crawl_id: UUID
    url_id: UUID
    fetch_attempt: Annotated[int, Field(ge=1)]
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def created_at_must_be_utc(cls, value: datetime) -> datetime:
        """Validate that the timestamp is timezone-aware UTC."""
        if value.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

        if value.utcoffset() != timedelta(0):
            raise ValueError("created_at must be in UTC")

        return value


class FetchUrlEvent(CrawlerEvent):
    """A URL admitted by Frontier and ready for Fetcher."""

    url: AnyHttpUrl
    depth: Annotated[int, Field(ge=0)]


class FetchStartedEvent(CrawlerEvent):
    """A Fetcher has claimed a task and Frontier should start its lease."""

    lease_seconds: Annotated[int, Field(gt=0)]


class PageFetchedEvent(CrawlerEvent):
    """Metadata for a bounded HTML response stored in object storage."""

    final_url: AnyHttpUrl
    http_status: Annotated[int, Field(ge=100, le=599)]
    content_type: Annotated[str, Field(min_length=1)]
    content_ref: Annotated[str, Field(min_length=1, max_length=1024)]


class LinksExtractedEvent(CrawlerEvent):
    """Links extracted from a fetched page by Content."""

    links: tuple[AnyHttpUrl, ...]


class PageProcessedEvent(CrawlerEvent):
    """A page persisted and indexed successfully by Content."""

    page_id: UUID


class FailureStage(StrEnum):
    """Pipeline component that produced a final failure."""

    FETCH = "fetch"
    CONTENT = "content"


class FailureCategory(StrEnum):
    """Stable categories used for final failures and operational metrics."""

    CONNECTION = "connection"
    TIMEOUT = "timeout"
    HTTP_STATUS = "http_status"
    UNSUPPORTED_CONTENT = "unsupported_content"
    RESPONSE_TOO_LARGE = "response_too_large"
    SSRF_BLOCKED = "ssrf_blocked"
    PARSING = "parsing"
    INDEXING = "indexing"
    INTERNAL = "internal"


class FetchRetryCategory(StrEnum):
    """Retryable failure categories for one started fetch execution."""

    CONNECTION = "connection"
    TIMEOUT = "timeout"
    HTTP_STATUS = "http_status"
    OBJECT_STORAGE = "object_storage"


class FetchRetryRequestedEvent(CrawlerEvent):
    """A retryable fetch-execution failure for Frontier to reschedule."""

    category: FetchRetryCategory
    detail: Annotated[str, Field(min_length=1, max_length=1000)]
    suggested_delay_seconds: Annotated[int, Field(ge=1, le=3600)]


class PageFailedEvent(CrawlerEvent):
    """A final fetch or content-processing failure."""

    stage: FailureStage
    category: FailureCategory
    detail: Annotated[str, Field(min_length=1, max_length=1_000)]
