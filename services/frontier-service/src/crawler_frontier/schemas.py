"""Pydantic request and response models for Frontier's internal API."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CreateCrawlRequest(BaseModel):
    """Input for Frontier's internal crawl-creation API."""

    model_config = ConfigDict(extra="forbid")

    seed_urls: list[str] = Field(min_length=1)
    additional_allowed_hosts: list[str] = Field(default_factory=list)


class CreatedCrawlResponse(BaseModel):
    """Response returned after Frontier has durably created a crawl."""

    crawl_id: UUID


class FetchLeaseRequest(BaseModel):
    """Input required to claim a lease for a scheduled fetch."""

    model_config = ConfigDict(extra="forbid")

    crawl_id: UUID
    url_id: UUID
    fetch_attempt: int = Field(ge=1)
    owner_token: UUID


class FetchLeaseResponse(BaseModel):
    """A granted or renewed fetch lease."""

    owner_token: UUID
    lease_until: datetime


class FetchLeaseRenewRequest(FetchLeaseRequest):
    """Input required to renew an existing fetch lease."""


class FetchLeaseSubmitRequest(FetchLeaseRenewRequest):
    """Input required to submit an outcome for a fetch lease."""

    event: dict[str, object]


class CrawlStatusResponse(BaseModel):
    """The derived Frontier status exposed to the API service."""

    crawl_id: UUID
    state: str
    url_counts: dict[str, int]
