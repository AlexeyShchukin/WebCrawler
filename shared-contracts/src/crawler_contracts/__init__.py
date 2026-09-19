"""Shared contracts for communication between crawler services."""

from crawler_contracts.events import (
    CrawlerEvent,
    FailureCategory,
    FailureStage,
    FetchRetryCategory,
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    FetchUrlEvent,
    LinksExtractedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)

__all__ = [
    "CrawlerEvent",
    "FailureCategory",
    "FailureStage",
    "FetchRetryCategory",
    "FetchRetryRequestedEvent",
    "FetchStartedEvent",
    "FetchUrlEvent",
    "LinksExtractedEvent",
    "PageFailedEvent",
    "PageFetchedEvent",
    "PageProcessedEvent",
]
