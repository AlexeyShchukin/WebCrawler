from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from crawler_contracts.events import (
    CrawlerEvent,
    FetchRetryRequestedEvent,
    FetchStartedEvent,
    FetchUrlEvent,
    LinksExtractedEvent,
    PageFailedEvent,
    PageFetchedEvent,
    PageProcessedEvent,
)


def event_envelope() -> dict[str, object]:
    return {
        "event_id": uuid4(),
        "crawl_id": uuid4(),
        "url_id": uuid4(),
        "created_at": datetime.now(UTC),
    }


def fetch_envelope() -> dict[str, object]:
    return event_envelope() | {"fetch_attempt": 1}


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        (FetchUrlEvent, {"url": "https://example.com/", "depth": 0}),
        (FetchStartedEvent, {"lease_seconds": 60}),
        (
            FetchRetryRequestedEvent,
            {
                "category": "timeout",
                "detail": "Read timed out",
                "suggested_delay_seconds": 5,
            },
        ),
        (
            PageFetchedEvent,
            {
                "final_url": "https://example.com/",
                "http_status": 200,
                "content_type": "text/html",
                "content_ref": "crawls/5f4a/urls/4d3c/fetches/1.html",
            },
        ),
        (LinksExtractedEvent, {"links": ["https://example.com/about"]}),
        (PageProcessedEvent, {"page_id": uuid4()}),
        (
            PageFailedEvent,
            {"stage": "fetch", "category": "timeout", "detail": "Read timed out"},
        ),
    ],
)
def test_events_preserve_the_common_envelope(
    event_type: type[FetchUrlEvent], payload: dict[str, object]
) -> None:
    envelope = fetch_envelope()

    event = event_type(**envelope, **payload)

    assert event.event_id == envelope["event_id"]
    assert event.crawl_id == envelope["crawl_id"]
    assert event.url_id == envelope["url_id"]
    assert event.fetch_attempt == 1
    assert event.created_at.tzinfo is not None


def test_event_round_trips_through_json() -> None:
    event = FetchUrlEvent(
        **fetch_envelope(),
        url="https://example.com/path",
        depth=2,
    )

    restored = FetchUrlEvent.model_validate_json(event.model_dump_json())

    assert restored == event


def test_crawler_event_requires_a_fetch_attempt() -> None:
    with pytest.raises(ValidationError, match="fetch_attempt"):
        CrawlerEvent(**event_envelope())


def test_fetch_lifecycle_event_rejects_zero_attempt() -> None:
    envelope = fetch_envelope()
    envelope["fetch_attempt"] = 0

    with pytest.raises(ValidationError, match="fetch_attempt"):
        FetchUrlEvent(
            **envelope,
            url="https://example.com/",
            depth=0,
        )


def test_event_rejects_non_utc_timestamp() -> None:
    envelope = fetch_envelope()
    envelope["created_at"] = datetime(2026, 9, 16, 20, 0, tzinfo=timezone(timedelta(hours=2)))

    with pytest.raises(ValidationError, match="UTC"):
        FetchUrlEvent(
            **envelope,
            url="https://example.com/",
            depth=0,
        )


def test_event_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="unexpected"):
        FetchStartedEvent(
            **fetch_envelope(),
            lease_seconds=60,
            unexpected="value",
        )


def test_fetch_retry_requested_event_requires_a_positive_delay() -> None:
    with pytest.raises(ValidationError, match="suggested_delay_seconds"):
        FetchRetryRequestedEvent(
            **fetch_envelope(),
            category="timeout",
            detail="Read timed out",
            suggested_delay_seconds=0,
        )


def test_fetch_retry_requested_event_rejects_a_content_failure_category() -> None:
    with pytest.raises(ValidationError, match="category"):
        FetchRetryRequestedEvent(
            **fetch_envelope(),
            category="parsing",
            detail="The document could not be parsed",
            suggested_delay_seconds=5,
        )


def test_page_fetched_event_rejects_inline_html() -> None:
    with pytest.raises(ValidationError, match="html"):
        PageFetchedEvent(
            **fetch_envelope(),
            final_url="https://example.com/",
            http_status=200,
            content_type="text/html",
            content_ref="crawls/5f4a/urls/4d3c/fetches/1.html",
            html="<html><body>Example</body></html>",
        )
