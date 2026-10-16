import json
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
        (FetchStartedEvent, {}),
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
    assert event_type.from_message_json(event.model_dump_json()) == event


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


def test_fetch_started_event_rejects_fetcher_lease_configuration() -> None:
    with pytest.raises(ValidationError, match="lease_seconds"):
        FetchStartedEvent(
            **fetch_envelope(),
            lease_seconds=60,
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


def test_outgoing_defaults_are_preserved_without_regenerating_received_fields():
    event = FetchStartedEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)
    assert event.event_id is not None
    assert event.created_at.utcoffset() == timedelta(0)
    body = event.model_dump_json()
    first = FetchStartedEvent.from_message_json(body)
    second = FetchStartedEvent.from_message_json(body)
    assert first == second == event


@pytest.mark.parametrize("missing", [
    ("event_id",), ("created_at",), ("event_id", "created_at"),
])
def test_repeated_incoming_json_cannot_generate_missing_envelope_fields(missing):
    payload = FetchStartedEvent(**fetch_envelope()).model_dump(mode="json")
    for field in missing:
        payload.pop(field)
    body = json.dumps(payload)
    for _ in range(2):
        with pytest.raises(ValueError, match="event_id|created_at"):
            FetchStartedEvent.from_message_json(body)


@pytest.mark.parametrize("field,value", [
    ("event_id", None), ("event_id", "not-a-uuid"),
    ("created_at", None), ("created_at", "not-a-date"),
    ("created_at", "2026-10-12T12:00:00"),
    ("created_at", "2026-10-12T12:00:00+03:00"),
])
def test_incoming_envelope_rejects_invalid_identity_or_timestamp(field, value):
    payload = FetchStartedEvent(**fetch_envelope()).model_dump(mode="json")
    payload[field] = value
    with pytest.raises(ValidationError, match=field):
        FetchStartedEvent.from_message_json(json.dumps(payload))


@pytest.mark.parametrize("field", ["event_id", "created_at"])
def test_incoming_json_rejects_duplicate_envelope_keys(field):
    payload = FetchStartedEvent(**fetch_envelope()).model_dump(mode="json")
    body = '{' + json.dumps(field) + ':' + json.dumps(payload[field]) + ',' + json.dumps(payload)[1:]
    with pytest.raises(ValueError, match=field):
        FetchStartedEvent.from_message_json(body)


@pytest.mark.parametrize("body", ["null", "[]", "42", '"event"'])
def test_incoming_json_requires_an_object(body):
    with pytest.raises(TypeError):
        FetchStartedEvent.from_message_json(body)


def test_equal_timestamps_do_not_merge_different_events():
    first = FetchStartedEvent(**fetch_envelope())
    second = FetchStartedEvent(crawl_id=first.crawl_id, url_id=first.url_id,
                               fetch_attempt=1, created_at=first.created_at)
    restored = [FetchStartedEvent.from_message_json(event.model_dump_json()) for event in (first, second)]
    assert restored[0].created_at == restored[1].created_at
    assert restored[0].event_id != restored[1].event_id
