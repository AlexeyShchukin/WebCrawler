import json
from uuid import uuid4

from crawler_contracts import FetchRetryRequestedEvent


def malformed_event_body(kind):
    """Build synthetic envelopes without relying on the inbound parser."""
    payload = FetchRetryRequestedEvent(
        crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
        category="timeout", detail="test", suggested_delay_seconds=1,
    ).model_dump(mode="json")
    if kind == "invalid_json":
        return b"invalid-json"
    if kind in {"missing_event_id", "missing_created_at", "missing_both"}:
        if kind != "missing_created_at":
            payload.pop("event_id")
        if kind != "missing_event_id":
            payload.pop("created_at")
    elif kind in {"duplicate_event_id", "duplicate_created_at"}:
        field = kind.removeprefix("duplicate_")
        return ('{' + json.dumps(field) + ':' + json.dumps(payload[field]) + ',' + json.dumps(payload)[1:]).encode()
    else:
        field, value = {
            "null_event_id": ("event_id", None),
            "invalid_event_id": ("event_id", "not-a-uuid"),
            "null_created_at": ("created_at", None),
            "invalid_created_at": ("created_at", "not-a-date"),
            "naive_created_at": ("created_at", "2026-10-12T12:00:00"),
            "non_utc_created_at": ("created_at", "2026-10-12T12:00:00+03:00"),
        }[kind]
        payload[field] = value
    return json.dumps(payload).encode()
