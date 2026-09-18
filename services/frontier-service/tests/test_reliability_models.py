from sqlalchemy import UniqueConstraint

from crawler_frontier.models.reliability import OutboxEvent, ProcessedEvent


def test_outbox_event_schema_records_publishable_event_data() -> None:
    table = OutboxEvent.__table__

    assert table.name == "outbox_events"
    assert set(table.c.keys()) == {
        "event_id",
        "routing_key",
        "payload",
        "created_at",
        "published_at",
        "publish_attempts",
        "last_publish_error",
    }
    assert table.c.event_id.primary_key
    assert not table.c.routing_key.nullable
    assert not table.c.payload.nullable
    assert table.c.published_at.nullable


def test_processed_event_schema_uses_consumer_and_event_as_idempotency_key() -> None:
    table = ProcessedEvent.__table__

    assert table.name == "processed_events"
    assert set(table.c.keys()) == {"id", "consumer_name", "event_id", "processed_at"}

    constraints = [constraint for constraint in table.constraints if isinstance(constraint, UniqueConstraint)]
    assert any(
        tuple(column.name for column in constraint.columns) == ("consumer_name", "event_id")
        for constraint in constraints
    )
