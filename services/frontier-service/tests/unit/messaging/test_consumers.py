from uuid import uuid4

import pytest
from crawler_contracts import CrawlerEvent

from crawler_frontier.domain.policy import FrontierPolicy
from crawler_frontier.messaging import consumers
from tests.support.event_payloads import malformed_event_body


class _TestEvent(CrawlerEvent):
    """Minimal event model used to exercise generic consumer delivery logic."""


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True, "constraint"])
async def test_consumer_settles_failures_only_after_confirmed_dlq(monkeypatch, permanent):
    from contextlib import asynccontextmanager

    from pamqp.commands import Basic

    event = _TestEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)
    effects = []

    @asynccontextmanager
    async def session_factory():
        yield object()

    class Handler:
        def __init__(self, *args):
            pass

        async def handle_test_event(self, *args):
            effects.append("attempt")
            if permanent == "constraint":
                from sqlalchemy.exc import IntegrityError
                raise IntegrityError("INSERT", {}, ValueError("invalid identity"))
            raise ConnectionError("temporary database failure")

    class Exchange:
        async def publish(self, message, **kwargs):
            effects.append("dlq")
            assert kwargs["mandatory"] is True
            assert kwargs["routing_key"] == "test.event"
            assert message.body == incoming.body
            assert message.expiration is None
            return Basic.Ack(delivery_tag=1)

    class Message:
        body = b"invalid-json" if permanent is True else event.model_dump_json().encode()
        message_id = str(event.event_id)

        async def ack(self):
            effects.append("ack")

        async def nack(self, *, requeue):
            effects.append("nack")

    monkeypatch.setattr(consumers, "FrontierEventHandler", Handler)
    callback = consumers.consumer_callback(
        _TestEvent, "test.event", "handle_test_event",
        session_factory, FrontierPolicy(),
        dead_letter_exchange=Exchange(),
    )
    incoming = Message()
    await callback(incoming)
    expected_attempts = 0 if permanent is True else (1 if permanent == "constraint" else 3)
    assert effects == ["attempt"] * expected_attempts + ["dlq", "ack"]


@pytest.mark.asyncio
async def test_callback_passes_registered_consumer_id_to_the_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = _TestEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)
    handled: list[tuple[_TestEvent, str]] = []

    class RecordingHandler:
        def __init__(self, session, policy) -> None:
            pass

        async def handle_test_event(
            self,
            received_event: _TestEvent,
            consumer_name: str,
        ) -> None:
            handled.append((received_event, consumer_name))

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            return None

    class FakeMessage:
        body = event.model_dump_json().encode()
        acknowledged = False

        async def ack(self) -> None:
            self.acknowledged = True

    monkeypatch.setattr(consumers, "FrontierEventHandler", RecordingHandler)
    callback = consumers.consumer_callback(
        _TestEvent,
        "test.event",
        "handle_test_event",
        lambda: FakeSession(),
        FrontierPolicy(),
    )
    message = FakeMessage()

    await callback(message)

    assert handled == [(event, "test.event")]
    assert message.acknowledged is True


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [
    "missing_event_id", "missing_created_at", "missing_both",
    "null_event_id", "invalid_event_id", "null_created_at", "invalid_created_at",
    "naive_created_at", "non_utc_created_at", "duplicate_event_id", "duplicate_created_at",
])
async def test_repeated_invalid_envelope_goes_to_dlq_without_creating_events(monkeypatch, kind):
    from pamqp.commands import Basic

    body = malformed_event_body(kind)
    effects, handled = [], []

    class Session:
        async def __aenter__(self):
            effects.append("session")
            return self

        async def __aexit__(self, *args):
            pass

    class Handler:
        def __init__(self, *args):
            pass

        async def handle_test_event(self, event, name):
            handled.append(event)

    class Exchange:
        async def publish(self, message, **kwargs):
            assert message.body == body
            assert kwargs == {"routing_key": "test.event", "mandatory": True, "timeout": 5.0}
            effects.append("dlq")
            return Basic.Ack(delivery_tag=1)

    class Message:
        message_id = "synthetic-original"

        def __init__(self):
            self.body = body

        async def ack(self):
            effects.append("ack")

        async def nack(self, *, requeue):
            effects.append("nack")

    monkeypatch.setattr(consumers, "FrontierEventHandler", Handler)
    callback = consumers.consumer_callback(
        _TestEvent, "test.event", "handle_test_event", Session,
        FrontierPolicy(), dead_letter_exchange=Exchange(),
    )
    await callback(Message())
    await callback(Message())
    assert handled == []
    assert effects == ["dlq", "ack", "dlq", "ack"]


@pytest.mark.asyncio
@pytest.mark.parametrize("handoff", ["unconfirmed", "nack", "disconnected"])
async def test_invalid_envelope_is_not_acknowledged_when_dlq_handoff_fails(monkeypatch, handoff):
    from pamqp.commands import Basic

    effects = []

    def unexpected_session():
        effects.append("session")
        raise AssertionError("Invalid envelopes must not enter a transaction")

    class Exchange:
        async def publish(self, message, **kwargs):
            effects.append("dlq")
            if handoff == "disconnected":
                raise ConnectionError("DLQ connection unavailable")
            return Basic.Nack(delivery_tag=1) if handoff == "nack" else None

    class Message:
        body = malformed_event_body("missing_both")
        message_id = "synthetic-original"
        processed = False

        async def ack(self):
            effects.append("ack")

        async def nack(self, *, requeue):
            assert requeue
            effects.append("requeue")

    async def delay(seconds):
        assert seconds == 1.0
        effects.append("delay")

    monkeypatch.setattr(consumers, "sleep", delay)
    callback = consumers.consumer_callback(
        _TestEvent, "test.event", "handle_test_event", unexpected_session,
        FrontierPolicy(), dead_letter_exchange=Exchange(),
    )
    await callback(Message())
    assert effects == ["dlq", "delay", "requeue"]


@pytest.mark.asyncio
async def test_valid_redelivery_preserves_the_received_identity_and_timestamp(monkeypatch):
    from contextlib import asynccontextmanager

    event = _TestEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)
    received, acknowledgements = [], []

    @asynccontextmanager
    async def sessions():
        yield object()

    class Handler:
        def __init__(self, *args):
            pass

        async def handle_test_event(self, incoming, name):
            received.append((incoming.event_id, incoming.created_at))

    class Message:
        body = event.model_dump_json().encode()

        async def ack(self):
            acknowledgements.append(True)

    monkeypatch.setattr(consumers, "FrontierEventHandler", Handler)
    callback = consumers.consumer_callback(
        _TestEvent, "test.event", "handle_test_event", sessions, FrontierPolicy(),
    )
    await callback(Message())
    await callback(Message())
    assert received == [(event.event_id, event.created_at)] * 2
    assert acknowledgements == [True, True]
