from uuid import uuid4

import pytest
from crawler_contracts import FetchStartedEvent

from crawler_frontier import consumers
from crawler_frontier.policy import FrontierPolicy


@pytest.mark.asyncio
async def test_callback_passes_registered_consumer_id_to_the_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = FetchStartedEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1)
    handled: list[tuple[FetchStartedEvent, str]] = []

    class RecordingHandler:
        def __init__(self, session, policy) -> None:
            pass

        async def handle_fetch_started(
            self,
            received_event: FetchStartedEvent,
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
        FetchStartedEvent,
        "fetch.started",
        "handle_fetch_started",
        lambda: FakeSession(),
        FrontierPolicy(),
    )
    message = FakeMessage()

    await callback(message)

    assert handled == [(event, "fetch.started")]
    assert message.acknowledged is True
