"""Lease-fenced Fetcher worker routes."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from crawler_contracts import FetchUrlEvent, PageFetchedEvent

from crawler_fetcher.lease_client import LeaseGrant, LeaseLostError
from crawler_fetcher.network import FetchedResponse, NetworkFailure
from crawler_fetcher.origin_policy import OriginDeferred
from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.worker import FetcherWorker


@pytest.fixture
def event():
    return FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1, url="https://example.com/page", depth=0)


@pytest.fixture
def worker():
    settings = FetcherSettings(rabbitmq_url="amqp://guest:guest@localhost/", redis_url="redis://localhost:6379/0", object_storage_endpoint="http://localhost:9000", object_storage_access_key="a", object_storage_secret_key="b", object_storage_bucket="crawler")
    result = FetcherWorker(settings)
    result.publisher = Mock(publish=AsyncMock())
    result.origin_policy = Mock(acquire=AsyncMock(), record_success=AsyncMock(), record_failure=AsyncMock())
    result.storage = Mock(put_for_event=AsyncMock(return_value="object-key"))
    result.session = result.resolver = Mock()
    result.fetch = AsyncMock(return_value=FetchedResponse("https://example.com/page", 200, "text/html", b"html"))
    result.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(uuid4(), datetime.now(UTC) + timedelta(minutes=1))), renew=AsyncMock(), submit=AsyncMock())
    return result


@pytest.fixture
def message(event):
    return Mock(body=event.model_dump_json().encode(), ack=AsyncMock(), reject=AsyncMock())


@pytest.mark.asyncio
async def test_success_submits_fenced_result(worker, message):
    await worker.handle_message(message)
    result = worker.lease_client.submit.await_args.args[0]
    assert isinstance(result, PageFetchedEvent)
    assert result.content_ref == "object-key"
    worker.publisher.publish.assert_not_awaited()
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_claim_loss_acks_without_fetch_or_result(worker, message):
    worker.lease_client.claim.side_effect = LeaseLostError()
    await worker.handle_message(message)
    worker.fetch.assert_not_awaited()
    worker.lease_client.submit.assert_not_awaited()
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_prestart_policy_failure_keeps_broker_retry(worker, message, event):
    worker.origin_policy.acquire.side_effect = OriginDeferred()
    await worker.handle_message(message)
    worker.publisher.publish.assert_awaited_once_with(event, exchange_name="crawler.retry", routing_key="fetch.url.retry.5s")
    worker.lease_client.claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_renew_cancels_fetch_and_never_submits(worker, message):
    worker.settings = worker.settings.model_copy(update={"fetch_lease_seconds": 0.03})
    worker.lease_client.renew.side_effect = LeaseLostError()
    entered = asyncio.Event()
    async def slow_fetch(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()
    worker.fetch.side_effect = slow_fetch
    delivery = asyncio.create_task(worker.handle_message(message))
    await entered.wait()
    await delivery
    worker.lease_client.submit.assert_not_awaited()
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_submit_loss_acks_without_direct_publication(worker, message):
    worker.lease_client.submit.side_effect = LeaseLostError()
    await worker.handle_message(message)
    worker.publisher.publish.assert_not_awaited()
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_transient_fetch_failure_submits_retry_request(worker, message):
    worker.fetch.side_effect = NetworkFailure("timeout")
    await worker.handle_message(message)
    result = worker.lease_client.submit.await_args.args[0]
    assert result.category.value == "timeout"
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_delivery_is_rejected(worker, message):
    message.body = b"{}"
    await worker.handle_message(message)
    message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_submit_connection_error_leaves_delivery_unacked(worker, message):
    worker.lease_client.submit.side_effect = ConnectionError("lost response")
    with pytest.raises(ConnectionError):
        await worker.handle_message(message)
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_creates_http_session_and_close_closes_it(worker, monkeypatch):
    from contextlib import asynccontextmanager

    from crawler_fetcher import worker as worker_module

    publisher = Mock()
    publisher.queue.consume = AsyncMock(return_value="consumer")
    publisher.queue.cancel = AsyncMock()
    publisher.channel.is_closed = False
    redis = Mock(ping=AsyncMock(), aclose=AsyncMock())

    @asynccontextmanager
    async def dependency(value):
        yield value

    monkeypatch.setattr(worker_module, "FetcherPublisher", lambda settings: dependency(publisher))
    monkeypatch.setattr(worker_module, "ObjectStorage", lambda settings: dependency(Mock()))
    monkeypatch.setattr(worker_module, "Redis", Mock(from_url=Mock(return_value=redis)))
    await worker.start()
    session = worker.session
    assert not session.closed
    assert session.timeout.total == worker.settings.http_total_timeout_seconds
    await worker.close()
    assert session.closed
    publisher.queue.cancel.assert_awaited_once_with("consumer")
    redis.aclose.assert_awaited_once()
