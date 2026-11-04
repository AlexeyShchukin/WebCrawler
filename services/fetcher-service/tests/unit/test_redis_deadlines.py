import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from crawler_contracts import FetchUrlEvent
from pydantic import ValidationError

from crawler_fetcher.lease_client import LeaseGrant, LeaseLostError
from crawler_fetcher.network import NetworkFailure
from crawler_fetcher.origin_policy import OriginPolicy, OriginPolicyUnavailable
from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.worker import FetcherWorker


class HungRedis:
    async def eval(self, *args):
        await asyncio.Event().wait()


def policy():
    return OriginPolicy(HungRedis(), SimpleNamespace(
        origin_requests_per_second=1, circuit_breaker_failure_threshold=5,
        redis_operation_timeout_seconds=0.02,
    ))


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["acquire", "record_success", "record_failure", "record_rate_limit"])
async def test_policy_operations_fail_closed_when_redis_hangs(method):
    args = ("https://example.com", 5) if method == "record_rate_limit" else ("https://example.com",)
    with pytest.raises(OriginPolicyUnavailable):
        await asyncio.wait_for(getattr(policy(), method)(*args), 0.3)


@pytest.mark.asyncio
async def test_cancellation_is_not_converted_to_policy_unavailable():
    task = asyncio.create_task(policy().acquire("https://example.com"))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_hung_prestart_admission_retries_all_deliveries_before_ack():
    worker = FetcherWorker(Mock())
    worker.origin_policy = policy()
    worker.publisher = Mock(publish=AsyncMock())
    worker.lease_client = Mock(claim=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url="https://example.com/", depth=0)
    messages = [Mock(body=event.model_dump_json().encode(), ack=AsyncMock()) for _ in range(10)]
    await asyncio.wait_for(asyncio.gather(*(worker.handle_message(m) for m in messages)), 0.5)
    assert worker.publisher.publish.await_count == 10
    worker.lease_client.claim.assert_not_awaited()
    assert all(message.ack.await_count == 1 for message in messages)


@pytest.mark.asyncio
async def test_prestart_timeout_waits_for_retry_confirmation_before_ack():
    confirmed = asyncio.Event()
    publishing = asyncio.Event()

    async def publish(*args, **kwargs):
        publishing.set()
        await confirmed.wait()

    worker = FetcherWorker(Mock())
    worker.origin_policy = policy()
    worker.publisher = Mock(publish=AsyncMock(side_effect=publish))
    worker.lease_client = Mock(claim=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url="https://example.com/", depth=0)
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock())
    task = asyncio.create_task(worker.handle_message(message))
    try:
        await asyncio.wait_for(publishing.wait(), 0.5)
        message.ack.assert_not_awaited()
        confirmed.set()
        await asyncio.wait_for(task, 0.5)
        message.ack.assert_awaited_once()
        worker.lease_client.claim.assert_not_awaited()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def test_invalid_redis_timeout_configuration_is_rejected():
    values = {"rabbitmq_url": "amqp://localhost/", "redis_url": "redis://localhost/",
              "object_storage_endpoint": "http://localhost:9000", "object_storage_access_key": "a",
              "object_storage_secret_key": "b", "object_storage_bucket": "crawler"}
    with pytest.raises(ValidationError):
        FetcherSettings(**values, redis_connect_timeout_seconds=5, redis_operation_timeout_seconds=1)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), 11])
def test_redis_timeout_must_be_finite_and_bounded(value):
    values = {"rabbitmq_url": "amqp://localhost/", "redis_url": "redis://localhost/",
              "object_storage_endpoint": "http://localhost:9000", "object_storage_access_key": "a",
              "object_storage_secret_key": "b", "object_storage_bucket": "crawler"}
    with pytest.raises(ValidationError):
        FetcherSettings(**values, redis_operation_timeout_seconds=value)


@pytest.mark.asyncio
async def test_startup_ping_timeout_closes_resources(monkeypatch):
    from crawler_fetcher import worker as worker_module

    closed = []
    @asynccontextmanager
    async def dependency(value):
        try:
            yield value
        finally:
            closed.append(value)

    async def hang():
        await asyncio.Event().wait()

    publisher = Mock()
    storage = Mock()
    redis = Mock(ping=AsyncMock(side_effect=hang), aclose=AsyncMock())
    monkeypatch.setattr(worker_module, "FetcherPublisher", lambda _: dependency(publisher))
    monkeypatch.setattr(worker_module, "ObjectStorage", lambda _: dependency(storage))
    monkeypatch.setattr(worker_module, "Redis", Mock(from_url=Mock(return_value=redis)))
    settings = FetcherSettings(rabbitmq_url="amqp://localhost/", redis_url="redis://localhost/",
                               object_storage_endpoint="http://localhost:9000",
                               object_storage_access_key="a", object_storage_secret_key="b",
                               object_storage_bucket="crawler", redis_connect_timeout_seconds=0.02,
                               redis_operation_timeout_seconds=0.03)
    worker = FetcherWorker(settings)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(worker.start(), 0.5)
    redis.aclose.assert_awaited_once()
    publisher.queue.consume.assert_not_called()
    assert closed == [storage, publisher]
    assert not worker.ready


@pytest.mark.asyncio
async def test_heartbeat_continues_while_policy_failure_is_bounded():
    settings = SimpleNamespace(fetch_lease_seconds=0.03, http_max_body_bytes=100,
                               http_chain_timeout_seconds=1)
    worker = FetcherWorker(settings)
    hung = policy()
    worker.origin_policy = Mock(acquire=AsyncMock(), record_failure=hung.record_failure)
    worker.fetch = AsyncMock(side_effect=NetworkFailure("timeout"))
    worker.session = worker.resolver = None
    worker.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(
        uuid4(), datetime.now(UTC) + timedelta(seconds=5))), renew=AsyncMock(), submit=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url="https://example.com/", depth=0)
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock())
    await asyncio.wait_for(worker.handle_message(message), 0.5)
    assert worker.lease_client.renew.await_count >= 1
    worker.lease_client.submit.assert_awaited_once()
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_lease_loss_cancels_hung_policy_record_before_submit():
    settings = SimpleNamespace(fetch_lease_seconds=0.03, http_max_body_bytes=100,
                               http_chain_timeout_seconds=1)
    worker = FetcherWorker(settings)
    worker.origin_policy = Mock(acquire=AsyncMock(), record_failure=policy().record_failure)
    worker.fetch = AsyncMock(side_effect=NetworkFailure("timeout"))
    worker.session = worker.resolver = None
    worker.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(
        uuid4(), datetime.now(UTC) + timedelta(seconds=5))),
        renew=AsyncMock(side_effect=LeaseLostError()), submit=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url="https://example.com/", depth=0)
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock())
    await asyncio.wait_for(worker.handle_message(message), 0.5)
    worker.lease_client.submit.assert_not_awaited()
    message.ack.assert_awaited_once()
