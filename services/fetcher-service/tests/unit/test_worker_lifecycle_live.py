import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from crawler_contracts import FetchUrlEvent

from crawler_fetcher.lease_client import LeaseGrant, LeaseLostError
from crawler_fetcher.network import FetchedResponse
from crawler_fetcher.worker import FetcherWorker


def make_worker():
    settings = SimpleNamespace(fetch_lease_seconds=0.03, http_max_body_bytes=1000,
                               http_chain_timeout_seconds=5)
    worker = FetcherWorker(settings)
    worker.origin_policy = Mock(acquire=AsyncMock(), record_success=AsyncMock())
    worker.storage = Mock(put_for_event=AsyncMock(return_value='key'))
    worker.session = worker.resolver = None
    worker.lease_client = Mock(claim=AsyncMock(return_value=LeaseGrant(
        uuid4(), datetime.now(UTC) + timedelta(minutes=1))), renew=AsyncMock(), submit=AsyncMock())
    event = FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                          url='https://example.com/', depth=0)
    message = Mock(body=event.model_dump_json().encode(), ack=AsyncMock())
    return worker, message


@pytest.mark.asyncio
async def test_heartbeat_ends_after_successful_operation():
    worker, message = make_worker()
    renewed = asyncio.Event()
    async def renew(**kwargs):
        renewed.set()
    async def fetch(*args, **kwargs):
        await renewed.wait()
        return FetchedResponse('https://example.com/', 200, 'text/html', b'html')
    worker.lease_client.renew.side_effect = renew
    worker.fetch = fetch
    await asyncio.wait_for(worker.handle_message(message), timeout=2)
    count = worker.lease_client.renew.await_count
    await asyncio.sleep(0.05)
    assert count == worker.lease_client.renew.await_count
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_submit_success_racing_with_lease_loss_acks_once():
    worker, message = make_worker()
    renewing, submitted = asyncio.Event(), asyncio.Event()
    async def renew(**kwargs):
        renewing.set()
        await submitted.wait()
        raise LeaseLostError()
    async def submit(*args, **kwargs):
        submitted.set()
    async def fetch(*args, **kwargs):
        await renewing.wait()
        return FetchedResponse('https://example.com/', 200, 'text/html', b'html')
    worker.lease_client.renew.side_effect = renew
    worker.lease_client.submit.side_effect = submit
    worker.fetch = fetch
    await asyncio.wait_for(worker.handle_message(message), timeout=2)
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_cancels_actual_callback_before_dependency_close():
    worker, message = make_worker()
    worker._shutdown_timeout_seconds = 0.01
    entered, cleaned = asyncio.Event(), asyncio.Event()
    order = []
    async def fetch(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.05)
            order.append('fetch cleanup')
            cleaned.set()
    async def close_dependency():
        assert cleaned.is_set()
        order.append('dependency close')
    worker.fetch = fetch
    worker._stack.push_async_callback(close_dependency)
    callback = asyncio.create_task(worker._consume_message(message))
    await entered.wait()
    await asyncio.wait_for(worker.close(), 2)
    assert callback.done()
    assert order == ['fetch cleanup', 'dependency close']
    message.ack.assert_not_awaited()
