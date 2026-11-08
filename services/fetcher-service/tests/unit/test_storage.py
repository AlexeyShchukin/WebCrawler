"""Object-storage boundaries use opaque attempt keys and managed clients."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from botocore.exceptions import ClientError
from crawler_contracts import FetchUrlEvent

from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.storage import ObjectStorage


@pytest.fixture
def settings() -> FetcherSettings:
    return FetcherSettings(
        rabbitmq_url="amqp://worker:password@broker:5672/",
        redis_url="redis://redis:6379/0",
        object_storage_endpoint="http://objects:9000",
        object_storage_access_key="test-access",
        object_storage_secret_key="test-secret",
        object_storage_bucket="html-bodies",
    )


@pytest.fixture
def event() -> FetchUrlEvent:
    return FetchUrlEvent(
        crawl_id=UUID("11111111-1111-1111-1111-111111111111"),
        url_id=UUID("22222222-2222-2222-2222-222222222222"),
        fetch_attempt=3,
        url="https://example.com/private/path?q=private",
        depth=0,
    )


@pytest.fixture
def s3(monkeypatch):
    client = SimpleNamespace(put_object=AsyncMock(return_value={}))
    context = SimpleNamespace()
    # Magic methods must live on a class for Python's context-manager protocol.
    manager = AsyncMock()
    manager.__aenter__.return_value = client
    session = Mock()
    session.client.return_value = manager
    factory = Mock(return_value=session)
    monkeypatch.setattr("crawler_fetcher.storage.aioboto3.Session", factory)
    context.client, context.manager = client, manager
    context.session, context.factory = session, factory
    return context


@pytest.mark.asyncio
async def test_upload_uses_settings_and_one_managed_s3_client(settings, event, s3) -> None:
    async with ObjectStorage(settings) as storage:
        ref = await storage.put_for_event(event, b"<html></html>", "text/html")
        assert "/fetches/3/" in ref and ref.endswith(".html")
        assert s3.manager.__aexit__.await_count == 0

    s3.factory.assert_called_once_with()
    s3.session.client.assert_called_once()
    args, kwargs = s3.session.client.call_args
    assert args == ("s3",)
    assert kwargs["endpoint_url"] == "http://objects:9000/"
    assert kwargs["aws_access_key_id"] == "test-access"
    assert kwargs["aws_secret_access_key"] == "test-secret"
    assert kwargs["config"].signature_version == "s3v4"
    assert kwargs["config"].s3 == {"addressing_style": "path"}
    s3.client.put_object.assert_awaited_once()
    assert s3.client.put_object.await_args.kwargs == {
        "Bucket": "html-bodies", "Key": ref, "Body": b"<html></html>",
        "ContentType": "text/html", "IfNoneMatch": "*",
    }
    s3.manager.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_upload_returns_only_after_success_and_propagates_failure(settings, event, s3) -> None:
    started, finish = asyncio.Event(), asyncio.Event()

    async def upload(**kwargs):
        started.set()
        await finish.wait()
        raise OSError("object storage unavailable")

    s3.client.put_object.side_effect = upload
    with pytest.raises(OSError, match="object storage unavailable"):
        async with ObjectStorage(settings) as storage:
            pending = asyncio.create_task(storage.put_for_event(event, b"html", "text/html"))
            await asyncio.wait_for(started.wait(), timeout=1)
            assert not pending.done()
            finish.set()
            await pending
    s3.manager.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_redelivery_keeps_each_published_snapshot_immutable(settings, event, s3) -> None:
    objects = {}

    async def conditional_put(**kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        key = kwargs["Key"]
        if key in objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"},
                               "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")
        objects[key] = (kwargs["Body"], kwargs["ContentType"])

    s3.client.put_object.side_effect = conditional_put
    async with ObjectStorage(settings) as storage:
        first, duplicate = await asyncio.gather(
            storage.put_for_event(event, b"first", "text/html"),
            storage.put_for_event(event, b"first", "text/html"),
        )
        second, different_type = await asyncio.gather(
            storage.put_for_event(event, b"second", "text/html"),
            storage.put_for_event(event, b"first", "text/html; charset=utf-8"),
        )
    assert duplicate == first
    assert len({first, second, different_type}) == 3
    assert objects[first] == (b"first", "text/html")
    assert objects[second] == (b"second", "text/html")
    assert objects[different_type] == (b"first", "text/html; charset=utf-8")


@pytest.mark.asyncio
async def test_conditional_upload_conflict_is_not_treated_as_success(settings, event, s3):
    s3.client.put_object.side_effect = ClientError(
        {"Error": {"Code": "ConditionalRequestConflict"},
         "ResponseMetadata": {"HTTPStatusCode": 409}}, "PutObject")
    async with ObjectStorage(settings) as storage:
        with pytest.raises(ClientError):
            await storage.put_for_event(event, b"html", "text/html")
