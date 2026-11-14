"""Bounded, managed reads from the configured object-storage bucket."""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from crawler_content.settings import ContentSettings
from crawler_content.storage import (
    ObjectNotFoundError,
    ObjectStorage,
    StorageAccessError,
    StorageConnectionError,
    StorageSizeError,
    StorageTimeoutError,
)


def settings(**kwargs):
    return ContentSettings(database_url="postgresql+asyncpg://u:p@localhost/content_db", rabbitmq_url="amqp://localhost/", elasticsearch_url="http://localhost:9200", object_storage_endpoint="http://localhost:9000", object_storage_access_key="access", object_storage_secret_key="secret", object_storage_bucket="bucket", content_max_body_bytes=5, **kwargs)


def response(chunks, length=None):
    body = MagicMock()
    body.read = AsyncMock(side_effect=[*chunks, b""])
    body.__aenter__ = AsyncMock(return_value=body)
    body.__aexit__ = AsyncMock(return_value=False)
    data = {"Body": body}
    if length is not None:
        data["ContentLength"] = length
    return data, body


@pytest.fixture
def client(monkeypatch):
    s3 = MagicMock()
    manager = MagicMock()
    manager.__aenter__ = AsyncMock(return_value=s3)
    manager.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.client.return_value = manager
    monkeypatch.setattr("crawler_content.storage.aioboto3.Session", lambda: session)
    return s3, manager, session


@pytest.mark.asyncio
async def test_opaque_key_bounded_stream_and_cleanup(client):
    s3, manager, session = client
    data, body = response([b"abc", b"de"], 5)
    s3.get_object = AsyncMock(return_value=data)
    key = "https://elsewhere.test/../../opaque?secret=1"
    async with ObjectStorage(settings()) as storage:
        assert await storage.read(key) == b"abcde"
        s3.get_object.assert_awaited_once_with(Bucket="bucket", Key=key)
    body.__aexit__.assert_awaited_once()
    manager.__aexit__.assert_awaited_once()
    config = session.client.call_args.kwargs["config"]
    assert config.signature_version == "s3v4" and config.s3["addressing_style"] == "path"
    assert config.connect_timeout == 5 and config.read_timeout == 15 and config.retries["total_max_attempts"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("chunks,length", [([b"x"], 6), ([b"123", b"456"], None), ([b"123", b"456"], 3)])
async def test_size_limit_closes_body(client, chunks, length):
    s3, _, _ = client
    data, body = response(chunks, length)
    s3.get_object = AsyncMock(return_value=data)
    async with ObjectStorage(settings()) as storage:
        with pytest.raises(StorageSizeError):
            await storage.read("object")
    body.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("code,expected", [("NoSuchKey", ObjectNotFoundError), ("AccessDenied", StorageAccessError)])
async def test_s3_error_codes_are_not_confused(client, code, expected):
    s3, _, _ = client
    s3.get_object = AsyncMock(side_effect=ClientError({"Error": {"Code": code, "Message": "failure"}}, "GetObject"))
    async with ObjectStorage(settings()) as storage:
        with pytest.raises(expected):
            await storage.read("object")


@pytest.mark.asyncio
async def test_connection_error_is_distinct(client):
    s3, _, _ = client
    s3.get_object = AsyncMock(side_effect=EndpointConnectionError(endpoint_url="http://localhost:9000"))
    async with ObjectStorage(settings()) as storage:
        with pytest.raises(StorageConnectionError):
            await storage.read("object")


@pytest.mark.asyncio
async def test_total_deadline_closes_body_and_client(client):
    s3, manager, _ = client
    data, body = response([])
    async def stall(_size):
        await asyncio.sleep(1)
    body.read = AsyncMock(side_effect=stall)
    s3.get_object = AsyncMock(return_value=data)
    async with ObjectStorage(settings(object_storage_total_timeout_seconds=0.01)) as storage:
        with pytest.raises(StorageTimeoutError):
            await storage.read("object")
    body.__aexit__.assert_awaited_once()
    manager.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_read_closes_body_and_propagates(client):
    s3, _, _ = client
    data, body = response([])
    body.read = AsyncMock(side_effect=asyncio.CancelledError())
    s3.get_object = AsyncMock(return_value=data)
    async with ObjectStorage(settings()) as storage:
        with pytest.raises(asyncio.CancelledError):
            await storage.read("object")
    body.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_s3_error_is_not_a_missing_key(client):
    s3, _, _ = client
    error = ClientError({"Error": {"Code": "NoSuchBucket", "Message": "failure"}}, "GetObject")
    s3.get_object = AsyncMock(side_effect=error)
    async with ObjectStorage(settings()) as storage:
        with pytest.raises(ClientError) as caught:
            await storage.read("object")
    assert caught.value is error


@pytest.mark.asyncio
async def test_read_failure_closes_response(client):
    from botocore.exceptions import ResponseStreamingError

    s3, _, _ = client
    data, body = response([])
    body.read = AsyncMock(side_effect=ResponseStreamingError(error="interrupted"))
    s3.get_object = AsyncMock(return_value=data)
    async with ObjectStorage(settings()) as storage:
        with pytest.raises(StorageConnectionError):
            await storage.read("object")
    body.__aexit__.assert_awaited_once()
