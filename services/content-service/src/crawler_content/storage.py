"""Bounded reads of opaque object keys from the configured S3 bucket."""
import asyncio
from contextlib import AsyncExitStack
from types import TracebackType
from typing import Any, Self

import aioboto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
    ResponseStreamingError,
)

from crawler_content.settings import ContentSettings


class ObjectNotFoundError(Exception):
    """The storage service confirmed that the requested key does not exist."""


class StorageAccessError(Exception):
    """Object storage denied access."""


class StorageConnectionError(Exception):
    """A connection or streaming operation failed."""


class StorageTimeoutError(Exception):
    """A storage operation exceeded its deadline."""


class StorageSizeError(Exception):
    """The object exceeds the configured maximum body size."""


class ObjectStorage:
    """Own one S3 client and close every response body after reading."""

    def __init__(self, settings: ContentSettings) -> None:
        self._settings = settings
        self._stack = AsyncExitStack()
        self._client: Any = None

    async def __aenter__(self) -> Self:
        config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=self._settings.object_storage_connect_timeout_seconds,
            read_timeout=self._settings.object_storage_read_timeout_seconds,
            retries={"total_max_attempts": 1},
        )
        self._client = await self._stack.enter_async_context(aioboto3.Session().client(
            "s3",
            endpoint_url=self._settings.object_storage_endpoint,
            aws_access_key_id=self._settings.object_storage_access_key.get_secret_value(),
            aws_secret_access_key=self._settings.object_storage_secret_key.get_secret_value(),
            config=config,
        ))
        return self

    async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None
    ) -> None:
        try:
            await self._stack.__aexit__(exc_type, exc, tb)
        finally:
            self._client = None

    async def read(self, content_ref: str) -> bytes:
        """Read a key from the configured bucket within byte and time limits."""
        if self._client is None:
            raise RuntimeError("Object storage client is not open")
        try:
            async with asyncio.timeout(self._settings.object_storage_total_timeout_seconds):
                obj = await self._client.get_object(
                    Bucket=self._settings.object_storage_bucket,
                    Key=content_ref
                )
                body = obj["Body"]
                async with body:
                    limit = self._settings.content_max_body_bytes
                    length = obj.get("ContentLength")
                    if length is not None and length > limit:
                        raise StorageSizeError("Object exceeds configured size limit")
                    data = bytearray()
                    while True:
                        chunk = await body.read(min(65536, limit - len(data) + 1))
                        if not chunk:
                            return bytes(data)
                        data.extend(chunk)
                        if len(data) > limit:
                            raise StorageSizeError("Object exceeds configured size limit")
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code == "NoSuchKey":
                raise ObjectNotFoundError("Object does not exist") from error
            if code == "AccessDenied":
                raise StorageAccessError("Object storage access denied") from error
            raise
        except (TimeoutError, ConnectTimeoutError, ReadTimeoutError) as error:
            raise StorageTimeoutError("Object storage request timed out") from error
        except (EndpointConnectionError, ConnectionClosedError, ResponseStreamingError) as error:
            raise StorageConnectionError("Object storage connection failed") from error
