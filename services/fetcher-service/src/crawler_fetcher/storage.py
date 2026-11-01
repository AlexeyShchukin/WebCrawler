"""Managed S3 uploads under deterministic fetch-attempt keys."""

from contextlib import AsyncExitStack
from hashlib import sha256
from types import TracebackType
from typing import TYPE_CHECKING, Self

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
from crawler_contracts import FetchUrlEvent

from crawler_fetcher.settings import FetcherSettings

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client


def fetch_prefix(event: FetchUrlEvent) -> str:
    """Build the storage prefix for a fetch attempt."""
    return (
        f"crawls/{event.crawl_id}/urls/{event.url_id}/"
        f"fetches/{event.fetch_attempt}"
    )


class ObjectStorage:
    """Own one S3 client for the worker lifetime."""

    def __init__(self, settings: FetcherSettings) -> None:
        self._settings = settings
        self._stack = AsyncExitStack()
        self._client: S3Client

    async def __aenter__(self) -> Self:
        session = aioboto3.Session()
        self._client = await self._stack.enter_async_context(session.client(
            "s3",
            endpoint_url=str(self._settings.object_storage_endpoint),
            aws_access_key_id=self._settings.object_storage_access_key.get_secret_value(),
            aws_secret_access_key=self._settings.object_storage_secret_key.get_secret_value(),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        ))
        return self

    async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
    ) -> None:
        await self._stack.__aexit__(exc_type, exc_value, traceback)

    async def put_for_event(
        self, event: FetchUrlEvent, body: bytes, content_type: str,
    ) -> str:
        """Store an immutable snapshot, including its content type in its identity."""
        encoded_type = content_type.encode("utf-8")
        digest = sha256(len(encoded_type).to_bytes(8, "big") + encoded_type + body).hexdigest()
        key = f"{fetch_prefix(event)}/{digest}.html"
        try:
            await self._client.put_object(
                Bucket=self._settings.object_storage_bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise
        return key
