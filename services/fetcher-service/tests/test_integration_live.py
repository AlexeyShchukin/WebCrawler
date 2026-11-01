"""Opt-in checks against isolated real RabbitMQ, Redis and MinIO services."""

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from crawler_contracts import FetchUrlEvent
from dotenv import dotenv_values
from redis.asyncio import Redis

from crawler_fetcher.lease_client import LeaseGrant, LeaseLostError
from crawler_fetcher.messaging import FetcherPublisher
from crawler_fetcher.network import FetchedResponse
from crawler_fetcher.origin_policy import OriginDeferred, OriginPolicy
from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.storage import ObjectStorage
from crawler_fetcher.worker import FetcherWorker

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(os.getenv("FETCHER_LIVE_INTEGRATION") != "1", reason="requires isolated live services"),
]


def live_settings():
    env = dotenv_values(Path(__file__).resolve().parents[3] / ".env.example")
    return FetcherSettings(
        rabbitmq_user=env["RABBITMQ_USER"], rabbitmq_password=env["RABBITMQ_PASSWORD"],
        rabbitmq_host="127.0.0.1", rabbitmq_port=int(os.getenv("LIVE_RABBIT_PORT", "5672")),
        redis_password=env["REDIS_PASSWORD"], redis_host="127.0.0.1",
        redis_port=int(os.getenv("LIVE_REDIS_PORT", "6379")),
        object_storage_endpoint=f"http://127.0.0.1:{os.getenv('LIVE_MINIO_PORT', '9000')}",
        object_storage_access_key=env["MINIO_ROOT_USER"],
        object_storage_secret_key=env["MINIO_ROOT_PASSWORD"],
        object_storage_bucket=env.get("OBJECT_STORAGE_BUCKET") or "crawler-raw-content",
        frontier_internal_url=os.getenv("LIVE_FRONTIER_URL", "http://127.0.0.1:18000"),
        origin_requests_per_second=1,
    )


def make_event():
    return FetchUrlEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1,
                         url=f"https://{uuid4().hex}.example/", depth=0)


async def read_object(storage, key):
    obj = await storage._client.get_object(Bucket=storage._settings.object_storage_bucket, Key=key)
    return await obj["Body"].read(), obj["ContentType"]


async def test_minio_concurrent_conditional_writes_allow_only_one_winner():
    settings = live_settings()
    key = f"integration/conditional-write/{uuid4()}"
    bodies = [f"writer-{i}".encode() for i in range(12)]
    start = asyncio.Event()
    async with ObjectStorage(settings) as storage:
        async def write(body):
            await start.wait()
            try:
                await storage._client.put_object(
                    Bucket=settings.object_storage_bucket, Key=key, Body=body,
                    ContentType="text/html", IfNoneMatch="*",
                )
                return body, None
            except ClientError as error:
                return body, error.response["ResponseMetadata"]["HTTPStatusCode"]

        try:
            tasks = [asyncio.create_task(write(body)) for body in bodies]
            await asyncio.sleep(0)
            start.set()
            results = await asyncio.gather(*tasks)
            winners = [body for body, status in results if status is None]
            failures = [status for _, status in results if status is not None]
            assert len(winners) == 1, results
            assert len(failures) == len(bodies) - 1
            assert set(failures) <= {409, 412}, results
            assert await read_object(storage, key) == (winners[0], "text/html")
        finally:
            await storage._client.delete_object(Bucket=settings.object_storage_bucket, Key=key)


async def test_minio_conditional_snapshots_are_immutable():
    settings, event = live_settings(), make_event()
    async with ObjectStorage(settings) as storage:
        keys = []
        try:
            first, duplicate = await asyncio.gather(
                storage.put_for_event(event, b"first", "text/html"),
                storage.put_for_event(event, b"first", "text/html"),
            )
            keys.append(first)
            changed = await storage.put_for_event(event, b"changed", "text/html")
            keys.append(changed)
            assert first == duplicate and first != changed
            assert await read_object(storage, first) == (b"first", "text/html")
            assert await read_object(storage, changed) == (b"changed", "text/html")
        finally:
            for key in keys:
                await storage._client.delete_object(Bucket=settings.object_storage_bucket, Key=key)


async def test_redis_admission_and_circuit_are_shared():
    settings = live_settings()
    redis = Redis.from_url(str(settings.redis_url))
    origin = f"https://{uuid4().hex}.example"
    first, second = OriginPolicy(redis, settings), OriginPolicy(redis, settings)
    try:
        await first.acquire(origin)
        with pytest.raises(OriginDeferred) as deferred:
            await second.acquire(origin)
        assert deferred.value.delay_seconds > 0
        for _ in range(settings.circuit_breaker_failure_threshold):
            await first.record_failure(origin)
        with pytest.raises(OriginDeferred) as cooldown:
            await second.acquire(origin)
        assert cooldown.value.delay_seconds > 50
    finally:
        await redis.delete(first.key_for(origin))
        await redis.aclose()


async def test_redelivery_after_frontier_submit_does_not_refetch_or_publish_stale_result():
    """A lost source ack cannot turn a committed result into a second fetch."""
    settings, event = live_settings(), make_event()
    redis = Redis.from_url(str(settings.redis_url))
    origin = str(event.url).rstrip("/")
    policy = OriginPolicy(redis, settings)
    keys = []
    try:
        async with FetcherPublisher(settings) as publisher, ObjectStorage(settings) as storage:
            worker = FetcherWorker(settings)
            worker.publisher, worker.storage, worker.origin_policy = publisher, storage, policy
            worker.session = worker.resolver = None
            owner_token = uuid4()

            class LeaseClient:
                async def claim(self, **_):
                    return LeaseGrant(owner_token, None)

                async def renew(self, **_):
                    return None

                async def submit(self, *_args, **_kwargs):
                    return None

            worker.lease_client = LeaseClient()
            fetches = 0

            async def fetch(*args, **kwargs):
                nonlocal fetches
                fetches += 1
                return FetchedResponse(str(event.url), 200, "text/html", b"first")

            worker.fetch = fetch

            class FailAck:
                body = event.model_dump_json().encode()

                async def ack(self):
                    raise ConnectionError("simulated crash after publication")

            with pytest.raises(ConnectionError, match="simulated crash"):
                await worker.handle_message(FailAck())
            assert fetches == 1
            key = await storage.put_for_event(event, b"first", "text/html")
            keys.append(key)
            await redis.delete(policy.key_for(origin))
            async def claim_after_redelivery(**_):
                raise LeaseLostError()
            worker.lease_client.claim = claim_after_redelivery
            class Ack:
                body = event.model_dump_json().encode()

                async def ack(self):
                    return None

            redelivery = Ack()
            await worker.handle_message(redelivery)
            assert fetches == 1
            for key in keys:
                await storage._client.delete_object(Bucket=settings.object_storage_bucket, Key=key)
    finally:
        await redis.delete(policy.key_for(origin))
        await redis.aclose()
