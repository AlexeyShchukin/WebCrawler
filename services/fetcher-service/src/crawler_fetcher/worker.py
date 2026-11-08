"""Fetch queue consumer coordinating admission, retrieval and event publication."""

import asyncio
import os
import random
import signal
from contextlib import AsyncExitStack
from typing import TypedDict
from uuid import UUID

import aiohttp
from aio_pika.abc import AbstractIncomingMessage
from crawler_contracts import (
    FetchRetryRequestedEvent,
    FetchUrlEvent,
    PageFailedEvent,
    PageFetchedEvent,
)
from crawler_contracts.events import FailureCategory, FailureStage, FetchRetryCategory
from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry
from yarl import URL

from crawler_fetcher.lease_client import FrontierLeaseClient, LeaseLostError
from crawler_fetcher.messaging import FetcherPublisher
from crawler_fetcher.network import (
    NetworkFailure,
    PublicResolver,
    create_http_session,
    fetch_html,
)
from crawler_fetcher.origin_policy import (
    OriginDeferred,
    OriginPolicy,
    OriginPolicyUnavailable,
)
from crawler_fetcher.outcomes import (
    FetchDisposition,
    ObjectStorageFailure,
    classify_fetch_failure,
    suggest_retry_delay,
)
from crawler_fetcher.settings import FetcherSettings
from crawler_fetcher.storage import ObjectStorage


class FetchIdentity(TypedDict):
    crawl_id: UUID
    url_id: UUID
    fetch_attempt: int


class FetcherWorker:
    """Own Fetcher dependencies and process one delivery at a time per callback."""

    def __init__(self, settings: FetcherSettings) -> None:
        self.settings = settings
        self._stack = AsyncExitStack()
        self.dependencies_ready = False
        self.fetch = fetch_html
        self.sleep = asyncio.sleep
        self._consumer_tag: str | None = None
        self._inflight: set[asyncio.Task[object]] = set()
        self._shutdown_timeout_seconds = 10.0

    def request_shutdown(self) -> None:
        """Stop the process so the deployment can restart a failed consumer."""
        os.kill(os.getpid(), signal.SIGTERM)

    @property
    def ready(self) -> bool:
        return self.dependencies_ready

    async def start(self) -> None:
        try:
            self.publisher = await self._stack.enter_async_context(FetcherPublisher(self.settings))
            self.storage = await self._stack.enter_async_context(ObjectStorage(self.settings))
            redis = Redis.from_url(
                str(self.settings.redis_url),
                socket_connect_timeout=self.settings.redis_connect_timeout_seconds,
                socket_timeout=self.settings.redis_operation_timeout_seconds,
                retry_on_timeout=False,
                retry=Retry(NoBackoff(), 0),
            )
            self._stack.push_async_callback(redis.aclose)
            async with asyncio.timeout(self.settings.redis_operation_timeout_seconds):
                await redis.ping()
            self.origin_policy = OriginPolicy(redis, self.settings)
            self.resolver = PublicResolver(aiohttp.resolver.DefaultResolver())
            self.session = await self._stack.enter_async_context(
                create_http_session(self.settings, self.resolver)
            )
            lease_session = await self._stack.enter_async_context(
                aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
            )
            self.lease_client = FrontierLeaseClient(
                lease_session,
                str(self.settings.frontier_internal_url)
            )
            self._consumer_tag = await self.publisher.queue.consume(self._consume_message)
            self.dependencies_ready = True
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        self.dependencies_ready = False
        try:
            if self._consumer_tag is not None:
                if not self.publisher.channel.is_closed:
                    try:
                        await self.publisher.queue.cancel(self._consumer_tag)
                    except Exception:  # channel may close while cancelling
                        if not self.publisher.channel.is_closed:
                            raise
                self._consumer_tag = None
        finally:
            if self._inflight:
                _, pending = await asyncio.wait(
                    self._inflight,
                    timeout=self._shutdown_timeout_seconds
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
            await self._stack.aclose()

    async def _consume_message(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._inflight.add(task)
        try:
            await self.handle_message(message)
        except Exception:  # noqa: BLE001 - delivery must not remain unacknowledged
            self.dependencies_ready = False
            try:
                await self.publisher.channel.close()
            finally:
                self.request_shutdown()
        finally:
            if task is not None:
                self._inflight.discard(task)

    async def handle_message(self, message: AbstractIncomingMessage) -> None:
        try:
            event = FetchUrlEvent.from_message_json(message.body)
        except (ValueError, TypeError, UnicodeDecodeError):
            await message.reject(requeue=False)
            return

        origin = str(URL(str(event.url)).origin())
        try:
            await self.origin_policy.acquire(origin)
        except (OriginDeferred, OriginPolicyUnavailable):
            await self.publisher.publish(
                event,
                exchange_name='crawler.retry',
                routing_key='fetch.url.retry.5s'
            )
            await message.ack()
            return

        identity: FetchIdentity = {
            'crawl_id': event.crawl_id,
            'url_id': event.url_id,
            'fetch_attempt': event.fetch_attempt
        }
        try:
            grant = await self.lease_client.claim(**identity)
        except LeaseLostError:
            await message.ack()
            return
        stop_heartbeat = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(
            identity,
            grant.owner_token,
            stop_heartbeat
        ))
        operation = asyncio.create_task(self._fetch_and_submit(
                message,
                event,
                identity,
                grant.owner_token,
                origin,
                stop_heartbeat,
                heartbeat
        ))
        try:
            done, _ = await asyncio.wait(
                {operation, heartbeat},
                return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat in done:
                await heartbeat
            await operation
        except LeaseLostError:
            await message.ack()
        finally:
            for task in (operation, heartbeat):
                if not task.done():
                    task.cancel()
            await asyncio.gather(operation, heartbeat, return_exceptions=True)

    async def _heartbeat(
            self,
            identity: FetchIdentity,
            owner_token: UUID,
            stop: asyncio.Event
    ) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.settings.fetch_lease_seconds / 3
                )
                return
            except TimeoutError:
                await self.lease_client.renew(**identity, owner_token=owner_token)

    async def _fetch_and_submit(
            self,
            message: AbstractIncomingMessage,
            event: FetchUrlEvent,
            identity: FetchIdentity,
            owner_token: UUID,
            origin: str,
            stop_heartbeat: asyncio.Event,
            heartbeat: asyncio.Task[None],
    ) -> None:
        async def finish_heartbeat() -> None:
            stop_heartbeat.set()
            await heartbeat
        async def admit_redirect(target_origin: str) -> None:
            nonlocal origin
            while True:
                try:
                    await self.origin_policy.acquire(target_origin)
                    break
                except OriginDeferred as deferred:
                    await self.sleep(max(0.01, deferred.delay_seconds))
            origin = target_origin

        try:
            response = await self.fetch(
                self.session,
                str(event.url),
                max_body_bytes=self.settings.http_max_body_bytes,
                resolver=self.resolver,
                admit_redirect=admit_redirect,
                chain_timeout_seconds=self.settings.http_chain_timeout_seconds,
            )
        except (OriginDeferred, OriginPolicyUnavailable):
            outcome = await self._failure(
                event,
                origin,
                error=NetworkFailure('connection_failed'),
                detail='origin_policy_unavailable',
                record_origin=False
            )
            await self.lease_client.submit(outcome, owner_token=owner_token)
            await finish_heartbeat()
            await message.ack()
            return
        except NetworkFailure as error:
            outcome = await self._failure(event, origin, error=error, detail=error.kind)
            await self.lease_client.submit(outcome, owner_token=owner_token)
            await finish_heartbeat()
            await message.ack()
            return

        if response.status >= 400:
            outcome = await self._failure(
                event,
                origin,
                http_status=response.status,
                retry_after=response.retry_after,
                detail=f'HTTP {response.status}'
            )
            await self.lease_client.submit(outcome, owner_token=owner_token)
            await finish_heartbeat()
            await message.ack()
            return

        try:
            content_ref = await self.storage.put_for_event(
                event,
                response.body,
                response.content_type
            )
        except Exception:  # noqa: BLE001 - storage SDK errors vary by backend
            outcome = await self._failure(
                event,
                origin,
                error=ObjectStorageFailure(),
                detail='object_storage_failure'
            )
            await self.lease_client.submit(outcome, owner_token=owner_token)
            await finish_heartbeat()
            await message.ack()
            return

        outcome = PageFetchedEvent(
            **identity,
            final_url=response.final_url,
            http_status=response.status,
            content_type=response.content_type,
            content_ref=content_ref
        )
        await self.lease_client.submit(outcome, owner_token=owner_token)
        try:
            await self.origin_policy.record_success(origin)
        except OriginPolicyUnavailable:
            pass
        await finish_heartbeat()
        await message.ack()

    async def _failure(
            self,
            event: FetchUrlEvent,
            origin: str,
            *,
            detail: str,
            error: Exception | None = None,
            http_status: int | None = None,
            retry_after: str | None = None,
            record_origin: bool = True
    ) -> PageFailedEvent | FetchRetryRequestedEvent:

        outcome = classify_fetch_failure(error, http_status=http_status)
        delay = suggest_retry_delay(event.fetch_attempt, retry_after, random.random)
        if record_origin:
            try:
                if http_status == 429:
                    await self.origin_policy.record_rate_limit(origin, delay)
                elif (http_status is not None and 500 <= http_status <= 599) or (
                    isinstance(error, NetworkFailure) and error.kind in {
                        'timeout', 'connection_failed', 'dns_failure',
                    }
                ):
                    await self.origin_policy.record_failure(origin)
            except OriginPolicyUnavailable:
                pass
        identity = {
            'crawl_id': event.crawl_id,
            'url_id': event.url_id,
            'fetch_attempt': event.fetch_attempt
        }
        if outcome.disposition == FetchDisposition.RETRY_REQUESTED:
            assert isinstance(outcome.category, FetchRetryCategory)
            result = FetchRetryRequestedEvent(
                **identity, category=outcome.category, detail=detail[:1000],
                suggested_delay_seconds=delay,
            )
        else:
            assert isinstance(outcome.category, FailureCategory)
            result = PageFailedEvent(
                **identity,
                stage=FailureStage.FETCH,
                category=outcome.category,
                detail=detail[:1000]
            )
        return result

