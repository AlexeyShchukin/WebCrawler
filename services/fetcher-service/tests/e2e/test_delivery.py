"""Real broker delivery -> Frontier lease -> HTTP -> MinIO -> submit -> broker ACK.

Run only against the isolated e2e Compose stack with FETCHER_E2E=1. The fixture
holds the origin response, submit forwarding, and Frontier's successful response
in turn. No lease decision or source-delivery acknowledgement is simulated.
"""

import asyncio
import json
import os
from contextlib import AsyncExitStack
from pathlib import Path
from uuid import uuid4

import aioboto3
import aiohttp
import pytest
from aio_pika import DeliveryMode, Message, connect_robust
from botocore.config import Config
from crawler_contracts import FetchUrlEvent, PageFetchedEvent
from dotenv import dotenv_values
from pamqp.commands import Basic

from crawler_fetcher.settings import FetcherSettings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_infra,
    pytest.mark.asyncio,
    pytest.mark.skipif(os.getenv("FETCHER_E2E") != "1", reason="requires isolated e2e stack"),
]


def live_settings():
    env = dotenv_values(Path(__file__).resolve().parents[4] / ".env.example")
    return FetcherSettings(
        rabbitmq_user=env["RABBITMQ_USER"], rabbitmq_password=env["RABBITMQ_PASSWORD"],
        rabbitmq_host="127.0.0.1", rabbitmq_port=int(os.getenv("LIVE_RABBIT_PORT", "19867")),
        redis_password=env["REDIS_PASSWORD"], redis_host="127.0.0.1",
        object_storage_endpoint=f"http://127.0.0.1:{os.getenv('LIVE_MINIO_PORT', '19900')}",
        object_storage_access_key=env["MINIO_ROOT_USER"],
        object_storage_secret_key=env["MINIO_ROOT_PASSWORD"],
        object_storage_bucket=env.get("OBJECT_STORAGE_BUCKET") or "crawler-raw-content",
        frontier_internal_url=os.getenv("LIVE_FRONTIER_URL", "http://127.0.0.1:19880"),
    )


async def eventually(probe, predicate, description, *, timeout=20):
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        last = await probe()
        if predicate(last):
            return last
        await asyncio.sleep(0.05)
    pytest.fail(f"Timed out waiting for {description}; last observation: {last!r}")


async def request_json(session, method, url, *, status=200, **kwargs):
    async with session.request(method, url, **kwargs) as response:
        body = await response.text()
        assert response.status == status, (method, url, response.status, body)
        return await response.json() if body else None


async def compose_command(*arguments, timeout=10):
    """Operate only on the dedicated test project, with bounded CLI cleanup."""
    root = Path(__file__).resolve().parents[4]
    process = await asyncio.create_subprocess_exec(
        "docker", "compose", "--project-name", "fetcher-audit-e2e",
        "--env-file", str(root / ".env.example"),
        "-f", str(Path(__file__).with_name("compose.yml")), *arguments,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(timeout):
            stdout, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode(errors="replace")
        return stdout
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def broker_queue_totals():
    """Read queue state directly: management's coarse totals can lag ACK stats."""
    stdout = await compose_command(
        "exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues", "--formatter=json",
        "name", "messages_ready", "messages_unacknowledged", "consumers",
    )
    return next(item for item in json.loads(stdout) if item["name"] == "fetch.url.queue")


async def assert_delivery_held(state):
    totals = await broker_queue_totals()
    assert totals["messages_ready"] == 0 and totals["messages_unacknowledged"] == 1, totals
    deliveries = (await state())["amqp_deliveries"]
    assert len(deliveries) == 1, deliveries
    assert deliveries[0]["ack_count"] == 0 and deliveries[0]["settlement"] is None


async def test_compose_commands_ignore_inherited_project_name(monkeypatch):
    # A parent shell must never redirect stop/start/down at another deployment.
    # `config` exercises actual Compose precedence without mutating containers.
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "unrelated-deployment")
    configured = json.loads(await compose_command("config", "--format", "json"))
    assert configured["name"] == "fetcher-audit-e2e"


@pytest.mark.parametrize(
    ("extension", "content_type", "body", "lose_ack"),
    [
        ("html", "text/html", b"<!doctype html><html><body>Real HTML delivery</body></html>", False),
        (
            "xhtml", "application/xhtml+xml",
            (b'<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml">'
             b"<body>Real XHTML delivery</body></html>"), False,
        ),
        ("html", "text/html", b"<html><body>Lost source ACK</body></html>", True),
    ],
    ids=["html", "xhtml", "lost-ack-redelivery"],
)
async def test_real_delivery_persists_before_submit_and_acks_after_acceptance(
    extension, content_type, body, lose_ack,
):
    settings = live_settings()
    case_id = uuid4().hex
    control = os.getenv("E2E_CONTROL_URL", "http://127.0.0.1:19889").rstrip("/")
    case_url = f"{control}/control/cases/{case_id}"
    frontier = str(settings.frontier_internal_url).rstrip("/")
    origin_url = f"http://93.184.216.3:8080/pages/{case_id}/page.{extension}"
    bucket = settings.object_storage_bucket
    prefix = None

    async with AsyncExitStack() as stack:
        http = await stack.enter_async_context(
            aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
        )
        connection = await connect_robust(str(settings.rabbitmq_url), timeout=10)
        stack.push_async_callback(connection.close)
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        exchange = await channel.get_exchange("crawler.topic", ensure=True)
        storage = await stack.enter_async_context(aioboto3.Session().client(
            "s3", endpoint_url=str(settings.object_storage_endpoint),
            aws_access_key_id=settings.object_storage_access_key.get_secret_value(),
            aws_secret_access_key=settings.object_storage_secret_key.get_secret_value(),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        ))
        captured_fetches = []
        captured_outcomes = []

        async def capture_fetch(message):
            captured_fetches.append(message.body)

        async def capture_outcome(message):
            captured_outcomes.append(message.body)

        for routing_key, callback in (
            ("fetch.url", capture_fetch), ("page.fetched", capture_outcome),
        ):
            queue = await channel.declare_queue(
                f"fetcher.e2e.{case_id}.{routing_key}", exclusive=True, auto_delete=True,
            )
            await queue.bind(exchange, routing_key=routing_key)
            await queue.consume(callback, no_ack=True)

        async def state():
            return await request_json(http, "GET", case_url)

        async def release(gate):
            await request_json(http, "POST", f"{case_url}/release/{gate}", status=204)

        async def crawl_status():
            return await request_json(http, "GET", f"{frontier}/internal/crawls/{crawl_id}")

        async def objects():
            response = await storage.list_objects_v2(Bucket=bucket, Prefix=prefix)
            assert not response.get("IsTruncated"), response
            return response.get("Contents", [])

        await request_json(http, "POST", case_url, status=201, json={
            "body": body.decode("utf-8"), "content_type": content_type,
        })
        try:
            await eventually(
                broker_queue_totals,
                lambda value: value["consumers"] == 1
                and value["messages_ready"] == value["messages_unacknowledged"] == 0,
                "one real Fetcher consumer with an idle source queue",
            )
            created = await request_json(
                http, "POST", f"{frontier}/internal/crawls", status=201,
                json={"seed_urls": [origin_url]},
            )
            crawl_id = created["crawl_id"]
            prefix = f"crawls/{crawl_id}/"
            await request_json(http, "POST", f"{case_url}/bind", status=204,
                               json={"crawl_id": crawl_id})

            async def fetch_events():
                return [event for raw in captured_fetches
                        if str((event := FetchUrlEvent.from_message_json(raw)).crawl_id) == crawl_id]

            async def outcomes():
                return [event for raw in captured_outcomes
                        if str((event := PageFetchedEvent.from_message_json(raw)).crawl_id) == crawl_id]

            fetch_event = (await eventually(fetch_events, bool, "Frontier fetch.url outbox event"))[0]
            assert str(fetch_event.url) == origin_url
            assert fetch_event.fetch_attempt == 1
            prefix = f"crawls/{crawl_id}/urls/{fetch_event.url_id}/fetches/1/"
            snapshot = await eventually(state, lambda value: value["origin_requests"] == 1,
                                        "real HTTP request reaching held origin")
            assert [claim["upstream_status"] for claim in snapshot["claims"]] == [200]
            identity = {
                "crawl_id": crawl_id, "url_id": str(fetch_event.url_id), "fetch_attempt": 1,
            }
            claim = snapshot["claims"][0]["payload"]
            assert {key: claim[key] for key in identity} == identity
            fetching = await crawl_status()
            assert fetching["url_counts"]["fetching"] == 1
            assert fetching["url_counts"]["downloaded"] == 0
            assert await objects() == []
            await assert_delivery_held(state)
            assert (await state())["amqp_deliveries"][0]["payload"] == fetch_event.model_dump(mode="json")

            # Both submit gates share one budget below the production lease
            # client's 10-second request timeout; individual probe limits alone
            # must not accidentally trigger a client retry during acceptance.
            async with asyncio.timeout(8):
                await release("origin")
                snapshot = await eventually(state, lambda value: len(value["submissions"]) == 1,
                                            "submit waiting before real Frontier", timeout=5)
                submitted = snapshot["submissions"][0]
                assert not submitted["forwarded"]
                assert submitted["payload"]["owner_token"] == claim["owner_token"]
                outcome = PageFetchedEvent.model_validate(submitted["payload"]["event"])
                assert outcome.crawl_id == fetch_event.crawl_id
                assert outcome.url_id == fetch_event.url_id
                assert outcome.fetch_attempt == 1
                assert outcome.content_type == content_type
                assert outcome.http_status == 200
                assert str(outcome.final_url) == origin_url
                assert outcome.content_ref.startswith(prefix)
                assert body.decode() not in outcome.model_dump_json()
                stored = await storage.get_object(Bucket=bucket, Key=outcome.content_ref)
                async with stored["Body"] as stream:
                    assert await stream.read() == body
                assert stored["ContentType"] == content_type
                stored_objects = await objects()
                assert [item["Key"] for item in stored_objects] == [outcome.content_ref]
                assert (await crawl_status())["url_counts"]["fetching"] == 1
                assert await outcomes() == []
                await assert_delivery_held(state)

                await release("submit")
                snapshot = await eventually(
                    state, lambda value: value["submissions"][0]["upstream_status"] is not None,
                    "real Frontier submit response", timeout=5,
                )
                assert snapshot["submissions"][0]["upstream_status"] == 204
                assert not snapshot["submissions"][0]["response_released"]
                downloaded = await crawl_status()
                assert downloaded["url_counts"]["downloaded"] == 1
                assert downloaded["url_counts"]["fetching"] == 0
                assert downloaded["url_counts"]["fetched"] == 0
                accepted = await eventually(
                    outcomes, bool, "accepted page.fetched outbox event", timeout=5)
                assert accepted == [outcome]
                await assert_delivery_held(state)

                if not lose_ack:
                    await release("response")
            if lose_ack:
                # Let normal shutdown cancel the still-unacknowledged callback
                # after its drain deadline. Closing the actual AMQP connection
                # must return the original delivery to the broker, without any
                # test republish or replacement of IncomingMessage.ack.
                try:
                    await compose_command("stop", "fetcher", timeout=35)
                    totals = await broker_queue_totals()
                    assert totals["messages_ready"] == 1, totals
                    assert totals["messages_unacknowledged"] == 0, totals
                    assert (await state())["amqp_deliveries"][0]["ack_count"] == 0
                finally:
                    try:
                        await release("response")
                    finally:
                        await compose_command("start", "fetcher")
            else:
                await eventually(
                    state,
                    lambda value: value["amqp_deliveries"][0]["ack_count"] == 1,
                    "actual source ACK after accepted submit response",
                )
                totals = await broker_queue_totals()
                assert totals["messages_ready"] == totals["messages_unacknowledged"] == 0, totals
                confirmation = await exchange.publish(
                    Message(
                        fetch_event.model_dump_json().encode(), content_type="application/json",
                        message_id=str(fetch_event.event_id), delivery_mode=DeliveryMode.PERSISTENT,
                    ), routing_key="fetch.url", mandatory=True, timeout=5,
                )
                assert isinstance(confirmation, Basic.Ack)
            snapshot = await eventually(
                state, lambda value: len(value["claims"]) == 2
                and value["claims"][-1]["upstream_status"] is not None,
                "real Frontier rejecting duplicate delivery claim",
            )
            assert [item["upstream_status"] for item in snapshot["claims"]] == [200, 409]
            snapshot = await eventually(
                state,
                lambda value: len(value["amqp_deliveries"]) >= 2
                and all(item["ack_count"] == 1 for item in value["amqp_deliveries"][1:]),
                "actual ACK of duplicate source delivery",
            )
            deliveries = snapshot["amqp_deliveries"]
            assert all(item["payload"] == fetch_event.model_dump(mode="json") for item in deliveries)
            if lose_ack:
                assert deliveries[0]["ack_count"] == 0
                redeliveries = [item for item in deliveries[1:] if item["redelivered"]]
                assert redeliveries
                assert all(item["connection_id"] != deliveries[0]["connection_id"]
                           for item in redeliveries)
            else:
                assert len(deliveries) == 2
                assert deliveries[0]["ack_count"] == 1
            totals = await broker_queue_totals()
            assert totals["messages_ready"] == totals["messages_unacknowledged"] == 0, totals
            snapshot = await state()
            assert snapshot["origin_requests"] == 1
            if lose_ack:
                # A lost submit response may itself be retried before shutdown;
                # every retry must carry exactly the same event and owner.
                assert all(item["payload"] == submitted["payload"]
                           and item["upstream_status"] == 204 for item in snapshot["submissions"])
            else:
                assert len(snapshot["submissions"]) == 1
            assert await objects() == stored_objects
            assert await outcomes() == [outcome]
            assert await crawl_status() == downloaded
        finally:
            # Release gates on failures too; delete only this case's object prefix.
            for gate in ("origin", "submit", "response"):
                await release(gate)
            if prefix is not None:
                for item in await objects():
                    await storage.delete_object(Bucket=bucket, Key=item["Key"])
            await request_json(http, "DELETE", case_url, status=204)
