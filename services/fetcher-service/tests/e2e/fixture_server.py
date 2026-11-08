"""Controlled origin, real Frontier proxy and transparent AMQP relay for e2e tests.

POST a unique /control/cases/{id}, bind its crawl_id at /bind, and release the
origin, submit, and response gates at /release/{gate}. GET reports observations;
DELETE releases all gates and forgets the case. Only successful submit responses
are held; every claim/renew/submit decision comes from the actual Frontier API.
The TCP relay observes per-delivery ACKs while forwarding the original frames.
"""

import asyncio
import json
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import aiohttp
from aiohttp import web
from amqp_observer import AMQPObserver


@dataclass
class Case:
    body: bytes
    content_type: str
    crawl_id: str | None = None
    origin_requests: int = 0
    origin: asyncio.Event = field(default_factory=asyncio.Event)
    submit: asyncio.Event = field(default_factory=asyncio.Event)
    response: asyncio.Event = field(default_factory=asyncio.Event)


CASES = web.AppKey("cases", dict)
OBSERVATIONS = web.AppKey("observations", list)
SESSION = web.AppKey("session", aiohttp.ClientSession)
DELIVERIES = web.AppKey("amqp_deliveries", list)
UPSTREAM = os.getenv("E2E_FRONTIER_UPSTREAM_URL", "http://frontier:8000").rstrip("/")
HOP_HEADERS = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "content-length",
})


async def client_context(app: web.Application) -> AsyncIterator[None]:
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10), auto_decompress=False,
    ) as session:
        app[SESSION] = session
        yield


async def broker_proxy_context(app: web.Application) -> AsyncIterator[None]:
    connections = set()

    async def connected(client_reader, client_writer):
        task = asyncio.current_task()
        connections.add(task)
        broker_writer = None
        pumps = set()
        try:
            broker_reader, broker_writer = await asyncio.open_connection("rabbitmq", 5672)
            observer = AMQPObserver(app[DELIVERIES])

            async def relay(reader, writer, *, from_broker):
                while data := await reader.read(64 * 1024):
                    observer.feed(data, from_broker=from_broker)
                    writer.write(data)
                    await writer.drain()

            pumps = {
                asyncio.create_task(relay(client_reader, broker_writer, from_broker=False)),
                asyncio.create_task(relay(broker_reader, client_writer, from_broker=True)),
            }
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                finished.result()
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            writers = [writer for writer in (client_writer, broker_writer) if writer is not None]
            for writer in writers:
                writer.close()
            await asyncio.gather(*(writer.wait_closed() for writer in writers), return_exceptions=True)
            connections.discard(task)

    server = await asyncio.start_server(connected, "0.0.0.0", 5673)
    try:
        yield
    finally:
        server.close()
        await server.wait_closed()
        for task in list(connections):
            task.cancel()
        await asyncio.gather(*connections, return_exceptions=True)


def get_case(request: web.Request) -> Case:
    try:
        return request.app[CASES][request.match_info["case_id"]]
    except KeyError as error:
        raise web.HTTPNotFound(text="Unknown test case") from error


async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ready"})


async def create_case(request: web.Request) -> web.Response:
    case_id = request.match_info["case_id"]
    if case_id in request.app[CASES]:
        raise web.HTTPConflict(text="Test case already registered")
    payload = await request.json()
    if payload.get("content_type") not in {"text/html", "application/xhtml+xml"}:
        raise web.HTTPBadRequest(text="Expected HTML or XHTML MIME type")
    if not isinstance(payload.get("body"), str):
        raise web.HTTPBadRequest(text="Expected UTF-8 body text")
    request.app[CASES][case_id] = Case(
        body=payload["body"].encode("utf-8"), content_type=payload["content_type"],
    )
    return web.json_response({"case_id": case_id}, status=201)


async def bind_case(request: web.Request) -> web.Response:
    case = get_case(request)
    payload = await request.json()
    if not isinstance(payload.get("crawl_id"), str):
        raise web.HTTPBadRequest(text="Expected crawl_id")
    case.crawl_id = payload["crawl_id"]
    return web.Response(status=204)


async def inspect_case(request: web.Request) -> web.Response:
    case = get_case(request)
    observations = [record for record in request.app[OBSERVATIONS]
                    if record["payload"].get("crawl_id") == case.crawl_id]
    return web.json_response({
        "crawl_id": case.crawl_id,
        "origin_requests": case.origin_requests,
        "claims": [record for record in observations if record["operation"] == "claim"],
        "renewals": [record for record in observations if record["operation"] == "renew"],
        "submissions": [record for record in observations if record["operation"] == "submit"],
        "amqp_deliveries": [record for record in request.app[DELIVERIES]
                            if record["payload"].get("crawl_id") == case.crawl_id],
    })


async def release_gate(request: web.Request) -> web.Response:
    case = get_case(request)
    gate = request.match_info["gate"]
    if gate not in {"origin", "submit", "response"}:
        raise web.HTTPNotFound(text="Unknown gate")
    getattr(case, gate).set()
    return web.Response(status=204)


async def delete_case(request: web.Request) -> web.Response:
    case = get_case(request)
    for gate in (case.origin, case.submit, case.response):
        gate.set()
    del request.app[CASES][request.match_info["case_id"]]
    request.app[OBSERVATIONS][:] = [record for record in request.app[OBSERVATIONS]
                                  if record["payload"].get("crawl_id") != case.crawl_id]
    request.app[DELIVERIES][:] = [record for record in request.app[DELIVERIES]
                                if record["payload"].get("crawl_id") != case.crawl_id]
    return web.Response(status=204)


async def origin(request: web.Request) -> web.Response:
    case = get_case(request)
    case.origin_requests += 1
    await case.origin.wait()
    return web.Response(body=case.body, content_type=case.content_type)


async def lease_proxy(request: web.Request) -> web.Response:
    operation = request.match_info["operation"]
    raw = await request.read()
    payload = json.loads(raw)
    case = next((case for case in request.app[CASES].values()
                 if case.crawl_id == payload.get("crawl_id")), None)
    record = {
        "operation": operation, "payload": payload, "forwarded": False,
        "upstream_status": None, "response_released": False,
    }
    request.app[OBSERVATIONS].append(record)
    if operation == "submit" and case is not None:
        await case.submit.wait()
    record["forwarded"] = True
    async with request.app[SESSION].post(
        f"{UPSTREAM}{request.path}", data=raw,
        headers={"Content-Type": request.headers.get("Content-Type", "application/json")},
        allow_redirects=False,
    ) as response:
        body = await response.read()
        status = response.status
        headers = {key: value for key, value in response.headers.items()
                   if key.lower() not in HOP_HEADERS}
    record["upstream_status"] = status
    if operation == "submit" and status == 204 and case is not None:
        await case.response.wait()
    record["response_released"] = True
    return web.Response(status=status, headers=headers, body=body)


def create_app() -> web.Application:
    app = web.Application()
    app[CASES] = {}
    app[OBSERVATIONS] = []
    app[DELIVERIES] = []
    app.cleanup_ctx.append(client_context)
    app.cleanup_ctx.append(broker_proxy_context)
    app.add_routes([
        web.get("/health", health),
        web.post("/control/cases/{case_id}", create_case),
        web.post("/control/cases/{case_id}/bind", bind_case),
        web.get("/control/cases/{case_id}", inspect_case),
        web.post("/control/cases/{case_id}/release/{gate}", release_gate),
        web.delete("/control/cases/{case_id}", delete_case),
        web.get("/pages/{case_id}/page.{extension:html|xhtml}", origin),
        web.post("/internal/fetch-leases/{operation:claim|renew|submit}", lease_proxy),
    ])
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=8080)
