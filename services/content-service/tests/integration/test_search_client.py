"""Elasticsearch 8.17 integration tests using isolated disposable indices."""

import asyncio
import os
import re
from uuid import uuid4

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch, BadRequestError, ConnectionTimeout

from crawler_content.search.client import IncompatibleMappingError, SearchClient
from crawler_content.search.mapping import search_index_mapping

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def isolated_search_index():
    index = "content-test-" + uuid4().hex
    endpoint = os.environ.get("CONTENT_TEST_ELASTICSEARCH_URL", "http://127.0.0.1:9200")
    client = AsyncElasticsearch(endpoint, request_timeout=10, max_retries=0)
    try:
        yield client, index
    finally:
        try:
            if not re.fullmatch(r"content-test-[0-9a-f]{32}", index):
                raise RuntimeError("Refusing to delete an unverified test index")
            await client.options(ignore_status=404).indices.delete(index=index)
        finally:
            await client.close()


async def test_create_repeat_and_document_round_trip(isolated_search_index):
    client, index = isolated_search_index
    version = await client.info()
    assert version["version"]["number"] == "8.17.0"
    search = SearchClient(client, index)
    await search.ensure_index()
    await search.ensure_index()
    page_id, crawl_id = str(uuid4()), str(uuid4())
    url = "https://example.test/" + "😀" * 8192
    document = {"page_id": page_id, "crawl_id": crawl_id, "url": url,
                "title": "Example page", "text": "searchable words", "keywords": ["searchable"]}
    await client.index(index=index, id=page_id, document=document, refresh="wait_for")
    assert (await client.get(index=index, id=page_id))["_source"] == document
    assert (await client.search(index=index, query={"match": {"text": "searchable"}}))["hits"]["total"]["value"] == 1
    assert (await client.search(index=index, query={"exists": {"field": "url"}}))["hits"]["total"]["value"] == 0
    with pytest.raises(BadRequestError):
        await client.index(index=index, id=str(uuid4()), document={**document, "html": "raw"})


async def test_existing_incompatible_index_is_not_recreated(isolated_search_index):
    client, index = isolated_search_index
    mapping = search_index_mapping()
    mapping["properties"]["title"]["type"] = "keyword"
    del mapping["properties"]["title"]["analyzer"]
    await client.indices.create(index=index, mappings=mapping)
    page_id = str(uuid4())
    await client.index(index=index, id=page_id, document={"title": "preserve"})
    with pytest.raises(IncompatibleMappingError):
        await SearchClient(client, index).ensure_index()
    assert (await client.get(index=index, id=page_id))["_source"]["title"] == "preserve"


async def test_transport_timeout_is_distinct_and_not_retried():
    connections = []
    release = asyncio.Event()

    async def stall(reader, writer):
        connections.append(writer)
        try:
            await reader.read(4096)
            await release.wait()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(stall, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = AsyncElasticsearch(f"http://127.0.0.1:{port}", request_timeout=0.1,
                                max_retries=0, retry_on_timeout=False)
    try:
        with pytest.raises(ConnectionTimeout):
            await client.info()
        assert len(connections) == 1
    finally:
        await client.close()
        release.set()
        server.close()
        await server.wait_closed()
