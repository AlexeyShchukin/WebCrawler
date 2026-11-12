"""Index setup must not hide errors or damage an existing index."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
from elasticsearch import ApiError

from crawler_content.search.client import (
    IncompatibleMappingError,
    SearchClient,
    open_search_client,
)
from crawler_content.search.mapping import search_index_mapping
from crawler_content.settings import ContentSettings


def api_error(status, error_type):
    meta = ApiResponseMeta(status, "1.1", HttpHeaders(), 0.0, NodeConfig("http", "localhost", 9200))
    return ApiError(error_type, meta, {"error": {"type": error_type}, "status": status})


@pytest.fixture
def setup():
    client = MagicMock()
    client.indices.create = AsyncMock(return_value={"acknowledged": True})
    client.indices.get_mapping = AsyncMock(return_value={"pages": {"mappings": search_index_mapping()}})
    return client, SearchClient(client, "pages")


@pytest.mark.asyncio
async def test_create_and_repeated_create_validate_existing_mapping(setup):
    client, search = setup
    await search.ensure_index()
    client.indices.create.assert_awaited_once_with(index="pages", mappings=search_index_mapping())
    client.indices.create.side_effect = api_error(400, "resource_already_exists_exception")
    await search.ensure_index()
    assert client.indices.get_mapping.await_count == 2
    client.indices.delete.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [api_error(400, "illegal_argument_exception"), api_error(403, "security_exception"), api_error(500, "resource_already_exists_exception")])
async def test_other_create_errors_propagate(setup, error):
    client, search = setup
    client.indices.create.side_effect = error
    with pytest.raises(ApiError) as caught:
        await search.ensure_index()
    assert caught.value is error
    client.indices.get_mapping.assert_not_awaited()


@pytest.mark.asyncio
async def test_unacknowledged_creation_is_not_ready(setup):
    client, search = setup
    client.indices.create.return_value = {"acknowledged": False}
    with pytest.raises(ConnectionError):
        await search.ensure_index()
    client.indices.get_mapping.assert_not_awaited()


@pytest.mark.asyncio
async def test_mapping_read_failure_propagates(setup):
    client, search = setup
    client.indices.get_mapping.side_effect = TimeoutError("unavailable")
    with pytest.raises(TimeoutError):
        await search.validate_mapping()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["type", "dynamic", "source", "limit", "analyzer", "extra", "missing"])
async def test_incompatible_mapping_fails_readiness_without_deletion(setup, change):
    client, search = setup
    mapping = search_index_mapping()
    if change == "type":
        mapping["properties"]["title"]["type"] = "keyword"
    elif change == "dynamic":
        mapping["dynamic"] = True
    elif change == "source":
        mapping["_source"] = {"enabled": False}
    elif change == "limit":
        mapping["properties"]["url"]["ignore_above"] = 32766
    elif change == "analyzer":
        mapping["properties"]["text"]["analyzer"] = "whitespace"
    elif change == "extra":
        mapping["properties"]["html"] = {"type": "text"}
    else:
        del mapping["properties"]["url"]
    client.indices.get_mapping.return_value = {"pages": {"mappings": mapping}}
    with pytest.raises(IncompatibleMappingError):
        await search.validate_mapping()
    client.indices.create.assert_not_awaited()
    client.indices.delete.assert_not_called()


@pytest.mark.asyncio
async def test_missing_or_ambiguous_mapping_is_not_accepted(setup):
    client, search = setup
    for response in ({}, {"pages": {"mappings": {}}, "other": {"mappings": {}}}):
        client.indices.get_mapping.return_value = response
        with pytest.raises(IncompatibleMappingError):
            await search.ensure_index()


def search_settings(**kwargs):
    return ContentSettings(
        database_url="postgresql+asyncpg://u:p@localhost/content_db", rabbitmq_url="amqp://localhost/",
        elasticsearch_url="http://localhost:9200", object_storage_endpoint="http://localhost:9000",
        object_storage_access_key="key", object_storage_secret_key="secret", object_storage_bucket="bucket", **kwargs,
    )


@pytest.mark.asyncio
async def test_managed_client_configures_one_transport_and_closes_on_exit(monkeypatch):
    client = MagicMock()
    client.close = AsyncMock()
    client.indices.create = AsyncMock(return_value={"acknowledged": True})
    client.indices.get_mapping = AsyncMock(return_value={"pages": {"mappings": search_index_mapping()}})
    constructor = MagicMock(return_value=client)
    monkeypatch.setattr("crawler_content.search.client.AsyncElasticsearch", constructor)
    settings = search_settings(elasticsearch_index="pages", elasticsearch_request_timeout_seconds=3)
    async with open_search_client(settings) as search:
        assert isinstance(search, SearchClient)
        await search.validate_mapping()
        client.close.assert_not_awaited()
    constructor.assert_called_once_with("http://localhost:9200", request_timeout=3, max_retries=0, retry_on_timeout=False)
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_managed_client_closes_after_failed_startup(monkeypatch):
    client = MagicMock()
    client.close = AsyncMock()
    client.indices.create = AsyncMock(side_effect=ConnectionError("unavailable"))
    monkeypatch.setattr("crawler_content.search.client.AsyncElasticsearch", MagicMock(return_value=client))
    with pytest.raises(ConnectionError):
        async with open_search_client(search_settings()):
            pytest.fail("startup must fail")
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_managed_client_closes_when_body_raises(monkeypatch):
    client = MagicMock()
    client.close = AsyncMock()
    client.indices.create = AsyncMock(return_value={"acknowledged": True})
    client.indices.get_mapping = AsyncMock(return_value={"pages": {"mappings": search_index_mapping()}})
    monkeypatch.setattr("crawler_content.search.client.AsyncElasticsearch", MagicMock(return_value=client))
    with pytest.raises(RuntimeError, match="shutdown"):
        async with open_search_client(search_settings()):
            raise RuntimeError("shutdown")
    client.close.assert_awaited_once()


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_elasticsearch_timeout_is_rejected(timeout):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        search_settings(elasticsearch_request_timeout_seconds=timeout)

