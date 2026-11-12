"""Safe creation and validation of the Content search index."""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

from elasticsearch import ApiError, AsyncElasticsearch

from crawler_content.search.mapping import search_index_mapping
from crawler_content.settings import ContentSettings


class IncompatibleMappingError(RuntimeError):
    """The existing index cannot safely store the expected projection."""


class SearchClient:
    """Operate on one configured index using an injected async client."""

    def __init__(self, client: AsyncElasticsearch, index: str) -> None:
        self._client = client
        self._index = index

    async def close(self) -> None:
        """Release the underlying client's network resources."""
        await self._client.close()

    async def ensure_index(self) -> None:
        """Create the index or validate the index created by another worker."""
        try:
            response = await self._client.indices.create(index=self._index, mappings=search_index_mapping())
        except ApiError as error:
            body = error.body
            info = body.get("error") if isinstance(body, Mapping) else None
            if not (error.status_code == 400 and isinstance(info, Mapping)
                    and info.get("type") == "resource_already_exists_exception"):
                raise
        else:
            if not response.get("acknowledged"):
                raise ConnectionError("Elasticsearch did not acknowledge index creation")
        await self.validate_mapping()

    async def validate_mapping(self) -> None:
        """Check the live mapping without creating or modifying the index."""
        response = await self._client.indices.get_mapping(index=self._index)
        if len(response) != 1:
            raise IncompatibleMappingError("Expected exactly one search index")
        index_info: Any = next(iter(response.values()))
        mapping = index_info.get("mappings") if isinstance(index_info, Mapping) else None
        if not isinstance(mapping, Mapping):
            raise IncompatibleMappingError("Search mapping is missing")
        expected = search_index_mapping()
        source = mapping.get("_source", {})
        if (mapping.get("dynamic") != "strict" or mapping.get("properties") != expected["properties"]
                or not isinstance(source, Mapping) or source not in ({}, {"enabled": True})):
            raise IncompatibleMappingError("Search index mapping is incompatible")


@asynccontextmanager
async def open_search_client(settings: ContentSettings) -> AsyncIterator[SearchClient]:
    """Own one client until startup fails or the service lifespan ends."""
    transport = AsyncElasticsearch(
        settings.elasticsearch_url,
        request_timeout=settings.elasticsearch_request_timeout_seconds,
        max_retries=0,
        retry_on_timeout=False,
    )
    search = SearchClient(transport, settings.elasticsearch_index)
    try:
        await search.ensure_index()
        yield search
    finally:
        await search.close()
