
import aiohttp
import pytest

from crawler_fetcher.network import (
    NetworkFailure,
    PublicResolver,
    _require_public_addresses,
    create_http_session,
)
from crawler_fetcher.settings import FetcherSettings


def settings(**kwargs):
    return FetcherSettings(
        rabbitmq_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0",
        object_storage_endpoint="http://localhost:9000",
        object_storage_access_key="a",
        object_storage_secret_key="b",
        object_storage_bucket="crawler-content",
        **kwargs,
    )



def test_unexpected_address_type_is_dns_failure(monkeypatch):
    def reject(value):
        raise TypeError("unexpected type")

    monkeypatch.setattr("crawler_fetcher.network.ipaddress.ip_address", reject)
    with pytest.raises(NetworkFailure, match="dns_failure"):
        _require_public_addresses([object()])


@pytest.mark.asyncio
async def test_session_has_configured_total():
    resolver = PublicResolver(aiohttp.resolver.DefaultResolver())
    async with create_http_session(
        settings(http_total_timeout_seconds=0.1), resolver
    ) as session:
        assert session.timeout.total == 0.1


