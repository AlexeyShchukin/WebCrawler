import pytest
from aio_pika import Connection
from pydantic import ValidationError
from redis.asyncio.connection import parse_url

from crawler_fetcher.settings import FetcherSettings

SETTINGS_VALUES = {
    "rabbitmq_url": "amqp://guest:guest@localhost/",
    "redis_url": "redis://localhost:6379/0",
    "object_storage_endpoint": "http://localhost:9000",
    "object_storage_access_key": "minioadmin",
    "object_storage_secret_key": "minioadmin",
    "object_storage_bucket": "crawler-content",
}


def test_settings_require_runtime_connection_values() -> None:
    with pytest.raises(ValidationError):
        FetcherSettings(_env_file=None)


def test_settings_use_a_bounded_default_response_limit() -> None:
    settings = FetcherSettings(**SETTINGS_VALUES)

    assert settings.http_max_body_bytes == 5 * 1024 * 1024


def test_settings_read_response_limit_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_MAX_BODY_BYTES", "1048576")

    assert FetcherSettings(**SETTINGS_VALUES).http_max_body_bytes == 1_048_576


def test_settings_reject_a_non_positive_response_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_MAX_BODY_BYTES", "0")

    with pytest.raises(ValidationError, match="http_max_body_bytes"):
        FetcherSettings(**SETTINGS_VALUES)


@pytest.mark.asyncio
async def test_raw_credentials_are_encoded_for_client_parsers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("RABBITMQ_USER", "worker@/#%:")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "p@/#%: $'")
    monkeypatch.setenv("REDIS_PASSWORD", "r@/#%: $'")
    values = {k: v for k, v in SETTINGS_VALUES.items() if k not in {"rabbitmq_url", "redis_url"}}

    settings = FetcherSettings(**values)
    rabbit = Connection(str(settings.rabbitmq_url)).url
    redis = parse_url(str(settings.redis_url))

    assert (rabbit.user, rabbit.password, rabbit.host) == ("worker@/#%:", "p@/#%: $'", "rabbitmq")
    assert (redis["password"], redis["host"], redis["db"]) == ("r@/#%: $'", "redis", 0)


def test_explicit_urls_override_raw_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RABBITMQ_USER", "ignored")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "ignored")
    monkeypatch.setenv("REDIS_PASSWORD", "ignored")

    settings = FetcherSettings(**SETTINGS_VALUES)

    assert str(settings.rabbitmq_url) == SETTINGS_VALUES["rabbitmq_url"]
    assert str(settings.redis_url) == SETTINGS_VALUES["redis_url"]


@pytest.mark.parametrize("raw_credentials", [False, True])
def test_connection_secrets_are_excluded_from_settings_output(raw_credentials: bool) -> None:
    values = dict(SETTINGS_VALUES)
    if raw_credentials:
        values.pop("rabbitmq_url")
        values.pop("redis_url")
        values.update(rabbitmq_user="worker", rabbitmq_password="rabbitsecret",
                      redis_password="redissecret")
    else:
        values.update(rabbitmq_url="amqp://worker:rabbitsecret@rabbitmq/",
                      redis_url="redis://:redissecret@redis/0")

    settings = FetcherSettings(**values)

    for output in (repr(settings), str(settings.model_dump()), settings.model_dump_json()):
        assert "rabbitsecret" not in output
        assert "redissecret" not in output
    assert "rabbitmq_url" not in settings.model_dump()
    assert "redis_url" not in settings.model_dump()
    assert "rabbitsecret" in str(settings.rabbitmq_url)
    assert "redissecret" in str(settings.redis_url)
