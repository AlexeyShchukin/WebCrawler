import pytest
from pydantic import ValidationError
from sqlalchemy.engine import make_url

from crawler_content.settings import ContentSettings


def base(**kwargs):
    return ContentSettings(database_url="postgresql+asyncpg://u:p@localhost/content_db", rabbitmq_url="amqp://localhost/", elasticsearch_url="http://localhost:9200", object_storage_endpoint="http://localhost:9000", object_storage_access_key="access", object_storage_secret_key="secret", object_storage_bucket="bucket", **kwargs)


def test_credentials_are_encoded_and_hidden(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u@name")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss/%")
    monkeypatch.setenv("RABBITMQ_USER", "r@user")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "r@pass")
    s = ContentSettings(elasticsearch_url="http://localhost:9200", object_storage_endpoint="http://localhost:9000", object_storage_access_key="access", object_storage_secret_key="private-value", object_storage_bucket="bucket")
    assert make_url(s.database_url).password == "p@ss/%"
    assert "r%40pass" in s.rabbitmq_url
    assert "p@ss/%" not in repr(s) and "r@pass" not in repr(s) and "private-value" not in repr(s)


def test_explicit_url_precedence(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "other")
    monkeypatch.setenv("POSTGRES_PASSWORD", "other")
    assert make_url(base().database_url).username == "u"


@pytest.mark.parametrize("kwargs", [{"database_pool_size": 0}, {"database_max_overflow": -1}, {"database_pool_timeout_seconds": 0}, {"outbox_poll_interval_seconds": float("nan")}, {"content_max_links": 0}])
def test_limits_rejected(kwargs):
    with pytest.raises(ValidationError):
        base(**kwargs)


def test_invalid_database_scheme():
    with pytest.raises(ValidationError):
        ContentSettings(database_url="sqlite:///test", rabbitmq_url="amqp://localhost/", elasticsearch_url="http://localhost:9200", object_storage_endpoint="http://localhost:9000", object_storage_access_key="a", object_storage_secret_key="b", object_storage_bucket="c")


def test_missing_settings():
    with pytest.raises(ValidationError):
        ContentSettings()
