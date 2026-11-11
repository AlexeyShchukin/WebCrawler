import pytest
from sqlalchemy.engine import make_url

from crawler_content.database import create_engine, create_session_factory
from crawler_content.migration_settings import (
    database_url_for_alembic_config,
    database_url_from_environment,
)
from crawler_content.settings import ContentSettings


def test_engine_pool_and_session_factory():
    s = ContentSettings(database_url="postgresql+asyncpg://u:p@localhost/content_db", rabbitmq_url="amqp://localhost/", elasticsearch_url="http://localhost:9200", object_storage_endpoint="http://localhost:9000", object_storage_access_key="a", object_storage_secret_key="b", object_storage_bucket="c", database_pool_size=3, database_max_overflow=2, database_pool_timeout_seconds=4)
    engine = create_engine(s)
    assert engine.pool.size() == 3
    assert engine.pool._max_overflow == 2
    assert engine.pool._timeout == 4
    assert create_session_factory(engine).kw["expire_on_commit"] is False


def test_migration_settings_are_independent(monkeypatch, tmp_path):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "u@name")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss/%")
    url = database_url_from_environment(env_file=tmp_path / "missing")
    assert make_url(url).password == "p@ss/%"
    assert make_url(url).database == "content_db"
    assert database_url_for_alembic_config(url) == url.replace("%", "%%")


def test_migration_url_precedence(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://a:b@localhost/db")
    assert database_url_from_environment(env_file=tmp_path / "missing").endswith("/db")


def test_migration_requires_credentials(monkeypatch, tmp_path):
    for key in ("DATABASE_URL", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError):
        database_url_from_environment(env_file=tmp_path / "missing")
