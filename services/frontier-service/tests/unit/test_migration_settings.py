from pathlib import Path

from alembic.config import Config

from crawler_frontier.migration_settings import (
    database_url_for_alembic_config,
    database_url_from_environment,
)


def test_migration_database_url_prefers_explicit_environment_value(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://runtime:secret@postgres:5432/frontier_db")

    assert database_url_from_environment(env_file=tmp_path / ".env") == (
        "postgresql+asyncpg://runtime:secret@postgres:5432/frontier_db"
    )


def test_migration_database_url_loads_local_compose_credentials(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_HOST", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_PORT", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_NAME", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_USER=crawler\nPOSTGRES_PASSWORD=pa:ss@word\n", encoding="utf-8")

    assert database_url_from_environment(env_file=env_file) == (
        "postgresql+asyncpg://crawler:pa%3Ass%40word@127.0.0.1:5433/frontier_db"
    )


def test_migration_database_url_ignores_unrelated_database_location_variables(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_HOST", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_PORT", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_NAME", raising=False)
    monkeypatch.setenv("DATABASE_HOST", "unrelated-database")
    monkeypatch.setenv("DATABASE_PORT", "1234")
    monkeypatch.setenv("DATABASE_NAME", "unrelated_database")
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_USER=crawler\nPOSTGRES_PASSWORD=secret\n", encoding="utf-8")

    assert database_url_from_environment(env_file=env_file) == (
        "postgresql+asyncpg://crawler:secret@127.0.0.1:5433/frontier_db"
    )


def test_migration_database_url_uses_compose_database_location(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "crawler")
    monkeypatch.setenv("POSTGRES_PASSWORD", "pa:ss@word")
    monkeypatch.setenv("FRONTIER_DATABASE_HOST", "postgres")
    monkeypatch.setenv("FRONTIER_DATABASE_PORT", "5432")
    monkeypatch.setenv("FRONTIER_DATABASE_NAME", "frontier_db")

    assert database_url_from_environment(env_file=tmp_path / ".env") == (
        "postgresql+asyncpg://crawler:pa%3Ass%40word@postgres:5432/frontier_db"
    )


def test_migration_database_url_round_trips_through_alembic_config_with_encoded_credentials(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_HOST", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_PORT", raising=False)
    monkeypatch.delenv("FRONTIER_DATABASE_NAME", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_USER=crawler\nPOSTGRES_PASSWORD=pa%:@word\n", encoding="utf-8")
    database_url = database_url_from_environment(env_file=env_file)
    config = Config()

    config.set_main_option("sqlalchemy.url", database_url_for_alembic_config(database_url))

    assert database_url == "postgresql+asyncpg://crawler:pa%25%3A%40word@127.0.0.1:5433/frontier_db"
    assert config.get_main_option("sqlalchemy.url") == database_url
