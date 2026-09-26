from pathlib import Path

from crawler_frontier.migration_settings import database_url_from_environment


def test_migration_database_url_prefers_explicit_environment_value(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://runtime:secret@postgres:5432/frontier_db")

    assert database_url_from_environment(env_file=tmp_path / ".env") == (
        "postgresql+asyncpg://runtime:secret@postgres:5432/frontier_db"
    )


def test_migration_database_url_loads_local_compose_credentials(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_USER=crawler\nPOSTGRES_PASSWORD=pa:ss@word\n", encoding="utf-8")

    assert database_url_from_environment(env_file=env_file) == (
        "postgresql+asyncpg://crawler:pa%3Ass%40word@127.0.0.1:5433/frontier_db"
    )
