"""Configuration helpers used exclusively by Frontier's Alembic environment."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
LOCAL_ENV_FILE = REPOSITORY_ROOT / ".env"


def database_url_from_environment(*, env_file: Path = LOCAL_ENV_FILE) -> str:
    """Return an explicit service URL or construct the local Compose URL.

    A deployable service supplies ``DATABASE_URL`` directly.  Local commands run
    from this monorepo instead load Compose's PostgreSQL credentials from the
    untracked repository ``.env`` file.
    """
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url

    load_dotenv(env_file, override=False)
    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Set DATABASE_URL, or define POSTGRES_USER and POSTGRES_PASSWORD in the repository .env file."
        )

    return URL.create(
        "postgresql+asyncpg",
        username=username,
        password=password,
        host=os.environ.get("FRONTIER_DATABASE_HOST", "127.0.0.1"),
        port=int(os.environ.get("FRONTIER_DATABASE_PORT", "5433")),
        database=os.environ.get("FRONTIER_DATABASE_NAME", "frontier_db"),
    ).render_as_string(hide_password=False)


def database_url_for_alembic_config(database_url: str) -> str:
    """Escape a SQLAlchemy URL for Alembic's ConfigParser-backed configuration."""
    return database_url.replace("%", "%%")
