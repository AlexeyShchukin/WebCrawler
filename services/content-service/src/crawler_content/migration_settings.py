"""Database-only configuration for Content migrations."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL

LOCAL_ENV_FILE = Path(__file__).resolve().parents[4] / ".env"


def database_url_from_environment(*, env_file: Path = LOCAL_ENV_FILE) -> str:
    if database_url := os.environ.get("DATABASE_URL"):
        return database_url
    load_dotenv(env_file, override=False)
    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    if not username or not password:
        raise RuntimeError("Set DATABASE_URL or POSTGRES_USER and POSTGRES_PASSWORD")
    return URL.create(
        "postgresql+asyncpg", username=username, password=password,
        host=os.environ.get("CONTENT_DATABASE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CONTENT_DATABASE_PORT", "5433")),
        database=os.environ.get("CONTENT_DATABASE_NAME", "content_db"),
    ).render_as_string(hide_password=False)


def database_url_for_alembic_config(database_url: str) -> str:
    return database_url.replace("%", "%%")
