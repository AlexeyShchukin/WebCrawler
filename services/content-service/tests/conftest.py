"""Isolated PostgreSQL fixture for explicitly selected integration tests."""
import asyncio
import os
import re
from collections.abc import Iterator
from uuid import uuid4

import pytest
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command
from crawler_content.migration_settings import database_url_from_environment


@pytest.fixture(scope="session")
def isolated_database() -> Iterator[str]:
    explicit = os.environ.get("CONTENT_TEST_ADMIN_URL")
    base = make_url(explicit or database_url_from_environment())
    if base.drivername != "postgresql+asyncpg":
        raise RuntimeError("Test administrator URL must use postgresql+asyncpg")
    if not explicit and base.host not in {"localhost", "127.0.0.1", "::1"}:
        raise RuntimeError("Set CONTENT_TEST_ADMIN_URL explicitly for non-local PostgreSQL")
    name = "content_test_" + uuid4().hex
    assert re.fullmatch(r"content_test_[0-9a-f]{32}", name)
    admin_url = base.set(database="postgres")
    test_url = base.set(database=name).render_as_string(hide_password=False)

    async def ddl(create: bool) -> None:
        if not re.fullmatch(r"content_test_[0-9a-f]{32}", name):
            raise RuntimeError("Refusing DDL on an unverified test database")
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                verb = "CREATE" if create else "DROP"
                suffix = "" if create else " WITH (FORCE)"
                await connection.execute(text(f'{verb} DATABASE "{name}"{suffix}'))
        finally:
            await engine.dispose()

    asyncio.run(ddl(True))
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = test_url
    try:
        command.upgrade(Config("alembic.ini"), "head")
        yield test_url
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        asyncio.run(ddl(False))
