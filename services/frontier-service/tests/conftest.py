"""Disposable PostgreSQL databases for integration tests, never the runtime DB."""

import asyncio
import os
import re
from collections.abc import Iterator
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command
from crawler_frontier.database import create_engine, create_session_factory
from crawler_frontier.migration_settings import database_url_from_environment
from crawler_frontier.settings import FrontierSettings


@pytest.fixture(scope="session", autouse=True)
def isolated_database(request: pytest.FixtureRequest) -> Iterator[str | None]:
    if not any(item.get_closest_marker("integration") for item in request.session.items):
        yield None
        return
    explicit_admin = os.environ.get("FRONTIER_TEST_ADMIN_URL")
    base = make_url(explicit_admin or database_url_from_environment())
    if not explicit_admin and base.host not in {"localhost", "127.0.0.1", "::1"}:
        raise RuntimeError("Set FRONTIER_TEST_ADMIN_URL explicitly for a non-local test server")
    name = "frontier_test_" + uuid4().hex
    admin_url = base.set(database="postgres")
    test_url = base.set(database=name).render_as_string(hide_password=False)

    async def database_ddl(*, create: bool) -> None:
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                # The identifier is generated here, never obtained from user input.
                sql = f'CREATE DATABASE "{name}"' if create else f'DROP DATABASE "{name}" WITH (FORCE)'
                await connection.execute(text(sql))
        finally:
            await engine.dispose()

    asyncio.run(database_ddl(create=True))
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
        asyncio.run(database_ddl(create=False))


@pytest_asyncio.fixture(autouse=True)
async def clean_database(request, isolated_database):
    if not request.node.get_closest_marker("integration"):
        yield
        return
    name = make_url(isolated_database).database or ""
    if not re.fullmatch(r"frontier_test_[0-9a-f]{32}", name):
        raise RuntimeError("Refusing to truncate a database not created by the test fixture")
    engine = create_async_engine(isolated_database)

    async def truncate():
        async with engine.begin() as connection:
            await connection.execute(text(
                "TRUNCATE outbox_events, processed_events, links, crawl_urls, crawls"
            ))

    try:
        await truncate()
        yield
    finally:
        try:
            await truncate()
        finally:
            await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(clean_database, isolated_database):
    engine = create_engine(FrontierSettings(database_url=isolated_database, rabbitmq_url="amqp://localhost/"))
    try:
        yield create_session_factory(engine)
    finally:
        await engine.dispose()
