"""Async PostgreSQL engine, sessions, and readiness helpers for Frontier."""

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from crawler_frontier.settings import FrontierSettings


def create_engine(settings: FrontierSettings) -> AsyncEngine:
    """Create Frontier's process-owned asynchronous PostgreSQL engine."""
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout_seconds,
        pool_pre_ping=True,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create sessions that retain attributes after their transaction commits."""
    return async_sessionmaker(engine, expire_on_commit=False)


async def database_healthcheck(engine: AsyncEngine) -> None:
    """Raise if Frontier cannot acquire a usable PostgreSQL connection."""
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield one database session for an internal Frontier HTTP request."""
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with session_factory() as session:
        yield session
