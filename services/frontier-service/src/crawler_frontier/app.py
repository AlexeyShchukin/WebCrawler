"""FastAPI application lifecycle for Frontier's internal API."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status

from crawler_frontier.database import (
    create_engine,
    create_session_factory,
    database_healthcheck,
)
from crawler_frontier.settings import FrontierSettings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Create one database engine per process and dispose it on shutdown."""
    engine = create_engine(FrontierSettings())
    app.state.database_engine = engine
    app.state.session_factory = create_session_factory(engine)
    try:
        yield
    finally:
        await engine.dispose()


def create_app() -> FastAPI:
    """Create the Frontier internal application without starting a server."""
    app = FastAPI(title="Crawler Frontier Service", lifespan=lifespan)

    @app.get("/health", include_in_schema=False)
    async def healthcheck() -> dict[str, str]:
        try:
            await database_healthcheck(app.state.database_engine)
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is unavailable",
            ) from error
        return {"status": "ok"}

    return app


app = create_app()
