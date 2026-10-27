"""ASGI application factory for Fetcher."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Protocol

from fastapi import FastAPI, HTTPException, status

from crawler_fetcher.settings import FetcherSettings


class FetcherWorkerProtocol(Protocol):
    """Runtime operations required by the Fetcher application."""

    dependencies_ready: bool

    async def start(self) -> None: ...

    async def close(self) -> None: ...


WorkerFactory = Callable[[FetcherSettings], FetcherWorkerProtocol]


def _create_worker(settings: FetcherSettings) -> FetcherWorkerProtocol:
    """Construct the worker when its lifespan starts."""
    from crawler_fetcher.worker import FetcherWorker

    return FetcherWorker(settings)


def create_app(
    settings: FetcherSettings | None = None,
    *,
    worker_factory: WorkerFactory = _create_worker,
) -> FastAPI:
    """Create the Fetcher ASGI application and manage its worker lifecycle."""
    runtime_settings = settings or FetcherSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        worker = worker_factory(runtime_settings)
        app.state.worker = worker
        try:
            await worker.start()
            yield
        finally:
            await worker.close()

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        worker: FetcherWorkerProtocol | None = getattr(app.state, "worker", None)
        if worker is None or not worker.dependencies_ready:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Dependencies are not ready.",
            )
        return {"status": "ok"}

    return app
