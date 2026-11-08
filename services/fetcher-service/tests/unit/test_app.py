from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from crawler_fetcher.app import create_app
from crawler_fetcher.settings import FetcherSettings

SETTINGS = FetcherSettings(
    rabbitmq_url="amqp://guest:guest@localhost/",
    redis_url="redis://localhost:6379/0",
    object_storage_endpoint="http://localhost:9000",
    object_storage_access_key="minioadmin",
    object_storage_secret_key="minioadmin",
    object_storage_bucket="crawler-content",
)


@pytest.mark.asyncio
async def test_app_closes_worker_on_shutdown() -> None:
    worker = AsyncMock()
    app = create_app(settings=SETTINGS, worker_factory=lambda _: worker)

    async with app.router.lifespan_context(app):
        worker.start.assert_awaited_once()

    worker.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_app_closes_worker_when_startup_fails() -> None:
    worker = AsyncMock()
    worker.start.side_effect = RuntimeError("worker startup failed")
    app = create_app(settings=SETTINGS, worker_factory=lambda _: worker)

    with pytest.raises(RuntimeError, match="worker startup failed"):
        async with app.router.lifespan_context(app):
            pass

    worker.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_health_is_available_when_worker_dependencies_are_ready() -> None:
    worker = AsyncMock(dependencies_ready=True)
    app = create_app(settings=SETTINGS, worker_factory=lambda _: worker)

    health_route = next(
        (route for route in app.routes if getattr(route, "path", None) == "/health"),
        None,
    )
    assert health_route is not None

    async with app.router.lifespan_context(app):
        response = await health_route.endpoint()

    assert response == {"status": "ok"}


@pytest.mark.asyncio
async def test_health_is_unavailable_when_worker_dependencies_are_not_ready() -> None:
    worker = AsyncMock(dependencies_ready=False)
    app = create_app(settings=SETTINGS, worker_factory=lambda _: worker)
    health_route = next(
        route for route in app.routes if getattr(route, "path", None) == "/health"
    )

    async with app.router.lifespan_context(app):
        with pytest.raises(HTTPException) as exception:
            await health_route.endpoint()

    assert exception.value.status_code == 503
