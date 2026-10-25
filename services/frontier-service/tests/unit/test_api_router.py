"""Contract tests for Frontier's internal API router."""

from crawler_frontier.api.router import router


def test_internal_api_router_exposes_all_non_health_endpoints() -> None:
    """Catch an endpoint being omitted while the application is reorganized."""
    routes = {route.path: route.methods for route in router.routes}

    assert routes == {
        "/internal/fetch-leases/claim": {"POST"},
        "/internal/fetch-leases/renew": {"POST"},
        "/internal/fetch-leases/submit": {"POST"},
        "/internal/crawls": {"POST"},
        "/internal/crawls/{crawl_id}": {"GET"},
    }
