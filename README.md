# Web Crawler

An event-driven web crawler built from four Python microservices.

The system crawls public HTML pages, stores raw content in object storage, stores the link graph and extracted keywords, and provides full-text search.

Architecture, service responsibilities, and the delivery plan are documented in [docs/architecture.md](docs/architecture.md).

## Development environments

Each service is an independent Python project with its own `pyproject.toml`,
`uv.lock`, and `.venv`. Run commands from the service directory:

```powershell
cd services/frontier-service
uv sync --all-groups
```

`shared-contracts` is a local editable dependency shared by the services. It contains only typed RabbitMQ message models.

## Services

- `api-service`: public API for crawl control, status, and search.
- `frontier-service`: URL scheduling, deduplication, and link graph storage.
- `fetcher-service`: scalable HTTP worker.
- `content-service`: HTML extraction, keyword persistence, and search indexing.
