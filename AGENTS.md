# Web Crawler Repository Instructions

## Project rules

- English is the project's primary language.
- Treat [docs/architecture.md](docs/architecture.md) as the source of truth for service responsibilities, event contracts, data ownership, and delivery order.
- Keep the architecture intentionally small: API, Frontier, Fetcher, Content, RabbitMQ, PostgreSQL, Redis, S3-compatible object storage, and Elasticsearch. Local development uses MinIO for object storage. Do not add infrastructure or services without an architectural need.
- Preserve strict service ownership. A service never reads or writes another service's PostgreSQL database. The API Service uses Frontier's internal HTTP API for crawl creation and status, and Content's internal HTTP API only for search and page details. Content receives crawl work exclusively through RabbitMQ events.
- Treat PostgreSQL as authoritative storage and Elasticsearch as a rebuildable search projection.
- Do not commit secrets, `.env` files, virtual environments, generated caches, or local infrastructure data.
- Do not create commits, amend commits, stage files, or push changes unless the repository owner explicitly requests it.

## Python and dependencies

- Support Python 3.12 and newer; local environments currently use Python 3.13.
- Each deployable service owns its `pyproject.toml`, `uv.lock`, and `.venv`. Run `uv` commands from that service directory.
- `shared-contracts` is a local editable package, not a deployable service. Services reference it through `[tool.uv.sources]`.
- Commit `pyproject.toml` and `uv.lock` when dependencies change. Never commit `.venv`.
- After changing dependencies, run `uv sync --all-groups` in the affected project. Use `uv lock --check` before handing off changes.

## Implementation workflow

- Implement one delivery-plan item at a time, in the order documented in `docs/architecture.md`.
- Add or update focused tests for behavior changes.
- Keep RabbitMQ messages compatible with the Pydantic models in `shared-contracts`.
- RabbitMQ carries metadata and object references only; raw fetched HTML is stored in object storage and referenced by `content_ref`.
- Message consumers must be idempotent.
- Use a transactional outbox for events derived from PostgreSQL state changes.
- Keep untrusted URL and HTML handling defensive: validate URLs, enforce response limits, and retain SSRF protections.

## Verification

- Run the smallest relevant test suite first, then the full suite for the changed project.
- Run Ruff for changed Python packages: `uv run ruff check src tests`.
- Do not claim a change is complete without fresh command output showing the relevant tests and checks pass.
