# Frontier Service

Owns crawl scheduling, URL normalization, URL deduplication, crawl state, and link graph storage.

Frontier is the only service that decides whether a discovered URL becomes a fetch task.

It also owns the URL state machine, fetch leases, retry limits, URL normalization, and the default seed-host crawl scope.

Frontier starts from one squashed Alembic initial-schema migration. On an empty
database, apply it with `alembic upgrade head`. It does not migrate databases
recorded against the removed pre-squash revision history: verify their schema and
data separately, then use an explicitly approved `alembic stamp` operation.

For local integration checks, start PostgreSQL and RabbitMQ from the repository
root. In the current Docker/Windows environment, host-based broker tests need
the RabbitMQ test-host override to reach RabbitMQ through `127.0.0.1:5672`:

```powershell
docker compose -f docker-compose.yml -f infrastructure/rabbitmq/compose.test-host.yml up -d postgres rabbitmq rabbitmq-init
```

Then change to this service directory and run:

```powershell
uv run pytest -p no:cacheprovider -q
uv run pytest -p no:cacheprovider -m integration -q
uv run ruff check --no-cache src tests
uv lock --check
```

Integration tests create and migrate a temporary `frontier_test_<uuid>` database,
reset all scheduling/reliability rows between cases, and drop only that database
after the run. They never clean the runtime database. Local credentials come from
the repository's untracked `.env`; use `FRONTIER_TEST_ADMIN_URL` and
`FRONTIER_TEST_RABBITMQ_URL` for explicit test servers. A non-local PostgreSQL server
requires the explicit test admin URL. The admin role must be allowed to create test
databases. Broker scenarios use private test queues and exchanges.

`/health` returns 503 until PostgreSQL, RabbitMQ subscriptions and both background
workers are ready. Repeated infrastructure failures make readiness unsuccessful
until recovery; permanent worker failures remain unhealthy. Fetcher obtains a
lease through HTTP `claim`, extends it with `renew`, and finalizes it with
owner-token checked `submit`; Frontier writes the accepted result through its
transactional outbox. Extracted links preserve graph data regardless of result
ordering and release any fetch lease without claiming Content completion.
