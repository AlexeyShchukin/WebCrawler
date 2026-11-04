# Fetcher Service

Consumes URL fetch tasks from RabbitMQ and downloads HTML pages.

Successful responses must have the MIME type `text/html` or
`application/xhtml+xml`. The response body is stored without parsing.

Fetcher enforces outbound HTTP controls including connection limits, timeouts, response size limits, retry backoff, per-origin rate limits, circuit breaking, and redirect validation.

The public HTTP session does not retain or automatically send cookies, including
across redirects and retries. Sites that require cookies may return different
content or fail to complete a redirect.

`HTTP_MAX_BODY_BYTES` limits an accepted response body. Its default is 5 MiB and its maximum is 50 MiB.

The application exposes `GET /health`, returning 200 after successful worker
startup and 503 when the worker is not ready. It is not a continuous probe of
every dependency. Compose checks this endpoint and restarts the process when it
exits, including after a consumer failure; an unhealthy status alone does not
restart the container.

The object-storage settings `OBJECT_STORAGE_ENDPOINT`, `OBJECT_STORAGE_ACCESS_KEY`,
`OBJECT_STORAGE_SECRET_KEY`, and `OBJECT_STORAGE_BUCKET` are required. For the
broker and Redis, provide `RABBITMQ_URL` and `REDIS_URL`, or provide separate
`RABBITMQ_USER`, `RABBITMQ_PASSWORD`, and `REDIS_PASSWORD` values. Explicit URLs
take precedence over separate credentials. The component host and port variables
are `RABBITMQ_HOST`, `RABBITMQ_PORT`, `REDIS_HOST`, and `REDIS_PORT`; their
defaults are `rabbitmq:5672` and `redis:6379`. Fetcher percent-encodes the
separate credentials when constructing its client URLs.

Compose obtains the separate credentials from the local `.env` file. To keep a
literal `$` in a Compose `.env` value, enclose the value in single quotes, for
example `REDIS_PASSWORD='pass$word'`; unquoted and double-quoted values undergo
Compose interpolation. Optional settings and defaults are:

| Variable | Default |
| --- | --- |
| `FETCH_CONCURRENCY` | 10 |
| `REDIS_CONNECT_TIMEOUT_SECONDS` | 2 |
| `REDIS_OPERATION_TIMEOUT_SECONDS` | 3 |
| `HTTP_CONNECT_TIMEOUT_SECONDS` | 10 |
| `HTTP_READ_TIMEOUT_SECONDS` | 30 |
| `HTTP_TOTAL_TIMEOUT_SECONDS` | 60 |
| `HTTP_CHAIN_TIMEOUT_SECONDS` | 120 |
| `HTTP_MAX_BODY_BYTES` | 5242880 |
| `ORIGIN_REQUESTS_PER_SECOND` | 1.0 |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | 5 |

Redis timeouts must be positive and at most 10 seconds; the connection timeout
must not exceed the operation timeout. The operation deadline includes connection
acquisition and applies even when `REDIS_URL` contains client timeout parameters.

Without a valid `Retry-After`, Fetcher suggests a bounded exponential delay with
integer additive jitter between the base delay and twice the base, capped at
3600 seconds. Frontier maps suggestions to fixed retry buckets, so distinct
suggested delays can still result in the same delivery time.

Fetcher claims and renews a Frontier HTTP lease before and during a fetch, then
submits `page.fetched`, `fetch.retry_requested`, or `page.failed` with the
owner token. Frontier publishes the accepted outcome from its transactional
outbox. If OriginPolicy blocks a request due to rate limiting or an open circuit breaker,
Fetcher schedules it for retry without claiming a Frontier lease.
HTML is stored in object storage; events contain the object reference
rather than the body.

Run checks from this directory:

```powershell
uv run pytest -q
uv run ruff check src tests
uv lock --check
```

Test groups can be run separately:

```powershell
uv run pytest tests/unit
uv run pytest tests/integration -m "not requires_infra"
uv run pytest tests/integration -m requires_infra
```

Integration tests are marked `integration`; those using Redis, RabbitMQ, or
MinIO also carry `requires_infra`. Redis tests require `TEST_REDIS_URL` pointing
to an isolated Redis server. The multi-service tests additionally require
`FETCHER_LIVE_INTEGRATION=1` and the isolated services described by their
fixture; without their required configuration these tests are skipped.
