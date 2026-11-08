# Fetcher delivery acceptance

This opt-in stack exercises the installed Fetcher and Frontier applications with
real RabbitMQ, PostgreSQL, Redis and MinIO. It is independent of the root Compose
project. Ports 19867, 19872, 19879, 19880, 19881, 19889 and 19900 must be free.
Use the sample credentials in the repository `.env.example`; no production `.env`
or existing Docker volume is used. Do not combine this Compose file with another.
Keep the explicit `--project-name fetcher-audit-e2e` in every command below: a
parent shell's `COMPOSE_PROJECT_NAME` can otherwise override the YAML project name.

From `services/fetcher-service`, build and start the isolated stack:

```powershell
docker compose --project-name fetcher-audit-e2e --env-file ../../.env.example -f tests/e2e/compose.yml build
docker compose --project-name fetcher-audit-e2e --env-file ../../.env.example -f tests/e2e/compose.yml up -d --wait --wait-timeout 180
```

Run the focused acceptance test or the full Fetcher suite, respectively:

```powershell
uv run python tests/e2e/run_checks.py tests/e2e/test_delivery.py -q -p no:cacheprovider
uv run python tests/e2e/run_checks.py
uv run ruff check src tests
uv lock --check
```

`run_checks.py` enables the real infrastructure tests and sets only the dedicated
stack's endpoints. It constructs the Redis URL from the sample credentials
without printing them. Ordinary `uv run pytest` skips these tests unless the
explicit opt-in variables are set.

The HTTP fixture is a controllable origin and transparent proxy to the real
Frontier lease API. It can hold the origin response, submit forwarding and the
real successful submit response. A transparent TCP relay also forwards Fetcher's
original AMQP bytes to/from the real broker. It observes `Basic.Deliver` and
outbound `Basic.Ack`, correlating each message by connection, channel and delivery
tag. Publisher confirms, rejects and ACKs of other deliveries cannot satisfy the
assertion. The observer has focused tests for fragmented frames, multiple ACKs,
separate channels/connections and rejection.

The acceptance test checks actual object bytes/MIME, Frontier state/outbox events,
per-delivery ACKs and broker queue state at each boundary, then republishes the
original event to test duplicate handling. Queue totals come directly from
`rabbitmqctl` through the Docker CLI. Asynchronous management statistics, including
aggregate ACK counters, are unsuitable as a synchronization oracle across cases.
The normal response path shares an eight-second test deadline across both submit
gates, below the production client's ten-second timeout. The proxies neither
fabricate lease decisions nor replace RabbitMQ ACKs.

A third case holds the accepted submit response and stops the actual Fetcher
through its normal shutdown/drain path. This intentionally extends the response
gate beyond the normal eight-second observation budget. It checks that closing the AMQP
connection returns the original unacknowledged delivery to the broker, then
starts a new Fetcher process. Real Frontier must reject the old attempt, the
broker must receive its ACK, and HTTP/object/outcome counts must remain unchanged.
This exercises lost ACK and actual broker redelivery; it does not simulate power
loss.

The controlled origin uses `93.184.216.3` solely inside an **internal Docker
network**. This public-shaped address lets the actual SSRF checks run unchanged;
no external website, DNS stub, private-IP allow-list or production bypass is
used. Do not reuse this network configuration for deployment. The fixture has
only synthetic HTML/XHTML pages and test control endpoints.

HTML and XHTML cases verify Fetcher's storage and `page.fetched` boundary.
They do not claim downstream XHTML processing: Content Service is currently a
scaffold without its consumer/parser/application.

After recording the test exit status, remove only this disposable stack:

```powershell
docker compose --project-name fetcher-audit-e2e --env-file ../../.env.example -f tests/e2e/compose.yml down --volumes
```
