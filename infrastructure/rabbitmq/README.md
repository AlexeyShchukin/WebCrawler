# RabbitMQ Topology

RabbitMQ creates its development credentials from `.env` on its first start.
The one-shot `rabbitmq-init` container then imports `definitions.json` through
the Management HTTP API. Application containers depend on that job completing
successfully, so they cannot start before the topology exists.

- `crawler.topic` is the durable topic exchange for normal crawler events.
- `crawler.retry` routes delayed `FetchUrlEvent` messages to four buckets: `fetch.url.retry.5s`, `fetch.url.retry.30s`, `fetch.url.retry.2m`, and `fetch.url.retry.10m`. Each delay queue dead-letters expired messages to `crawler.topic` with routing key `fetch.url`.
- `crawler.dlx` receives messages that a processing queue rejects without requeueing. Each processing queue has its own durable dead-letter queue.

The Fetcher publishes pre-request infrastructure retries to the 5-second `crawler.retry` bucket, never to the primary `crawler.topic` `fetch.url` route. Frontier publishes a new `FetchUrlEvent` to a bucket selected from `suggested_delay_seconds` only after it allocates the next fetch execution attempt.

Publisher confirms are a client-channel setting rather than a broker definition. Every future publisher must open its `aio-pika` channel with `publisher_confirms=True`, publish persistent messages, and acknowledge an outbox event only after the broker confirms the publication.
