# Fetcher Service

Consumes URL fetch tasks from RabbitMQ and downloads HTML pages.

Fetcher enforces outbound HTTP controls including connection limits, timeouts, response size limits, retry backoff, per-origin rate limits, circuit breaking, and redirect validation.

`HTTP_MAX_BODY_BYTES` limits an accepted response body. Its default is 5 MiB and its maximum is 50 MiB.
