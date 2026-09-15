# Fetcher Service

Consumes URL fetch tasks from RabbitMQ and downloads HTML pages.

Fetcher enforces outbound HTTP controls including connection limits, timeouts, response size limits, retry backoff, per-origin rate limits, circuit breaking, and redirect validation.
