# crawler-contracts

Shared Pydantic models for RabbitMQ messages exchanged by crawler services.

This package does not contain business logic, database models, service configuration, or transport implementations.

## Event contracts

Every event inherits the same immutable envelope:

- `event_id`: globally unique event identity.
- `crawl_id`: crawl that owns the event.
- `url_id`: normalized URL node affected by the event.
- `fetch_attempt`: Frontier-allocated fetch execution attempt number.
- `created_at`: timezone-aware UTC timestamp.

`fetch_attempt` distinguishes a late result from an earlier fetch execution from the currently scheduled execution for the same `url_id`. A temporary failure before `fetch.started`, such as unavailable Redis origin-policy state, does not increment it.

The package rejects unknown fields. Consumers use `event_id` together with their handler name as the idempotency key in `processed_events`.

| Model | Producer | Purpose                                                       |
|---|---|---------------------------------------------------------------|
| `FetchUrlEvent` | Frontier | Schedule an admitted URL for fetching                         |
| `FetchStartedEvent` | Fetcher | Start the Frontier fetch lease                                |
| `FetchRetryRequestedEvent` | Fetcher | Ask Frontier to schedule a retry for a transient fetch-execution failure |
| `PageFetchedEvent` | Fetcher | Deliver metadata and an object-storage reference for raw HTML |
| `LinksExtractedEvent` | Content | Send discovered links to Frontier                             |
| `PageProcessedEvent` | Content | Confirm that a page was persisted and indexed                 |
| `PageFailedEvent` | Fetcher or Content | Report a final pipeline failure                               |

Use `model_dump_json()` before publishing and `model_validate_json()` after consuming a message.
