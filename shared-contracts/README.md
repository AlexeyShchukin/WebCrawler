# crawler-contracts

Shared Pydantic models for RabbitMQ messages exchanged by crawler services.

This package does not contain business logic, database models, service configuration, or transport implementations.

## Event contracts

Every event inherits the same immutable envelope:

- `event_id`: globally unique event identity.
- `crawl_id`: crawl that owns the event.
- `url_id`: normalized URL node affected by the event.
- `fetch_attempt`: fetch lifecycle attempt number.
- `created_at`: timezone-aware UTC timestamp.

`fetch_attempt` distinguishes a late result from an earlier fetch attempt from the currently scheduled attempt for the same `url_id`.

The package rejects unknown fields. Consumers use `event_id` together with their handler name as the idempotency key in `processed_events`.

| Model | Producer | Purpose                                                       |
|---|---|---------------------------------------------------------------|
| `FetchUrlEvent` | Frontier | Schedule an admitted URL for fetching                         |
| `FetchStartedEvent` | Fetcher | Start the Frontier fetch lease                                |
| `PageFetchedEvent` | Fetcher | Deliver metadata and an object-storage reference for raw HTML |
| `LinksExtractedEvent` | Content | Send discovered links to Frontier                             |
| `PageProcessedEvent` | Content | Confirm that a page was persisted and indexed                 |
| `PageFailedEvent` | Fetcher or Content | Report a final pipeline failure                               |

Use `model_dump_json()` before publishing and `model_validate_json()` after consuming a message.
