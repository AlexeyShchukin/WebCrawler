# Frontier Service

Owns crawl scheduling, URL normalization, URL deduplication, crawl state, and link graph storage.

Frontier is the only service that decides whether a discovered URL becomes a fetch task.

It also owns the URL state machine, fetch leases, retry limits, URL normalization, and the default seed-host crawl scope.
