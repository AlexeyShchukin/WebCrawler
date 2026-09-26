from sqlalchemy import ForeignKeyConstraint, UniqueConstraint

from crawler_frontier.models import Crawl, CrawlUrl, Link, OutboxEvent
from crawler_frontier.state_machine import UrlStatus


def _index_by_name(table, name: str):
    return next(index for index in table.indexes if index.name == name)


def test_crawl_url_schema_enforces_one_normalized_url_per_crawl() -> None:
    table = CrawlUrl.__table__

    assert set(table.c.keys()) == {
        "id",
        "crawl_id",
        "normalized_url",
        "depth",
        "status",
        "fetch_attempt",
        "lease_until",
        "last_error_category",
        "last_error_detail",
        "created_at",
        "updated_at",
    }

    constraints = [constraint for constraint in table.constraints if isinstance(constraint, UniqueConstraint)]
    assert any(
        constraint.name == "uq_crawl_urls_crawl_id_normalized_url"
        and tuple(column.name for column in constraint.columns) == ("crawl_id", "normalized_url")
        for constraint in constraints
    )


def test_frontier_models_store_the_documented_graph_and_url_statuses() -> None:
    assert set(Crawl.__table__.c.keys()) == {"id", "seed_urls", "allowed_hosts", "created_at"}
    assert tuple(column.name for column in Link.__table__.primary_key.columns) == (
        "source_url_id",
        "target_url_id",
    )
    assert set(CrawlUrl.__table__.c.status.type.enums) == {status.value for status in UrlStatus}


def test_link_schema_restricts_both_endpoints_to_its_crawl() -> None:
    table = Link.__table__

    assert set(table.c.keys()) == {
        "crawl_id",
        "source_url_id",
        "target_url_id",
        "created_at",
    }

    foreign_keys = [constraint for constraint in table.constraints if isinstance(constraint, ForeignKeyConstraint)]
    assert {
        (
            constraint.name,
            tuple(column.name for column in constraint.columns),
            tuple(element.target_fullname for element in constraint.elements),
        )
        for constraint in foreign_keys
    } == {
        (
            "fk_links_source_url_within_crawl",
            ("crawl_id", "source_url_id"),
            ("crawl_urls.crawl_id", "crawl_urls.id"),
        ),
        (
            "fk_links_target_url_within_crawl",
            ("crawl_id", "target_url_id"),
            ("crawl_urls.crawl_id", "crawl_urls.id"),
        ),
    }

    crawl_url_uniques = [
        constraint
        for constraint in CrawlUrl.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert any(
        constraint.name == "uq_crawl_urls_crawl_id_id"
        and tuple(column.name for column in constraint.columns) == ("crawl_id", "id")
        for constraint in crawl_url_uniques
    )
    target_lookup = _index_by_name(table, "ix_links_target_url_id")
    assert tuple(column.name for column in target_lookup.columns) == ("target_url_id",)


def test_scheduler_and_lease_recovery_indexes_match_their_queries() -> None:
    ready = _index_by_name(CrawlUrl.__table__, "ix_crawl_urls_ready")
    expired_lease = _index_by_name(CrawlUrl.__table__, "ix_crawl_urls_expired_lease")

    assert tuple(column.name for column in ready.columns) == ("crawl_id", "created_at", "id")
    assert str(ready.dialect_options["postgresql"]["where"]) == "status = 'queued'"
    assert tuple(column.name for column in expired_lease.columns) == ("lease_until", "id")
    assert (
        str(expired_lease.dialect_options["postgresql"]["where"])
        == "status = 'fetching' AND lease_until IS NOT NULL"
    )


def test_outbox_pending_index_matches_the_delivery_query() -> None:
    pending = _index_by_name(OutboxEvent.__table__, "ix_outbox_events_pending")

    assert tuple(column.name for column in pending.columns) == ("created_at", "event_id")
    assert str(pending.dialect_options["postgresql"]["where"]) == "published_at IS NULL"
