"""Create the complete current Frontier schema from an empty database.

Revision ID: 0001_frontier_initial_schema
Revises:
Create Date: 2026-10-15
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0001_frontier_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


url_status = postgresql.ENUM(
    "queued",
    "fetching",
    "downloaded",
    "fetched",
    "failed",
    "skipped",
    name="url_status",
    create_type=False,
)


def upgrade() -> None:
    """Create Frontier's current schema without replaying historical transforms."""
    bind = op.get_bind()
    url_status.create(bind, checkfirst=True)

    op.create_table(
        "crawls",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("seed_urls", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("allowed_hosts", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_crawls"),
    )
    op.create_table(
        "crawl_urls",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("crawl_id", sa.Uuid(), nullable=False),
        sa.Column("normalized_url", sa.String(length=16384), nullable=False),
        sa.Column("url_digest", sa.LargeBinary(length=32), nullable=False),
        sa.Column("origin", sa.String(length=512), nullable=False),
        sa.Column("depth", sa.Integer(), nullable=False),
        sa.Column("status", url_status, server_default=sa.text("'queued'"), nullable=False),
        sa.Column("fetch_attempt", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_category", sa.String(length=64), nullable=True),
        sa.Column("last_error_detail", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("octet_length(url_digest) = 32", name="ck_crawl_urls_digest_length"),
        sa.CheckConstraint(
            "url_digest = sha256(convert_to(normalized_url, 'UTF8'))",
            name="ck_crawl_urls_digest_matches",
        ),
        sa.CheckConstraint("depth >= 0", name="ck_crawl_urls_depth_nonnegative"),
        sa.CheckConstraint("fetch_attempt >= 1", name="ck_crawl_urls_fetch_attempt_positive"),
        sa.ForeignKeyConstraint(["crawl_id"], ["crawls.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_crawl_urls"),
        sa.UniqueConstraint("crawl_id", "url_digest", name="uq_crawl_urls_crawl_id_url_digest"),
        sa.UniqueConstraint("crawl_id", "id", name="uq_crawl_urls_crawl_id_id"),
    )
    op.execute("""
        CREATE FUNCTION frontier_set_url_digest() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          NEW.url_digest := pg_catalog.sha256(pg_catalog.convert_to(NEW.normalized_url, 'UTF8'));
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER crawl_urls_set_digest BEFORE INSERT OR UPDATE ON crawl_urls
        FOR EACH ROW EXECUTE FUNCTION frontier_set_url_digest()
    """)
    op.create_index(
        "ix_crawl_urls_ready",
        "crawl_urls",
        ["crawl_id", "created_at", "id"],
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "ix_crawl_urls_expired_lease",
        "crawl_urls",
        ["lease_until", "id"],
        postgresql_where=sa.text("status = 'fetching' AND lease_until IS NOT NULL"),
    )
    op.create_index(
        "ix_crawl_urls_admitted_origin",
        "crawl_urls",
        ["crawl_id", "origin"],
        postgresql_where=sa.text("status <> 'skipped'"),
    )
    op.create_table(
        "links",
        sa.Column("crawl_id", sa.Uuid(), nullable=False),
        sa.Column("source_url_id", sa.Uuid(), nullable=False),
        sa.Column("target_url_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["crawl_id", "source_url_id"],
            ["crawl_urls.crawl_id", "crawl_urls.id"],
            name="fk_links_source_url_within_crawl",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["crawl_id", "target_url_id"],
            ["crawl_urls.crawl_id", "crawl_urls.id"],
            name="fk_links_target_url_within_crawl",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("source_url_id", "target_url_id", name="pk_links"),
    )
    op.create_index("ix_links_target_url_id", "links", ["target_url_id"])
    op.create_table(
        "outbox_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("exchange_name", sa.String(length=128), nullable=False),
        sa.Column("routing_key", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False),
        sa.Column("last_publish_error", sa.String(length=1024), nullable=True),
        sa.PrimaryKeyConstraint("event_id", name="pk_outbox_events"),
    )
    op.create_index(
        "ix_outbox_events_pending",
        "outbox_events",
        ["created_at", "event_id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "processed_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("consumer_name", sa.String(length=128), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_processed_events"),
        sa.UniqueConstraint("consumer_name", "event_id", name="uq_processed_events_consumer_name_event_id"),
    )


def downgrade() -> None:
    """Drop the complete baseline schema from a database created by this revision."""
    op.drop_table("processed_events")
    op.drop_index("ix_outbox_events_pending", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_links_target_url_id", table_name="links")
    op.drop_table("links")
    op.drop_index("ix_crawl_urls_admitted_origin", table_name="crawl_urls")
    op.drop_index("ix_crawl_urls_expired_lease", table_name="crawl_urls")
    op.drop_index("ix_crawl_urls_ready", table_name="crawl_urls")
    op.drop_table("crawl_urls")
    op.drop_table("crawls")
    op.execute("DROP FUNCTION frontier_set_url_digest()")
    url_status.drop(op.get_bind(), checkfirst=True)
