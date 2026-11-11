"""Create the Content-owned schema.

Revision ID: 0001_content_initial_schema
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_content_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pages",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("url_id", sa.Uuid(), nullable=False),
        sa.Column("crawl_id", sa.Uuid(), nullable=False),
        sa.Column("fetch_attempt", sa.Integer(), nullable=False),
        sa.Column("source_event_id", sa.Uuid(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("content_ref", sa.String(1024), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("title", sa.Text()), sa.Column("text", sa.Text()),
        sa.Column("canonical_url", sa.Text()), sa.Column("extracted_links", postgresql.JSONB()),
        sa.Column("processing_state", sa.String(32), nullable=False),
        sa.Column("error_category", sa.String(64)), sa.Column("error_detail", sa.String(1000)),
        sa.Column("text_truncated", sa.Boolean(), nullable=False),
        sa.Column("title_truncated", sa.Boolean(), nullable=False),
        sa.Column("links_truncated", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("url_id", name="uq_pages_url_id"),
        sa.CheckConstraint("fetch_attempt >= 1", name="ck_pages_attempt_positive"),
        sa.CheckConstraint("http_status BETWEEN 100 AND 599", name="ck_pages_http_status"),
        sa.CheckConstraint("processing_state IN ('indexing_pending', 'indexed', 'failed')", name="ck_pages_state"),
        sa.CheckConstraint("processing_state = 'failed' OR (title IS NOT NULL AND text IS NOT NULL AND extracted_links IS NOT NULL)", name="ck_pages_extraction"),
        sa.CheckConstraint("(processing_state = 'indexed' AND indexed_at IS NOT NULL) OR (processing_state <> 'indexed' AND indexed_at IS NULL)", name="ck_pages_indexed_at"),
        sa.CheckConstraint("(processing_state = 'failed' AND error_category IS NOT NULL AND error_detail IS NOT NULL) OR (processing_state <> 'failed' AND error_category IS NULL AND error_detail IS NULL)", name="ck_pages_failure"),
    )
    op.create_index("ix_pages_crawl_state", "pages", ["crawl_id", "processing_state"])
    op.create_table(
        "keywords", sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("normalized_term", sa.String(64), nullable=False),
        sa.UniqueConstraint("normalized_term", name="uq_keywords_normalized_term"),
    )
    op.create_table(
        "page_keywords",
        sa.Column("page_id", sa.Uuid(), sa.ForeignKey("pages.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("keyword_id", sa.Uuid(), sa.ForeignKey("keywords.id"), primary_key=True),
        sa.Column("score", sa.Float(), nullable=False),
        sa.CheckConstraint("score > 0 AND score < 'Infinity'::float8", name="ck_page_keywords_score"),
    )
    op.create_table(
        "outbox_events", sa.Column("event_id", sa.Uuid(), primary_key=True),
        sa.Column("exchange_name", sa.String(128), nullable=False),
        sa.Column("routing_key", sa.String(128), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("publish_attempts", sa.Integer(), nullable=False),
        sa.Column("last_publish_error", sa.String(1024)),
    )
    op.create_index("ix_outbox_events_pending", "outbox_events", ["created_at", "event_id"], postgresql_where=sa.text("published_at IS NULL"))
    op.create_table(
        "processed_events", sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("consumer_name", sa.String(128), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_event", postgresql.JSONB(), nullable=False),
        sa.UniqueConstraint("consumer_name", "event_id", name="uq_processed_events_consumer_name_event_id"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_index("ix_outbox_events_pending", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("page_keywords")
    op.drop_table("keywords")
    op.drop_index("ix_pages_crawl_state", table_name="pages")
    op.drop_table("pages")
