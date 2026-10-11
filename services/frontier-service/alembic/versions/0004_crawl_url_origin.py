"""Store canonical origins for Frontier admission limits.

Revision ID: 0004_crawl_url_origin
Revises: 0003_outbox_exchange_name
Create Date: 2026-10-10
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "0004_crawl_url_origin"
down_revision: str | Sequence[str] | None = "0003_outbox_exchange_name"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("crawl_urls", sa.Column("origin", sa.String(length=512), nullable=True))
    op.execute(
        "UPDATE crawl_urls "
        "SET origin = split_part(normalized_url, '/', 1) || '//' || split_part(normalized_url, '/', 3)"
    )
    op.alter_column("crawl_urls", "origin", nullable=False)
    op.create_index(
        "ix_crawl_urls_admitted_origin",
        "crawl_urls",
        ["crawl_id", "origin"],
        postgresql_where=sa.text("status <> 'skipped'"),
    )


def downgrade() -> None:
    op.drop_index("ix_crawl_urls_admitted_origin", table_name="crawl_urls")
    op.drop_column("crawl_urls", "origin")
