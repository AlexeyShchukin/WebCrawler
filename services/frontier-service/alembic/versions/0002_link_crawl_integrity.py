"""Enforce that each graph edge stays within one crawl.

Revision ID: 0002_link_crawl_integrity
Revises: 0001_frontier_schema
Create Date: 2026-09-26
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "0002_link_crawl_integrity"
down_revision: str | Sequence[str] | None = "0001_frontier_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("links", sa.Column("crawl_id", sa.Uuid(), nullable=True))
    op.execute(
        """
        UPDATE links
        SET crawl_id = crawl_urls.crawl_id
        FROM crawl_urls
        WHERE crawl_urls.id = links.source_url_id
        """
    )
    op.alter_column("links", "crawl_id", nullable=False)

    op.create_unique_constraint("uq_crawl_urls_crawl_id_id", "crawl_urls", ["crawl_id", "id"])
    op.drop_constraint("links_source_url_id_fkey", "links", type_="foreignkey")
    op.drop_constraint("links_target_url_id_fkey", "links", type_="foreignkey")
    op.create_foreign_key(
        "fk_links_source_url_within_crawl",
        "links",
        "crawl_urls",
        ["crawl_id", "source_url_id"],
        ["crawl_id", "id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_links_target_url_within_crawl",
        "links",
        "crawl_urls",
        ["crawl_id", "target_url_id"],
        ["crawl_id", "id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("fk_links_target_url_within_crawl", "links", type_="foreignkey")
    op.drop_constraint("fk_links_source_url_within_crawl", "links", type_="foreignkey")
    op.create_foreign_key(
        "links_target_url_id_fkey",
        "links",
        "crawl_urls",
        ["target_url_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "links_source_url_id_fkey",
        "links",
        "crawl_urls",
        ["source_url_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_constraint("uq_crawl_urls_crawl_id_id", "crawl_urls", type_="unique")
    op.drop_column("links", "crawl_id")
