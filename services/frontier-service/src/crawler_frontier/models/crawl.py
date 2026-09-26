"""Frontier-owned crawl scheduling and graph tables."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crawler_frontier.models.base import Base
from crawler_frontier.state_machine import UrlStatus


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(UTC)


URL_STATUS_TYPE = Enum(
    UrlStatus,
    name="url_status",
    values_callable=lambda statuses: [status.value for status in statuses],
)


class Crawl(Base):
    """One independently scheduled crawl and its immutable admission scope."""

    __tablename__ = "crawls"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    seed_urls: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    allowed_hosts: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class CrawlUrl(Base):
    """One normalized URL node and its Frontier-owned scheduling state."""

    __tablename__ = "crawl_urls"
    __table_args__ = (
        UniqueConstraint("crawl_id", "normalized_url", name="uq_crawl_urls_crawl_id_normalized_url"),
        UniqueConstraint("crawl_id", "id", name="uq_crawl_urls_crawl_id_id"),
        CheckConstraint("depth >= 0", name="ck_crawl_urls_depth_nonnegative"),
        CheckConstraint("fetch_attempt >= 1", name="ck_crawl_urls_fetch_attempt_positive"),
        Index(
            "ix_crawl_urls_ready",
            "crawl_id",
            "created_at",
            "id",
            postgresql_where=text("status = 'queued'"),
        ),
        Index(
            "ix_crawl_urls_expired_lease",
            "lease_until",
            "id",
            postgresql_where=text("status = 'fetching' AND lease_until IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    crawl_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False
    )
    normalized_url: Mapped[str] = mapped_column(String(4096), nullable=False)
    depth: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[UrlStatus] = mapped_column(
        URL_STATUS_TYPE,
        nullable=False,
        default=UrlStatus.QUEUED,
        server_default=text("'queued'"),
    )
    fetch_attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class Link(Base):
    """A deduplicated directed edge between two crawl URL nodes."""

    __tablename__ = "links"
    __table_args__ = (
        Index("ix_links_target_url_id", "target_url_id"),
        ForeignKeyConstraint(
            ["crawl_id", "source_url_id"],
            ["crawl_urls.crawl_id", "crawl_urls.id"],
            name="fk_links_source_url_within_crawl",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["crawl_id", "target_url_id"],
            ["crawl_urls.crawl_id", "crawl_urls.id"],
            name="fk_links_target_url_within_crawl",
            ondelete="CASCADE",
        ),
    )

    crawl_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    source_url_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    target_url_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
