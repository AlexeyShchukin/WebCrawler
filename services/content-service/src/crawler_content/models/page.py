"""Authoritative page and processing state."""
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crawler_content.models.base import Base, utc_now


class Page(Base):
    __tablename__ = "pages"
    __table_args__ = (
        UniqueConstraint("url_id", name="uq_pages_url_id"),
        CheckConstraint("fetch_attempt >= 1", name="ck_pages_attempt_positive"),
        CheckConstraint("http_status BETWEEN 100 AND 599", name="ck_pages_http_status"),
        CheckConstraint("processing_state IN ('indexing_pending', 'indexed', 'failed')", name="ck_pages_state"),
        CheckConstraint("processing_state = 'failed' OR (title IS NOT NULL AND text IS NOT NULL AND extracted_links IS NOT NULL)", name="ck_pages_extraction"),
        CheckConstraint("(processing_state = 'indexed' AND indexed_at IS NOT NULL) OR (processing_state <> 'indexed' AND indexed_at IS NULL)", name="ck_pages_indexed_at"),
        CheckConstraint("(processing_state = 'failed' AND error_category IS NOT NULL AND error_detail IS NOT NULL) OR (processing_state <> 'failed' AND error_category IS NULL AND error_detail IS NULL)", name="ck_pages_failure"),
        Index("ix_pages_crawl_state", "crawl_id", "processing_state"),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    url_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    crawl_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    fetch_attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    source_event_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    http_status: Mapped[int] = mapped_column(Integer, nullable=False)
    content_type: Mapped[str] = mapped_column(Text, nullable=False)
    content_ref: Mapped[str] = mapped_column(String(1024), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    text: Mapped[str | None] = mapped_column(Text)
    canonical_url: Mapped[str | None] = mapped_column(Text)
    extracted_links: Mapped[list[str] | None] = mapped_column(JSONB)
    processing_state: Mapped[str] = mapped_column(String(32), nullable=False)
    error_category: Mapped[str | None] = mapped_column(String(64))
    error_detail: Mapped[str | None] = mapped_column(String(1000))
    text_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    title_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    links_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
