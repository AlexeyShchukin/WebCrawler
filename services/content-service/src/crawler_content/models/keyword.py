"""Keyword dictionary and weighted page relationships."""
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    Float,
    ForeignKey,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from crawler_content.models.base import Base


class Keyword(Base):
    __tablename__ = "keywords"
    __table_args__ = (UniqueConstraint(
        "normalized_term",
        name="uq_keywords_normalized_term"
    ),)
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    normalized_term: Mapped[str] = mapped_column(String(64), nullable=False)


class PageKeyword(Base):
    __tablename__ = "page_keywords"
    __table_args__ = (CheckConstraint(
        "score > 0 AND score < 'Infinity'::float8",
        name="ck_page_keywords_score"
    ),)
    page_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("pages.id", ondelete="CASCADE"),
        primary_key=True
    )
    keyword_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("keywords.id"), primary_key=True)
    score: Mapped[float] = mapped_column(Float, nullable=False)
