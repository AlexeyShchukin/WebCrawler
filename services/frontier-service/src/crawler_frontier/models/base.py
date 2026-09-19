"""Declarative base for Frontier-owned PostgreSQL tables."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for Frontier SQLAlchemy models."""
