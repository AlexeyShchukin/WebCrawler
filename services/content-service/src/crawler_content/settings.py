"""Validated configuration for the Content service."""

from typing import Any
from urllib.parse import quote

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class ContentSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str = Field(repr=False, exclude=True)
    rabbitmq_url: str = Field(repr=False, exclude=True)
    elasticsearch_url: str = Field(repr=False, exclude=True)
    elasticsearch_index: str = Field(default="crawler-pages-v1", min_length=1)
    elasticsearch_request_timeout_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)
    object_storage_endpoint: str
    object_storage_access_key: SecretStr
    object_storage_secret_key: SecretStr
    object_storage_bucket: str = Field(min_length=1)
    object_storage_connect_timeout_seconds: float = Field(default=5, gt=0, allow_inf_nan=False)
    object_storage_read_timeout_seconds: float = Field(default=15, gt=0, allow_inf_nan=False)
    object_storage_total_timeout_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)

    postgres_user: str | None = Field(default=None, exclude=True, repr=False)
    postgres_password: str | None = Field(default=None, exclude=True, repr=False)
    content_database_host: str = Field(default="postgres", exclude=True)
    content_database_port: int = Field(default=5432, ge=1, le=65535, exclude=True)
    content_database_name: str = Field(default="content_db", exclude=True)
    rabbitmq_user: str | None = Field(default=None, exclude=True, repr=False)
    rabbitmq_password: str | None = Field(default=None, exclude=True, repr=False)
    rabbitmq_host: str = Field(default="rabbitmq", exclude=True)
    rabbitmq_port: int = Field(default=5672, ge=1, le=65535, exclude=True)

    database_pool_size: int = Field(default=5, gt=0)
    database_max_overflow: int = Field(default=10, ge=0)
    database_pool_timeout_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
    outbox_poll_interval_seconds: float = Field(default=1, gt=0, allow_inf_nan=False)
    content_concurrency: int = Field(default=4, gt=0)
    content_prefetch_count: int = Field(default=4, gt=0)
    content_max_body_bytes: int = Field(default=52428800, gt=0)
    content_max_text_chars: int = Field(default=1000000, gt=0)
    content_max_title_chars: int = Field(default=2048, gt=0)
    content_max_links: int = Field(default=1000, gt=0)
    content_max_keywords: int = Field(default=50, gt=0)

    @model_validator(mode="before")
    @classmethod
    def connection_urls(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = values.copy()
        if (
                not values.get("database_url")
                and values.get("postgres_user")
                and values.get("postgres_password")
        ):
            values["database_url"] = URL.create(
                "postgresql+asyncpg",
                username=values["postgres_user"],
                password=values["postgres_password"],
                host=values.get("content_database_host", "postgres"),
                port=int(values.get("content_database_port", 5432)),
                database=values.get("content_database_name", "content_db"),
            ).render_as_string(hide_password=False)
        if (
                not values.get("rabbitmq_url")
                and values.get("rabbitmq_user")
                and values.get("rabbitmq_password")
        ):
            user = quote(values["rabbitmq_user"], safe="")
            password = quote(values["rabbitmq_password"], safe="")
            host = values.get("rabbitmq_host", "rabbitmq")
            port = int(values.get("rabbitmq_port", 5672))
            values["rabbitmq_url"] = f"amqp://{user}:{password}@{host}:{port}/"
        return values

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        return value
