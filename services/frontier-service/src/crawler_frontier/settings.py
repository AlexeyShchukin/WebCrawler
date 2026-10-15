"""Runtime configuration for the Frontier service."""

from typing import Any
from urllib.parse import quote

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class FrontierSettings(BaseSettings):
    """Configuration supplied by the Frontier process environment."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str
    rabbitmq_url: str

    postgres_user: str | None = Field(default=None, exclude=True)
    postgres_password: str | None = Field(default=None, exclude=True, repr=False)
    frontier_database_host: str = Field(default="postgres", exclude=True)
    frontier_database_port: int = Field(default=5432, exclude=True)
    frontier_database_name: str = Field(default="frontier_db", exclude=True)
    rabbitmq_user: str | None = Field(default=None, exclude=True)
    rabbitmq_password: str | None = Field(default=None, exclude=True, repr=False)
    rabbitmq_host: str = Field(default="rabbitmq", exclude=True)
    rabbitmq_port: int = Field(default=5672, exclude=True)

    database_pool_size: int = Field(default=5, gt=0)
    database_max_overflow: int = Field(default=10, ge=0)
    database_pool_timeout_seconds: int = 30
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    lease_recovery_poll_interval_seconds: float = Field(default=5.0, gt=0, allow_inf_nan=False)

    max_fetch_attempts: int = 3
    fetch_lease_seconds: int = 120
    max_url_length: int = 4096
    max_crawl_depth: int = 5
    max_urls_per_crawl: int = 10_000
    max_urls_per_origin: int = 1_000

    @model_validator(mode="before")
    @classmethod
    def build_urls_from_connection_components(cls, values: Any) -> Any:
        """Build client URLs locally when Compose supplies raw credentials."""
        if not isinstance(values, dict):
            return values

        values = values.copy()
        if "database_url" not in values:
            username = values.get("postgres_user")
            password = values.get("postgres_password")
            if username and password:
                values["database_url"] = URL.create(
                    "postgresql+asyncpg",
                    username=username,
                    password=password,
                    host=values.get("frontier_database_host", "postgres"),
                    port=int(values.get("frontier_database_port", 5432)),
                    database=values.get("frontier_database_name", "frontier_db"),
                ).render_as_string(hide_password=False)
        if "rabbitmq_url" not in values:
            username = values.get("rabbitmq_user")
            password = values.get("rabbitmq_password")
            if username and password:
                values["rabbitmq_url"] = (
                    f"amqp://{quote(username, safe='')}:{quote(password, safe='')}"
                    f"@{values.get('rabbitmq_host', 'rabbitmq')}:{int(values.get('rabbitmq_port', 5672))}/"
                )
        return values

    @field_validator("database_url")
    @classmethod
    def database_url_must_use_asyncpg(cls, value: str) -> str:
        """Require the PostgreSQL driver used by Frontier's async ORM."""
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use the postgresql+asyncpg scheme")
        return value
