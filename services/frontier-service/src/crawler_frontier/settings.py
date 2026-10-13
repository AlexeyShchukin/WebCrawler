"""Runtime configuration for the Frontier service."""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class FrontierSettings(BaseSettings):
    """Configuration supplied by the Frontier process environment."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str
    rabbitmq_url: str

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

    @field_validator("database_url")
    @classmethod
    def database_url_must_use_asyncpg(cls, value: str) -> str:
        """Require the PostgreSQL driver used by Frontier's async ORM."""
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use the postgresql+asyncpg scheme")
        return value
