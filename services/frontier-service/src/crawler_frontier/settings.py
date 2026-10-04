"""Runtime configuration for the Frontier service."""

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class FrontierSettings(BaseSettings):
    """Configuration supplied by the Frontier process environment."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str
    database_pool_size: int = 5
    database_max_overflow: int = 10
    database_pool_timeout_seconds: int = 30

    @field_validator("database_url")
    @classmethod
    def database_url_must_use_asyncpg(cls, value: str) -> str:
        """Require the PostgreSQL driver used by Frontier's async ORM."""
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use the postgresql+asyncpg scheme")
        return value
