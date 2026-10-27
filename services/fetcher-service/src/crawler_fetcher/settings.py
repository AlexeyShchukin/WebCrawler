"""Runtime configuration for Fetcher."""

from typing import Annotated, Any
from urllib.parse import quote

from pydantic import AmqpDsn, AnyHttpUrl, Field, RedisDsn, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class FetcherSettings(BaseSettings):
    """Validated Fetcher settings read from environment variables."""

    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    rabbitmq_url: AmqpDsn = Field(exclude=True, repr=False)
    redis_url: RedisDsn = Field(exclude=True, repr=False)
    rabbitmq_user: str | None = Field(default=None, exclude=True)
    rabbitmq_password: str | None = Field(default=None, exclude=True, repr=False)
    rabbitmq_host: str = Field(default="rabbitmq", exclude=True)
    rabbitmq_port: int = Field(default=5672, exclude=True)
    redis_password: str | None = Field(default=None, exclude=True, repr=False)
    redis_host: str = Field(default="redis", exclude=True)
    redis_port: int = Field(default=6379, exclude=True)
    object_storage_endpoint: AnyHttpUrl
    object_storage_access_key: SecretStr
    object_storage_secret_key: SecretStr
    object_storage_bucket: Annotated[str, Field(min_length=3, max_length=63)]
    frontier_internal_url: AnyHttpUrl = "http://frontier:8000"
    fetch_concurrency: Annotated[int, Field(default=10, ge=1, le=100)]
    fetch_lease_seconds: Annotated[int, Field(default=120, ge=1, le=3600)]
    http_connect_timeout_seconds: Annotated[float, Field(default=10, gt=0, le=120)]
    http_read_timeout_seconds: Annotated[float, Field(default=30, gt=0, le=300)]
    http_total_timeout_seconds: Annotated[float, Field(default=60, gt=0, le=3600)]
    http_chain_timeout_seconds: Annotated[float, Field(default=120, gt=0, le=3600)]
    http_max_body_bytes: Annotated[
        int,
        Field(default=5 * 1024 * 1024, ge=1, le=50 * 1024 * 1024),
    ]
    origin_requests_per_second: Annotated[float, Field(default=1.0, gt=0, le=100)]
    circuit_breaker_failure_threshold: Annotated[int, Field(default=5, ge=1, le=100)]

    @model_validator(mode="before")
    @classmethod
    def build_urls_from_components(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = values.copy()
        if "rabbitmq_url" not in values:
            user, password = values.get("rabbitmq_user"), values.get("rabbitmq_password")
            if user and password:
                values["rabbitmq_url"] = (
                    f"amqp://{quote(user, safe='')}:{quote(password, safe='')}"
                    f"@{values.get('rabbitmq_host', 'rabbitmq')}:"
                    f"{values.get('rabbitmq_port', 5672)}/"
                )
        if "redis_url" not in values and values.get("redis_password"):
            password = quote(values["redis_password"], safe="")
            values["redis_url"] = (
                f"redis://:{password}@{values.get('redis_host', 'redis')}:"
                f"{values.get('redis_port', 6379)}/0"
            )
        return values
