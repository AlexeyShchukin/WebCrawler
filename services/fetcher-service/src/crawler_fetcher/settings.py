"""Runtime configuration for Fetcher."""

from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FetcherSettings(BaseSettings):
    """Validated Fetcher settings read from environment variables."""

    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    http_max_body_bytes: Annotated[
        int,
        Field(default=5 * 1024 * 1024, ge=1, le=50 * 1024 * 1024),
    ]
