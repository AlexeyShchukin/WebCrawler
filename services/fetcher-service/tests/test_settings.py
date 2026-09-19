import pytest
from pydantic import ValidationError

from crawler_fetcher.settings import FetcherSettings


def test_settings_use_a_bounded_default_response_limit() -> None:
    settings = FetcherSettings()

    assert settings.http_max_body_bytes == 5 * 1024 * 1024


def test_settings_read_response_limit_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_MAX_BODY_BYTES", "1048576")

    assert FetcherSettings().http_max_body_bytes == 1_048_576


def test_settings_reject_a_non_positive_response_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_MAX_BODY_BYTES", "0")

    with pytest.raises(ValidationError, match="http_max_body_bytes"):
        FetcherSettings()
