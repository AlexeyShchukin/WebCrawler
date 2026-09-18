import pytest

from crawler_frontier.url_policy import (
    UrlValidationError,
    is_allowed_url,
    normalize_url,
)


def test_normalize_url_applies_only_safe_transformations() -> None:
    normalized = normalize_url("HTTP://Example.COM:80#section")

    assert normalized == "http://example.com/"


def test_normalize_url_preserves_query_and_path_semantics() -> None:
    assert normalize_url("https://example.com/foo?b=2&a=1") == "https://example.com/foo?b=2&a=1"
    assert normalize_url("https://example.com/foo/") == "https://example.com/foo/"


def test_normalize_url_resolves_a_relative_link_against_its_source() -> None:
    normalized = normalize_url("../about#team", base_url="https://example.com/docs/page")

    assert normalized == "https://example.com/about"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "https://user:password@example.com/",
        "/relative-without-a-base",
        "https://example.com/contains space",
    ],
)
def test_normalize_url_rejects_unsafe_or_incomplete_input(url: str) -> None:
    with pytest.raises(UrlValidationError):
        normalize_url(url)


def test_normalize_url_rejects_values_over_the_configured_limit() -> None:
    with pytest.raises(UrlValidationError):
        normalize_url("https://example.com/" + "a" * 20, max_length=20)


def test_scope_allows_only_seed_hosts_by_default() -> None:
    seed_urls = ["https://example.com/start"]

    assert is_allowed_url("https://example.com/about", seed_urls=seed_urls)
    assert not is_allowed_url("https://sub.example.com/about", seed_urls=seed_urls)
    assert not is_allowed_url("https://example.com.evil.test/about", seed_urls=seed_urls)


def test_scope_allows_an_explicit_additional_host() -> None:
    assert is_allowed_url(
        "https://docs.example.com/guide",
        seed_urls=["https://example.com/start"],
        additional_allowed_hosts=["docs.example.com"],
    )
