import pytest

from crawler_frontier.domain.url_policy import (
    UrlValidationError,
    is_allowed_url,
    normalize_allowed_host,
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


@pytest.mark.parametrize("value", ["exam%ple.test", "example.test:443", "a..test", "-a.test", "a_.test"])
def test_allowed_hosts_reject_non_hostname_values(value):
    with pytest.raises(UrlValidationError):
        normalize_allowed_host(value)


def test_unicode_urls_and_hosts_share_event_contract_canonical_form():
    from pydantic import AnyHttpUrl

    normalized = normalize_url("https://BÜCHER.test/café#section")
    assert normalized == "https://xn--bcher-kva.test/caf%C3%A9"
    assert str(AnyHttpUrl(normalized)) == normalized
    assert normalize_allowed_host("BÜCHER.test") == "xn--bcher-kva.test"
    assert normalize_allowed_host("Example.TEST.") == "example.test"


@pytest.mark.parametrize("value", ["https://exam%ple.test/", "https://a..test/", "https://-a.test/"])
def test_invalid_hostname_is_a_frontier_validation_error(value):
    with pytest.raises(UrlValidationError):
        normalize_url(value)


def test_ipv4_mapped_ipv6_allowlist_uses_the_same_canonical_host_as_urls():
    allowed_host = "::ffff:192.0.2.1"

    assert normalize_allowed_host(allowed_host) == "::ffff:c000:201"
    assert is_allowed_url(
        "https://[::ffff:192.0.2.1]/page",
        seed_urls=["https://example.test/"],
        additional_allowed_hosts=[allowed_host],
    )


def test_unicode_idn_trailing_dot_is_removed_after_idna_canonicalization():
    assert normalize_allowed_host("B\u00dcCHER.test\u3002") == "xn--bcher-kva.test"
    assert normalize_url("https://B\u00dcCHER.test\u3002/page") == "https://xn--bcher-kva.test/page"


def test_percent_encoded_idn_hostname_is_validated_as_a_url_not_an_allowlist_host():
    assert normalize_url("https://b%C3%BCcher.test/") == "https://xn--bcher-kva.test/"
    with pytest.raises(UrlValidationError):
        normalize_allowed_host("b%C3%BCcher.test")


@pytest.mark.parametrize("value", ["[::1", "::1]"])
def test_allowed_host_rejects_unpaired_ipv6_brackets(value):
    with pytest.raises(UrlValidationError):
        normalize_allowed_host(value)


def test_url_rejects_invalid_authority_before_rebuilding_it():
    with pytest.raises(UrlValidationError):
        normalize_url("https://[::1]garbage/")


@pytest.mark.parametrize(
    ("value", "max_length"),
    [("child\\\\path", 80), ("child\npath", 80), ("child" * 20, 30)],
)
def test_relative_input_is_validated_before_urljoin(value, max_length):
    with pytest.raises(UrlValidationError):
        normalize_url(value, base_url="https://example.test/base/", max_length=max_length)


def test_urljoin_parse_errors_are_url_validation_errors():
    with pytest.raises(UrlValidationError):
        normalize_url("child", base_url="https://[invalid]/")
