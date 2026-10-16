"""URL normalization and crawl-scope policy for Frontier."""

import re
from collections.abc import Iterable
from ipaddress import IPv6Address
from urllib.parse import urljoin, urlsplit, urlunsplit

from pydantic import AnyHttpUrl, ValidationError


class UrlValidationError(ValueError):
    """Raised when a URL cannot enter the crawl frontier."""


_DEFAULT_PORTS = {"http": 80, "https": 443}
_ALLOWED_SCHEMES = frozenset({"http", "https"})


def normalize_url(value: str, *, base_url: str | None = None, max_length: int = 4096) -> str:
    """Return a canonical absolute HTTP(S) URL using only safe transformations."""
    if max_length < 1:
        raise ValueError("max_length must be positive")
    _validate_url_input(value, max_length=max_length)

    try:
        candidate = urljoin(base_url, value) if base_url is not None else value
    except (TypeError, ValueError) as error:
        raise UrlValidationError("URL has invalid syntax") from error
    _validate_url_input(candidate, max_length=max_length)

    try:
        validated = AnyHttpUrl(candidate)
        parsed = urlsplit(candidate)
        port = parsed.port
    except (ValidationError, ValueError) as error:
        raise UrlValidationError("URL has an invalid hostname or port") from error

    scheme = parsed.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise UrlValidationError("URL scheme must be http or https")
    if parsed.username is not None or parsed.password is not None:
        raise UrlValidationError("URL userinfo is not allowed")
    hostname = urlsplit(str(validated)).hostname
    if hostname is None:
        raise UrlValidationError("URL must include a hostname")

    normalized_host = _normalize_canonical_hostname(hostname)
    if ":" in normalized_host and not normalized_host.startswith("["):
        normalized_host = f"[{normalized_host}]"

    normalized_port = "" if port in (None, _DEFAULT_PORTS[scheme]) else f":{port}"
    normalized_path = parsed.path or "/"
    value = urlunsplit((scheme, f"{normalized_host}{normalized_port}", normalized_path, parsed.query, ""))
    try:
        canonical = str(AnyHttpUrl(value))
    except ValidationError as error:
        raise UrlValidationError("URL has invalid HTTP(S) syntax") from error
    if len(canonical) > max_length:
        raise UrlValidationError("Canonical URL exceeds the configured size limit")
    return canonical


def is_allowed_url(
    value: str,
    *,
    seed_urls: Iterable[str],
    additional_allowed_hosts: Iterable[str] = (),
) -> bool:
    """Return whether a URL has a hostname explicitly allowed for this crawl."""
    candidate_host = _hostname(normalize_url(value))
    seed_hosts = {_hostname(normalize_url(seed_url)) for seed_url in seed_urls}
    extra_hosts = {normalize_allowed_host(host) for host in additional_allowed_hosts}
    return candidate_host in seed_hosts | extra_hosts


def _hostname(value: str) -> str:
    hostname = urlsplit(value).hostname
    if hostname is None:
        raise UrlValidationError("URL must include a hostname")
    return hostname.lower()


def normalize_allowed_host(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise UrlValidationError("Allowed host must be a non-empty hostname")
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise UrlValidationError("Allowed host must not contain whitespace or control characters")
    if any(character in value for character in ("/", "@", "?", "#", "%", "\\")):
        raise UrlValidationError("Allowed host must be a hostname without URL components")

    has_open_bracket, has_close_bracket = "[" in value, "]" in value
    if has_open_bracket or has_close_bracket:
        if not (value.startswith("[") and value.endswith("]") and value.count("[") == value.count("]") == 1):
            raise UrlValidationError("Allowed IPv6 host must use paired square brackets")
        return _normalize_ipv6_hostname(value[1:-1])

    if ":" in value:
        return _normalize_ipv6_hostname(value)

    try:
        hostname = urlsplit(str(AnyHttpUrl(f"https://{value}/"))).hostname
    except (ValidationError, ValueError) as error:
        raise UrlValidationError("Invalid hostname") from error
    if hostname is None:
        raise UrlValidationError("Invalid hostname")
    return _normalize_dns_hostname(hostname)


def _validate_url_input(value: object, *, max_length: int) -> None:
    if not isinstance(value, str) or not value:
        raise UrlValidationError("URL must be a non-empty string")
    if len(value) > max_length:
        raise UrlValidationError("URL exceeds the configured size limit")
    if any(character.isspace() or ord(character) < 32 for character in value) or "\\" in value:
        raise UrlValidationError("URL must not contain whitespace or control characters")


def _normalize_canonical_hostname(value: str) -> str:
    if ":" in value:
        return _normalize_ipv6_hostname(value)
    return _normalize_dns_hostname(value)


def _normalize_ipv6_hostname(value: str) -> str:
    try:
        address = IPv6Address(value)
        hostname = urlsplit(str(AnyHttpUrl(f"https://[{address}]/"))).hostname
    except (ValidationError, ValueError) as error:
        raise UrlValidationError("Allowed host must not contain a port") from error
    if hostname is None:
        raise UrlValidationError("Invalid hostname")
    return hostname


def _normalize_dns_hostname(value: str) -> str:
    hostname = value.removesuffix(".")
    if not hostname or len(hostname) > 253 or any(
        not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
        for label in hostname.split(".")
    ):
        raise UrlValidationError("Invalid DNS hostname")
    return hostname
