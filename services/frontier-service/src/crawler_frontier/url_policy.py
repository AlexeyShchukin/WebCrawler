"""URL normalization and crawl-scope policy for Frontier."""

from collections.abc import Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit


class UrlValidationError(ValueError):
    """Raised when a URL cannot enter the crawl frontier."""


_DEFAULT_PORTS = {"http": 80, "https": 443}
_ALLOWED_SCHEMES = frozenset({"http", "https"})


def normalize_url(value: str, *, base_url: str | None = None, max_length: int = 4096) -> str:
    """Return a canonical absolute HTTP(S) URL using only safe transformations."""
    if not isinstance(value, str) or not value:
        raise UrlValidationError("URL must be a non-empty string")
    if max_length < 1:
        raise ValueError("max_length must be positive")

    candidate = urljoin(base_url, value) if base_url is not None else value
    if len(candidate) > max_length:
        raise UrlValidationError("URL exceeds the configured size limit")
    if any(character.isspace() or ord(character) < 32 for character in candidate):
        raise UrlValidationError("URL must not contain whitespace or control characters")

    try:
        parsed = urlsplit(candidate)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise UrlValidationError("URL has an invalid hostname or port") from error

    scheme = parsed.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise UrlValidationError("URL scheme must be http or https")
    if parsed.username is not None or parsed.password is not None:
        raise UrlValidationError("URL userinfo is not allowed")
    if hostname is None:
        raise UrlValidationError("URL must include a hostname")

    normalized_host = hostname.lower()
    if ":" in normalized_host and not normalized_host.startswith("["):
        normalized_host = f"[{normalized_host}]"

    normalized_port = "" if port in (None, _DEFAULT_PORTS[scheme]) else f":{port}"
    normalized_path = parsed.path or "/"
    return urlunsplit((scheme, f"{normalized_host}{normalized_port}", normalized_path, parsed.query, ""))


def is_allowed_url(
    value: str,
    *,
    seed_urls: Iterable[str],
    additional_allowed_hosts: Iterable[str] = (),
) -> bool:
    """Return whether a URL has a hostname explicitly allowed for this crawl."""
    candidate_host = _hostname(normalize_url(value))
    seed_hosts = {_hostname(normalize_url(seed_url)) for seed_url in seed_urls}
    extra_hosts = {_normalize_allowed_host(host) for host in additional_allowed_hosts}
    return candidate_host in seed_hosts | extra_hosts


def _hostname(value: str) -> str:
    hostname = urlsplit(value).hostname
    if hostname is None:
        raise UrlValidationError("URL must include a hostname")
    return hostname.lower()


def _normalize_allowed_host(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise UrlValidationError("Allowed host must be a non-empty hostname")
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise UrlValidationError("Allowed host must not contain whitespace or control characters")
    if any(character in value for character in ("://", "/", "@", "?", "#")):
        raise UrlValidationError("Allowed host must be a hostname without URL components")
    return value.lower()
