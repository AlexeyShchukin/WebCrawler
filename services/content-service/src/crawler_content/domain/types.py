"""Immutable values returned by content extraction."""
from dataclasses import dataclass

EVENT_ENVELOPE_RESERVE_BYTES = 8192


@dataclass(frozen=True, slots=True)
class ExtractionLimits:
    max_title_chars: int = 2048
    max_text_chars: int = 1000000
    max_links: int = 1000
    max_link_payload_bytes: int = 1048576

    def __post_init__(self) -> None:
        if min(self.max_title_chars, self.max_text_chars, self.max_links) <= 0:
            raise ValueError("Extraction limits must be positive")
        if self.max_link_payload_bytes <= EVENT_ENVELOPE_RESERVE_BYTES:
            raise ValueError("Link payload limit must reserve space for the event envelope")


@dataclass(frozen=True, slots=True)
class ExtractedPage:
    title: str
    text: str
    canonical_url: str | None
    links: tuple[str, ...]
    title_truncated: bool
    text_truncated: bool
    links_truncated: bool = False
