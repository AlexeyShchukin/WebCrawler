"""Extract bounded static text from untrusted HTML and XHTML."""
import codecs
import json
import re
from email.message import Message
from io import StringIO
from urllib.parse import urljoin, urlsplit

from pydantic import AnyHttpUrl, TypeAdapter, ValidationError
from selectolax.lexbor import LexborHTMLParser, LexborNode

from crawler_content.domain.types import (
    EVENT_ENVELOPE_RESERVE_BYTES,
    ExtractedPage,
    ExtractionLimits,
)

_HTTP_URL = TypeAdapter(AnyHttpUrl)
_CHARSET = re.compile(rb"\bcharset\s*=\s*['\"]?\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
_XML_ENCODING = re.compile(
    rb"<\?xml\b[^>]*\bencoding\s*=\s*['\"]([a-zA-Z0-9._-]+)",
    re.IGNORECASE
)
_META = re.compile(rb"<meta\b[^>]*>", re.IGNORECASE)
_HIDDEN_STYLE = re.compile(
    r"(?:^|;)\s*(?:display\s*:\s*none|visibility\s*:\s*hidden)\s*(?:!important\s*)?(?:;|$)",
    re.IGNORECASE
)
_EXCLUDED = frozenset({"script", "style", "template", "noscript", "head"})
_BLOCKS = frozenset({
    "address", "article", "aside", "blockquote", "br", "div", "dl", "dt", "dd", "fieldset",
    "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header",
    "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table", "td", "th", "tr", "ul",
})


class UnsupportedContentTypeError(ValueError):
    """The document is not one of Fetcher's supported HTML media types."""


def _decode(body: bytes, content_type: str) -> str:
    for marker, codec in ((codecs.BOM_UTF32_BE, "utf-32"), (codecs.BOM_UTF32_LE, "utf-32"),
                          (codecs.BOM_UTF8, "utf-8-sig"), (codecs.BOM_UTF16_BE, "utf-16"),
                          (codecs.BOM_UTF16_LE, "utf-16")):
        if body.startswith(marker):
            return body.decode(codec, errors="replace")
    message = Message()
    message["content-type"] = content_type
    charset = message.get_content_charset()
    if not charset:
        prefix = body[:1024]
        match = _XML_ENCODING.search(prefix)
        if not match:
            for meta in _META.finditer(prefix):
                match = _CHARSET.search(meta.group())
                if match:
                    break
        if match:
            charset = match.group(1).decode("ascii")
    try:
        return body.decode(charset or "utf-8", errors="replace")
    except (LookupError, TypeError):
        return body.decode("utf-8", errors="replace")


def _canonical(tree: LexborHTMLParser, final_url: str) -> str | None:
    for node in tree.css("link[rel][href]"):
        attrs = node.attributes
        if "canonical" not in (attrs.get("rel") or "").lower().split():
            continue
        hint = attrs.get("href") or ""
        if not hint or len(hint) > 16384 or any(ord(c) < 32 or ord(c) == 127 for c in hint):
            continue
        try:
            resolved = urljoin(final_url, hint)
            parsed = urlsplit(resolved)
            if len(resolved) > 16384 or parsed.username is not None or parsed.password is not None:
                continue
            return str(_HTTP_URL.validate_python(resolved))
        except (ValueError, ValidationError):
            continue
    return None


def _links(
        tree: LexborHTMLParser,
        final_url: str,
        limits: ExtractionLimits
) -> tuple[tuple[str, ...], bool]:
    """Extract an ordered prefix while reserving 8 KiB for the event envelope."""
    result: list[str] = []
    seen: set[str] = set()
    # ASCII-escaped JSON is an upper bound for the normal UTF-8 serialization.
    used_bytes = EVENT_ENVELOPE_RESERVE_BYTES
    for node in tree.css("a[href]"):
        href = node.attributes.get("href") or ""
        if len(href) > 16384 or any(ord(c) < 32 or ord(c) == 127 for c in href):
            continue
        try:
            resolved = urljoin(final_url, href)
            parsed = urlsplit(resolved)
            if len(resolved) > 16384 or parsed.username is not None or parsed.password is not None:
                continue
            candidate = str(_HTTP_URL.validate_python(resolved))
            if len(candidate) > 16384:
                continue
        except (ValueError, ValidationError):
            continue
        if candidate in seen:
            continue
        size = len(json.dumps(candidate, ensure_ascii=True).encode("ascii")) + 2
        if len(result) >= limits.max_links or used_bytes + size > limits.max_link_payload_bytes:
            return tuple(result), True
        seen.add(candidate)
        result.append(candidate)
        used_bytes += size
    return tuple(result), False


def _visible_text(root: LexborNode | None, limit: int) -> tuple[str, bool]:
    """Normalize incrementally, stopping once an additional character is known."""
    if root is None:
        return "", False
    output = StringIO()
    length = 0
    pending_space = False
    stack = [(root, False)]
    while stack:
        node, closing = stack.pop()
        if closing:
            pending_space = length > 0
            continue
        if node is not root and node.next is not None:
            stack.append((node.next, False))
        if node.tag == "-text":
            for character in node.text():
                if character == "\x01":
                    continue
                if character.isspace():
                    pending_space = length > 0
                    continue
                if pending_space:
                    if length == limit:
                        return output.getvalue(), True
                    output.write(" ")
                    length += 1
                    pending_space = False
                if length == limit:
                    return output.getvalue(), True
                output.write(character)
                length += 1
            continue
        attributes = node.attributes
        if (not (node is root and node.tag == "title") and (node.tag in _EXCLUDED or "hidden" in attributes
                                                            or (attributes.get(
                    "aria-hidden") or "").strip().lower() == "true"
                                                            or _HIDDEN_STYLE.search(attributes.get("style") or ""))):
            continue
        if node.tag in _BLOCKS:
            pending_space = length > 0
            stack.append((node, True))
        if node.first_child is not None:
            stack.append((node.first_child, False))
    return output.getvalue(), False


def extract_html(
        body: bytes,
        final_url: str,
        content_type: str,
        limits: ExtractionLimits
) -> ExtractedPage:
    """Decode and parse static HTML; unknown charsets fall back to UTF-8 replacement."""
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type not in {"text/html", "application/xhtml+xml"}:
        raise UnsupportedContentTypeError("Unsupported content type")
    # Preserve invalid NULs as a control character in attributes until URL
    # validation; stripping them first could turn an invalid URL into a valid one.
    html = _decode(body, content_type).replace("\x00", "\x01")
    tree = LexborHTMLParser(html)
    title_node = tree.css_first("title")
    title, title_truncated = _visible_text(title_node, limits.max_title_chars)
    canonical_url = _canonical(tree, final_url)
    links, links_truncated = _links(tree, final_url, limits)
    visible, text_truncated = _visible_text(tree.body, limits.max_text_chars)
    return ExtractedPage(
        title=title,
        text=visible,
        canonical_url=canonical_url,
        links=links,
        title_truncated=title_truncated,
        text_truncated=text_truncated,
        links_truncated=links_truncated,
    )
