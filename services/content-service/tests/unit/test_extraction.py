"""Static, bounded extraction of page metadata and visible text."""

from dataclasses import FrozenInstanceError

import pytest

from crawler_content.domain.extraction import (
    UnsupportedContentTypeError,
    _visible_text,
    extract_html,
)
from crawler_content.domain.types import ExtractionLimits


def extract(body, content_type="text/html", limits=None):
    return extract_html(body, "https://example.test/path/page", content_type, limits or ExtractionLimits())


def test_extract_static_text_title_and_canonical():
    page = extract(b'<title>  Example  title </title><link rel="canonical" href="../canonical">'
                   b'<body>Hello <b>world</b><script>evil()</script><style>x</style>'
                   b'<template>inert</template><div hidden>secret</div>'
                   b'<div aria-hidden="true">private</div><p> goodbye </p></body>')
    assert page.title == "Example title"
    assert page.text == "Hello world goodbye"
    assert page.canonical_url == "https://example.test/canonical"
    assert page.links == ()
    with pytest.raises(FrozenInstanceError):
        page.title = "changed"


def test_static_hidden_subtrees_and_nul_are_excluded():
    page = extract(b'<body>A\x00B<div style="display: none"><p>secret</p></div>'
                   b'<span style="visibility:hidden">hidden</span><p>C</p></body>')
    assert page.text == "AB C"


@pytest.mark.parametrize("body,content_type,expected", [
    (b'\xef\xbb\xbf<title>caf\xc3\xa9</title>', 'text/html; charset=windows-1252', 'caf\u00e9'),
    (b'<title>caf\xe9</title>', 'text/html; charset=windows-1252', 'caf\u00e9'),
    (b'<meta charset="windows-1252"><title>caf\xe9</title>', 'text/html', 'caf\u00e9'),
    (b'<?xml version="1.0" encoding="windows-1252"?><title>caf\xe9</title>', 'application/xhtml+xml', 'caf\u00e9'),
    (b'<title>bad\xff</title>', 'text/html; charset=unknown-encoding', 'bad\ufffd'),
])
def test_decoding_precedence_and_fallback(body, content_type, expected):
    assert extract(body, content_type).title == expected


def test_bounded_normalized_title_and_text():
    page = extract(b'<title> abc def </title><body>abc def ghi</body>', limits=ExtractionLimits(max_title_chars=4, max_text_chars=5))
    assert page.title == 'abc ' and page.title_truncated
    assert page.text == 'abc d' and page.text_truncated


def test_empty_and_malformed_html_are_tolerated():
    assert extract(b'').title == ''
    assert extract(b'<body><p>First<p>Second').text == 'First Second'


def test_inline_text_keeps_word_boundaries_and_blocks_are_separated():
    page = extract(b'<body><p>inter<strong>national</strong> text</p><p>next<br>line</p></body>')
    assert page.text == 'international text next line'


@pytest.mark.parametrize("hint", ["javascript:alert(1)", "https://u:p@example.test/path", "https://example.test/\nfoo"])
def test_canonical_hint_is_untrusted(hint):
    page = extract(f'<link rel="canonical" href="{hint}"><body>safe</body>'.encode())
    assert page.canonical_url is None


def test_unsupported_mime_rejected():
    with pytest.raises(UnsupportedContentTypeError):
        extract(b"irrelevant", "application/pdf")


def test_nul_in_canonical_is_not_erased_into_a_valid_url():
    page = extract(b'<link rel="canonical" href="https://example.test/a\x00b"><body>safe</body>')
    assert page.canonical_url is None


def test_resolves_links_against_final_url_and_retains_self_cycles():
    page = extract(b'<base href="https://attacker.test/"><body>'
                   b'<a href="../next">one</a><a href="//other.test/a">two</a>'
                   b'<a href="#top">self</a><a href="">empty self</a>'
                   b'<a href="../next">duplicate</a><a href="https://example.test/path/page">cycle</a>')
    assert page.links == (
        "https://example.test/next", "https://other.test/a", "https://example.test/path/page#top",
        "https://example.test/path/page",
    )
    assert not page.links_truncated


def test_rejects_malformed_unsafe_and_control_character_links():
    page = extract(b'<a href="javascript:alert(1)">a</a><a href="mailto:a@b.test">b</a>'
                   b'<a href="https://u:p@example.test/">c</a><a href="http://[bad">d</a>'
                   b'<a href="/bad\x00path">e</a><a href="/bad\npath">f</a>'
                   b'<a href="/valid">g</a>')
    assert page.links == ("https://example.test/valid",)


def test_links_limited_by_count_after_deduplication():
    page = extract(b'<a href="/a">a</a><a href="/a">a</a><a href="/b">b</a>'
                   b'<a href="/c">c</a>', limits=ExtractionLimits(max_links=2))
    assert page.links == ("https://example.test/a", "https://example.test/b")
    assert page.links_truncated


def test_duplicate_at_limit_does_not_mark_truncation():
    page = extract(b'<a href="/a">a</a><a href="/a">b</a>', limits=ExtractionLimits(max_links=1))
    assert page.links == ("https://example.test/a",)
    assert not page.links_truncated


def test_link_payload_limit_preserves_prefix_and_envelope_budget():
    import json
    from uuid import uuid4

    from crawler_contracts import LinksExtractedEvent

    limits = ExtractionLimits(max_link_payload_bytes=8260)
    page = extract(b'<a href="/first">a</a><a href="/second">b</a><a href="/third">c</a>', limits=limits)
    assert page.links == ("https://example.test/first", "https://example.test/second")
    assert page.links_truncated
    event = LinksExtractedEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1, links=page.links)
    assert len(json.dumps(event.model_dump(mode="json")).encode("utf-8")) <= limits.max_link_payload_bytes


def test_link_length_is_checked_before_and_after_url_validation():
    oversized = b"/" + b"a" * 16384
    expanded = ("/" + "é" * 6000).encode()
    page = extract(b'<a href="' + oversized + b'">x</a><a href="' + expanded + b'">y</a><a href="/ok">z</a>')
    assert page.links == ("https://example.test/ok",)


def test_maximum_event_payload_includes_serialization_overhead():
    import json
    from uuid import uuid4

    from crawler_contracts import LinksExtractedEvent

    body = b"".join(f'<a href="/{i}/'.encode() + b"x" * 5000 + b'">x</a>' for i in range(300))
    page = extract(body)
    assert page.links_truncated
    event = LinksExtractedEvent(crawl_id=uuid4(), url_id=uuid4(), fetch_attempt=1, links=page.links)
    assert len(json.dumps(event.model_dump(mode="json")).encode("utf-8")) <= 1048576


def test_extraction_never_opens_network_connections(monkeypatch):
    import socket

    def reject(*args, **kwargs):
        raise AssertionError("HTML parsing attempted network access")

    monkeypatch.setattr(socket.socket, "connect", reject)
    page = extract(b'<link rel="canonical" href="https://remote.test/canonical">'
                   b'<img src="https://remote.test/image"><script src="https://remote.test/script"></script>'
                   b'<a href="https://remote.test/link">link</a>')
    assert page.canonical_url == "https://remote.test/canonical"
    assert page.links == ("https://remote.test/link",)


def test_html_entities_and_hidden_nested_content():
    page = extract(b'<title>One &amp; Two</title><body><div hidden><p>secret</p></div>'
                   b'<p>Hello &amp; welcome</p><a href="/path?a=1&amp;b=2">link</a></body>')
    assert page.title == "One & Two"
    assert page.text == "Hello & welcome link"
    assert page.links == ("https://example.test/path?a=1&b=2",)


@pytest.mark.parametrize("body,limit,expected,truncated", [
    (b"<body>abc   </body>", 3, "abc", False),
    (b"<body>abc <b> </b> d</body>", 3, "abc", True),
    (b"<body>ab <b>c</b> d</body>", 3, "ab ", True),
    (b"<body>  abc  def  </body>", 7, "abc def", False),
    (b"<body> ab\x00cd </body>", 4, "abcd", False),
])
def test_bounded_text_normalization_at_boundaries(body, limit, expected, truncated):
    page = extract(body, limits=ExtractionLimits(max_text_chars=limit))
    assert (page.text, page.text_truncated) == (expected, truncated)


def test_visible_text_stops_before_visiting_remaining_nodes():
    class TextNode:
        tag = "-text"
        next = None

        def __init__(self, value):
            self.value = value

        def text(self):
            if self.value is None:
                raise AssertionError("visited text after truncation was established")
            return self.value

    first = TextNode("a" * 100)
    first.next = TextNode(None)
    root = type("Root", (), {"tag": "body", "attributes": {}, "first_child": first})()
    assert _visible_text(root, 3) == ("aaa", True)


@pytest.mark.parametrize("raw,limit,expected,truncated", [
    (b" abc   ", 3, "abc", False),
    (b" abc  def", 3, "abc", True),
    (b" ab cdef", 3, "ab ", True),
    (b" ab\x00cd ", 4, "abcd", False),
])
def test_title_uses_bounded_normalization(raw, limit, expected, truncated, monkeypatch):
    from crawler_content.domain import extraction

    original = extraction._visible_text
    calls = []

    def observe(root, max_chars):
        if root is not None and root.tag == "title":
            calls.append(max_chars)
        return original(root, max_chars)

    monkeypatch.setattr(extraction, "_visible_text", observe)
    page = extract(b"<title>" + raw + b"</title>", limits=ExtractionLimits(max_title_chars=limit))
    assert (page.title, page.title_truncated) == (expected, truncated)
    assert calls == [limit]


def test_oversized_title_is_truncated():
    page = extract(b"<title>" + b"a" * 1000000 + b"</title>", limits=ExtractionLimits(max_title_chars=8))
    assert (page.title, page.title_truncated) == ("aaaaaaaa", True)


def test_title_metadata_is_extracted_even_with_hidden_attribute():
    assert extract(b'<title hidden>Example</title><body>Text</body>').title == "Example"


def test_xml_encoding_capture_and_declared_charset_are_honored():
    from crawler_content.domain.extraction import _XML_ENCODING

    declaration = b'<?xml version="1.0" encoding="windows-1252"?>'
    assert _XML_ENCODING.search(declaration).group(1) == b"windows-1252"
    assert extract(declaration + b'<title>caf\xe9</title>', "application/xhtml+xml").title == "café"
