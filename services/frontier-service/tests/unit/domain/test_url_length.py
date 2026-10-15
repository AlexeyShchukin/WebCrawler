"""Configured input and canonical length limits apply at every admission boundary."""

import pytest

from crawler_frontier.domain.url_policy import UrlValidationError, normalize_url


def test_canonical_url_expansion_also_obeys_the_length_limit():
    with pytest.raises(UrlValidationError):
        normalize_url("https://example.test/é", max_length=22)
