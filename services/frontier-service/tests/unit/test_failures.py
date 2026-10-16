from types import SimpleNamespace

import pytest
from aio_pika.exceptions import ChannelInvalidStateError
from sqlalchemy.exc import DBAPIError

from crawler_frontier.failures import is_transient_failure


@pytest.mark.parametrize("sqlstate,expected", [
    ("40001", True), ("40P01", True), ("55P03", True), ("08006", True),
    ("23505", False), ("23503", False), ("42601", False),
])
def test_only_temporary_postgres_failures_are_retried(sqlstate, expected):
    error = DBAPIError("query", {}, SimpleNamespace(sqlstate=sqlstate))
    assert is_transient_failure(error) is expected


@pytest.mark.parametrize("error,expected", [
    (ChannelInvalidStateError("channel is recovering"), True),
    (RuntimeError("invalid publisher configuration"), False),
    (ValueError("invalid exchange"), False),
])
def test_closed_channel_is_retryable_without_retrying_all_runtime_errors(error, expected):
    assert is_transient_failure(error) is expected
