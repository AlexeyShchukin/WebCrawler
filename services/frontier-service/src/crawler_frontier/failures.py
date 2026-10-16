"""Separate temporary infrastructure failures from permanent processing errors."""

from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

_RETRYABLE_SQLSTATES = frozenset({
    "40001", "40P01", "55P03", "57014", "53000", "53100", "53200", "53300", "53400",
    "57P01", "57P02", "57P03",
})


def is_transient_failure(error: Exception) -> bool:
    if isinstance(error, DBAPIError):
        sqlstate = getattr(error.orig, "sqlstate", None) or getattr(error.orig, "pgcode", None)
        if sqlstate:
            return sqlstate.startswith("08") or sqlstate in _RETRYABLE_SQLSTATES
        return error.connection_invalidated or isinstance(error, (OperationalError, InterfaceError))
    return isinstance(error, (OSError, TimeoutError, PoolTimeoutError, AMQPError, ChannelInvalidStateError))
