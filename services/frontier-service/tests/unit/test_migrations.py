"""Frontier's squashed initial-schema migration regressions."""

from alembic.config import Config
from alembic.script import ScriptDirectory

INITIAL_REVISION = "0001_frontier_initial_schema"


def test_initial_schema_is_the_only_alembic_head() -> None:
    """Catch reintroduction of an incremental chain after history was squashed."""
    script = ScriptDirectory.from_config(Config("alembic.ini"))

    assert tuple(script.get_heads()) == (INITIAL_REVISION,)
    revision = script.get_revision(INITIAL_REVISION)
    assert revision is not None and revision.down_revision is None
