"""Frontier schema-migration regressions."""

from alembic.config import Config
from alembic.script import ScriptDirectory

CURRENT_REVISION = "0001_frontier_initial_schema"


def test_lease_ownership_is_the_current_alembic_head() -> None:
    script = ScriptDirectory.from_config(Config("alembic.ini"))

    assert tuple(script.get_heads()) == (CURRENT_REVISION,)
    revision = script.get_revision(CURRENT_REVISION)
    assert revision is not None and revision.down_revision is None
