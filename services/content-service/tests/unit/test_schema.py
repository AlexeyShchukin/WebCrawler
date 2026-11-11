import importlib.util
from pathlib import Path

from sqlalchemy import CheckConstraint, Index, MetaData, Table, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from crawler_content.models import Base


def test_content_tables_and_local_foreign_keys():
    assert set(Base.metadata.tables) == {
        "pages", "keywords", "page_keywords", "outbox_events", "processed_events"
    }
    assert all(fk.column.table.name in Base.metadata.tables for table in Base.metadata.tables.values() for fk in table.foreign_keys)


def test_identity_and_reliability_constraints():
    def uniques(name):
        return {tuple(column.name for column in item.columns) for item in Base.metadata.tables[name].constraints if isinstance(item, UniqueConstraint)}

    assert ("url_id",) in uniques("pages")
    assert ("normalized_term",) in uniques("keywords")
    assert ("consumer_name", "event_id") in uniques("processed_events")
    assert {c.name for c in Base.metadata.tables["page_keywords"].primary_key} == {"page_id", "keyword_id"}
    assert all(any(isinstance(c, CheckConstraint) for c in Base.metadata.tables[name].constraints) for name in ("pages", "page_keywords"))


def test_postgresql_ddl_and_pending_index():
    dialect = postgresql.dialect()
    for table in Base.metadata.sorted_tables:
        assert "CREATE TABLE" in str(CreateTable(table).compile(dialect=dialect))
    index = next(i for i in Base.metadata.tables["outbox_events"].indexes if i.name == "ix_outbox_events_pending")
    assert "WHERE published_at IS NULL" in str(CreateIndex(index).compile(dialect=dialect))


def test_migration_matches_model_columns(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "alembic/versions/0001_content_initial_schema.py"
    spec = importlib.util.spec_from_file_location("content_initial_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    metadata = MetaData()

    class Recorder:
        def create_table(self, name, *items):
            Table(name, metadata, *items)

        def create_index(self, name, table, columns, **kwargs):
            Index(name, *(metadata.tables[table].c[column] for column in columns), **kwargs)

    monkeypatch.setattr(module, "op", Recorder())
    module.upgrade()
    assert set(metadata.tables) == set(Base.metadata.tables)
    dialect = postgresql.dialect()
    for name, actual in metadata.tables.items():
        expected = Base.metadata.tables[name]
        assert set(actual.c.keys()) == set(expected.c.keys())
        for column in actual.c:
            other = expected.c[column.name]
            assert str(column.type.compile(dialect=dialect)) == str(other.type.compile(dialect=dialect))
            assert column.nullable == other.nullable
        assert {c.name for c in actual.constraints if c.name} == {c.name for c in expected.constraints if c.name}
        assert {i.name for i in actual.indexes} == {i.name for i in expected.indexes}
