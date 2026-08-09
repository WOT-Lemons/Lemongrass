from sqlalchemy import BigInteger, DateTime, Integer, Text

from lemongrass import _schema


def test_races_columns_and_types():
    c = _schema.races.c
    assert isinstance(c.race_id.type, Text)
    assert c.race_id.primary_key
    assert isinstance(c.session_count.type, Integer)
    assert isinstance(c.race_time.type, DateTime)
    assert c.race_time.type.timezone is True
    assert c.race_time.nullable is False
    assert c.end_time.nullable is True
    # Empty-string tags vanish in Influx, so the import must be able to land ''
    # rather than being rejected. Not-null with a default, not bare not-null.
    assert c.name.nullable is False
    assert c.track_name.nullable is False


def test_sessions_columns_and_fk():
    c = _schema.sessions.c
    assert isinstance(c.session_id.type, BigInteger)
    assert c.session_id.primary_key
    fk = next(iter(c.race_id.foreign_keys))
    assert fk.column is _schema.races.c.race_id
    assert fk.ondelete == "CASCADE"


def test_constraints_are_named():
    # Alembic downgrades need every constraint and index to have a stable name.
    for table in (_schema.races, _schema.sessions):
        for constraint in table.constraints:
            assert constraint.name, f"unnamed constraint on {table.name}"
        for index in table.indexes:
            assert index.name, f"unnamed index on {table.name}"


def test_schema_module_does_not_import_lemongrass_internals():
    # _schema must stay a leaf so _db and Alembic's env.py can both import it.
    import ast
    import pathlib
    src = pathlib.Path(_schema.__file__).read_text()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("lemongrass"):
            raise AssertionError(f"_schema imports {node.module}")
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("lemongrass")
