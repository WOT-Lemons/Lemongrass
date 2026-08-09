import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError


def _upgrade(url, revision="head"):
    from alembic import command

    from lemongrass import _db
    command.upgrade(_db.alembic_config(url), revision)


def _downgrade(url, revision="base"):
    from alembic import command

    from lemongrass import _db
    command.downgrade(_db.alembic_config(url), revision)


def test_script_location_is_inside_the_installed_package():
    # Migrations ship as package data so deployment is "run the CLI", not
    # "have a checkout with an alembic.ini next to you".
    import pathlib

    import lemongrass
    from lemongrass import _db
    loc = pathlib.Path(_db.alembic_config("postgresql://x/y")
                       .get_main_option("script_location"))
    assert loc.is_dir()
    assert loc.is_relative_to(pathlib.Path(lemongrass.__file__).parent)
    assert (loc / "versions").is_dir()
    # A future [tool.uv.build-backend] exclude could silently drop this out of
    # the wheel; the Dockerfile builds that same wheel, so `alembic revision`
    # against an installed lemongrass would fail with no template to render.
    assert (loc / "script.py.mako").is_file()


def test_upgrade_creates_the_expected_tables(clean_db, postgres_url):
    _upgrade(postgres_url)
    names = set(inspect(clean_db).get_table_names())
    assert {"races", "sessions", "alembic_version"} <= names


def test_upgrade_is_idempotent(clean_db, postgres_url):
    _upgrade(postgres_url)
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        assert conn.execute(text("SELECT count(*) FROM alembic_version")).scalar() == 1


def test_downgrade_removes_the_tables(clean_db, postgres_url):
    _upgrade(postgres_url)
    _downgrade(postgres_url)
    names = set(inspect(clean_db).get_table_names())
    assert "races" not in names
    assert "sessions" not in names


def test_sessions_cascade_and_fk(clean_db, postgres_url):
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO races (race_id, race_time) VALUES ('r1', now())"))
        conn.execute(text(
            "INSERT INTO sessions (session_id, race_id) VALUES (10, 'r1')"))
    # The foreign key is enforced. This runs in its own transaction so the
    # aborted statement doesn't roll back the rows committed above.
    with pytest.raises(IntegrityError):
        with clean_db.begin() as conn:
            conn.execute(text(
                "INSERT INTO sessions (session_id, race_id) VALUES (11, 'nope')"))
    with clean_db.begin() as conn:
        assert conn.execute(text("SELECT count(*) FROM sessions")).scalar() == 1
        conn.execute(text("DELETE FROM races WHERE race_id = 'r1'"))
        assert conn.execute(text("SELECT count(*) FROM sessions")).scalar() == 0


def test_defaults_land_for_omitted_text_columns(clean_db, postgres_url):
    # An Influx race point written from an unsuccessful details fetch has no
    # name/track_name tag at all; the import must be able to insert it.
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO races (race_id, race_time) VALUES ('r2', now())"))
        row = conn.execute(text(
            "SELECT name, track_name, updated_at FROM races WHERE race_id='r2'"
        )).one()
    assert row.name == ""
    assert row.track_name == ""
    assert row.updated_at is not None
