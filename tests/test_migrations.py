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


def test_migration_head_matches_the_schema_module(clean_db, postgres_url):
    # Migrations are hand-written (autogenerate renders a rename as
    # drop-plus-add, which destroys data), so nothing mechanically keeps them
    # in step with _schema.py. Without this, a new revision that forgets a
    # column fails at runtime — _db's statements name a column the database
    # does not have — instead of in CI.
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from lemongrass import _schema
    _upgrade(postgres_url)
    with clean_db.connect() as conn:
        diff = compare_metadata(
            MigrationContext.configure(conn), _schema.metadata)
    # alembic_version is not in _schema.metadata, so it shows up as an extra
    # table on every run; it is Alembic's own bookkeeping, not drift.
    diff = [d for d in diff
            if not (isinstance(d, tuple) and len(d) == 2
                    and d[0] == 'remove_table'
                    and d[1].name == 'alembic_version')]
    assert diff == [], f"schema drift between _schema.py and the migrations: {diff}"


def test_upgrade_creates_the_identity_tables(clean_db, postgres_url):
    _upgrade(postgres_url)
    names = set(inspect(clean_db).get_table_names())
    assert {"venues", "layouts", "events", "teams", "team_aliases",
            "entries"} <= names


def test_downgrade_removes_the_identity_tables_and_columns(clean_db, postgres_url):
    _upgrade(postgres_url)
    _downgrade(postgres_url, "0001")
    insp = inspect(clean_db)
    names = set(insp.get_table_names())
    assert not ({"venues", "layouts", "events", "teams", "team_aliases",
                 "entries"} & names)
    assert "races" in names
    columns = {c["name"] for c in insp.get_columns("races")}
    assert not ({"venue_id", "layout_id", "event_id"} & columns)


def _seed_identity(conn):
    conn.execute(text("INSERT INTO venues (venue_id, name) VALUES ('njmp', 'NJMP')"))
    conn.execute(text("INSERT INTO venues (venue_id, name) VALUES ('other', 'Other')"))
    conn.execute(text("INSERT INTO layouts (venue_id, layout_id, name) "
                      "VALUES ('njmp', 'thunderbolt', 'Thunderbolt Course')"))
    conn.execute(text("INSERT INTO races (race_id, race_time) VALUES ('r1', now())"))


def test_venue_without_layout_is_allowed(clean_db, postgres_url):
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        _seed_identity(conn)
        conn.execute(text(
            "UPDATE races SET venue_id='njmp' WHERE race_id='r1'"))
        assert conn.execute(text(
            "SELECT layout_id FROM races WHERE race_id='r1'")).scalar() is None


def test_layout_without_venue_is_rejected_by_the_check(clean_db, postgres_url):
    # MATCH SIMPLE skips the composite FK check when any column is NULL, so the
    # composite key alone would silently permit this orphan.
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        _seed_identity(conn)
    with pytest.raises(IntegrityError):
        with clean_db.begin() as conn:
            conn.execute(text(
                "UPDATE races SET layout_id='thunderbolt' WHERE race_id='r1'"))


def test_layout_belonging_to_another_venue_is_rejected(clean_db, postgres_url):
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        _seed_identity(conn)
    with pytest.raises(IntegrityError):
        with clean_db.begin() as conn:
            conn.execute(text(
                "UPDATE races SET venue_id='other', layout_id='thunderbolt' "
                "WHERE race_id='r1'"))


def test_entries_are_unique_per_race_and_car(clean_db, postgres_url):
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO races (race_id, race_time) VALUES ('r1', now())"))
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('a', 'A')"))
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('b', 'B')"))
        conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                          "VALUES ('r1', '252', 'a')"))
        # Two cars for one team in one race is two rows, no schema change.
        conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                          "VALUES ('r1', '253', 'a')"))
    with pytest.raises(IntegrityError):
        with clean_db.begin() as conn:
            conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                              "VALUES ('r1', '252', 'b')"))


def test_one_alias_cannot_map_to_two_teams(clean_db, postgres_url):
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('a', 'A')"))
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('b', 'B')"))
        conn.execute(text("INSERT INTO team_aliases (team_id, alias) "
                          "VALUES ('a', 'wot lemons')"))
    with pytest.raises(IntegrityError):
        with clean_db.begin() as conn:
            conn.execute(text("INSERT INTO team_aliases (team_id, alias) "
                              "VALUES ('b', 'wot lemons')"))


def test_deleting_a_race_cascades_to_its_entries(clean_db, postgres_url):
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO races (race_id, race_time) VALUES ('r1', now())"))
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('a', 'A')"))
        conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                          "VALUES ('r1', '252', 'a')"))
        conn.execute(text("DELETE FROM races WHERE race_id='r1'"))
        assert conn.execute(text("SELECT count(*) FROM entries")).scalar() == 0


def test_a_referenced_team_cannot_be_deleted(clean_db, postgres_url):
    # entries.team_id is NO ACTION on purpose: dropping a team must go through
    # `teams merge`, which reassigns its entries first.
    _upgrade(postgres_url)
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO races (race_id, race_time) VALUES ('r1', now())"))
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('a', 'A')"))
        conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                          "VALUES ('r1', '252', 'a')"))
    with pytest.raises(IntegrityError):
        with clean_db.begin() as conn:
            conn.execute(text("DELETE FROM teams WHERE team_id='a'"))
