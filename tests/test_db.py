import subprocess
import sys

import pytest
from sqlalchemy import text


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    from lemongrass import _db
    _db.reset_engine()
    yield
    _db.reset_engine()


def test_import_creates_no_engine():
    # Importing must not create an engine: every command imports the CLI,
    # including ones that never use a database.
    from lemongrass import _db
    assert _db._engine is None


def test_database_url_uses_config_and_env(monkeypatch, tmp_path):
    cfg = tmp_path / "lemongrass.toml"
    cfg.write_text(
        "[postgres]\n"
        'host = "db.internal"\n'
        "port = 6543\n"
        'database = "lg"\n'
        'user = "svc"\n'
        'password_env = "PGPASS"\n'
    )
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    monkeypatch.setenv("PGPASS", "s3cret")
    from lemongrass import _db
    url = _db.database_url()
    assert url.drivername == "postgresql+psycopg"
    assert url.host == "db.internal"
    assert url.port == 6543
    assert url.database == "lg"
    assert url.username == "svc"
    assert url.password == "s3cret"


def test_database_url_exits_when_password_unset(monkeypatch):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    monkeypatch.delenv("LEMONGRASS_DB_PASSWORD", raising=False)
    from lemongrass import _db
    with pytest.raises(SystemExit) as exc:
        _db.database_url()
    assert exc.value.code == 1


def test_db_password_present_does_not_exit(monkeypatch):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    monkeypatch.delenv("LEMONGRASS_DB_PASSWORD", raising=False)
    from lemongrass import _db
    assert _db.db_password_present() is False
    monkeypatch.setenv("LEMONGRASS_DB_PASSWORD", "x")
    assert _db.db_password_present() is True


def test_engine_is_memoized(monkeypatch, postgres_url):
    monkeypatch.setattr("lemongrass._db.database_url", lambda: postgres_url)
    from lemongrass import _db
    assert _db.engine() is _db.engine()


def test_connect_executes_in_a_transaction(monkeypatch, postgres_url, clean_db):
    monkeypatch.setattr("lemongrass._db.database_url", lambda: postgres_url)
    from lemongrass import _db
    with _db.connect() as conn:
        conn.execute(text("CREATE TABLE t (id int)"))
        conn.execute(text("INSERT INTO t VALUES (1)"))
    with _db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM t")).scalar() == 1


def test_connect_rolls_back_on_error(monkeypatch, postgres_url, clean_db):
    monkeypatch.setattr("lemongrass._db.database_url", lambda: postgres_url)
    from lemongrass import _db
    with _db.connect() as conn:
        conn.execute(text("CREATE TABLE t (id int)"))
    with pytest.raises(RuntimeError):
        with _db.connect() as conn:
            conn.execute(text("INSERT INTO t VALUES (1)"))
            raise RuntimeError("boom")
    with _db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM t")).scalar() == 0


@pytest.mark.parametrize("module", ["lemongrass.telem", "lemongrass.pisugar_monitor"])
def test_pi_commands_do_not_import_db(module):
    # telem and pisugar-monitor run on the Raspberry Pi, which reaches InfluxDB
    # through an HTTP tunnel that cannot carry the Postgres wire protocol. They
    # must never acquire a database dependency, even transitively.
    code = (
        f"import sys, {module}; "
        "sys.exit(1 if 'lemongrass._db' in sys.modules else 0)"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0
