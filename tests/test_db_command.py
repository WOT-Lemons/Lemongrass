import logging
import sys
from unittest.mock import patch

from sqlalchemy import inspect


def test_usage_error_without_subcommand(monkeypatch, capsys):
    from lemongrass import db
    monkeypatch.setattr(sys, "argv", ["lemongrass-db"])
    assert db.main() == 1
    assert "Subcommands:" in capsys.readouterr().out


def test_usage_error_on_unknown_subcommand(monkeypatch, capsys):
    from lemongrass import db
    monkeypatch.setattr(sys, "argv", ["lemongrass-db", "frobnicate"])
    assert db.main() == 1
    assert "Subcommands:" in capsys.readouterr().out


def test_upgrade_creates_tables(monkeypatch, clean_db, postgres_url):
    from lemongrass import _db, db
    monkeypatch.setattr(_db, "database_url", lambda: postgres_url)
    monkeypatch.setattr(sys, "argv", ["lemongrass-db", "upgrade"])
    assert db.main() == 0
    assert "races" in inspect(clean_db).get_table_names()


def test_current_reports_head_after_upgrade(monkeypatch, clean_db, postgres_url, capsys, caplog):
    from lemongrass import _db, db
    monkeypatch.setattr(_db, "database_url", lambda: postgres_url)
    monkeypatch.setattr(sys, "argv", ["lemongrass-db", "upgrade"])
    # alembic reports the applied revision through the "alembic" logger, not
    # print, so this is what actually needs asserting on for finding 1: that
    # `db upgrade` is not silent about what it did.
    with caplog.at_level(logging.INFO):
        assert db.main() == 0
    assert "0001" in caplog.text
    capsys.readouterr()
    monkeypatch.setattr(sys, "argv", ["lemongrass-db", "current"])
    assert db.main() == 0
    assert "0001" in capsys.readouterr().out


def test_import_legacy_dispatches_and_prints_counts(monkeypatch, capsys):
    from lemongrass import db as db_mod
    summary = {'races_read': 3, 'races_written': 3, 'sessions_read': 4,
               'sessions_written': 3, 'sessions_skipped': 1,
               'orphan_race_ids': ['64202']}
    monkeypatch.setattr(sys, 'argv', ['lemongrass-db', 'import-legacy', '--dry-run'])
    with patch('lemongrass._influx.connect'), \
         patch('lemongrass._legacy_migration.import_legacy',
               return_value=summary) as run, \
         patch('lemongrass._db.list_races', return_value=[]), \
         patch('lemongrass._db.list_sessions', return_value=[]):
        assert db_mod.main() == 0
    assert run.call_args.kwargs['dry_run'] is True
    out = capsys.readouterr().out
    assert '64202' in out
