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
    summary = {'races_read': 3, 'races_written': 3, 'races_would_write': 3,
               'races_skipped_existing': 0, 'sessions_read': 4,
               'sessions_written': 3, 'sessions_would_write': 3,
               'sessions_skipped_existing': 0, 'sessions_skipped': 1,
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


def test_export_legacy_dispatches_and_writes_to_stdout(monkeypatch, capsys):
    from lemongrass import db as db_mod
    monkeypatch.setattr(sys, 'argv', ['lemongrass-db', 'export-legacy'])
    with patch('lemongrass._legacy_migration.export_legacy',
               return_value={'races': 2, 'sessions': 3}) as run:
        assert db_mod.main() == 0
    assert run.call_args.args[0] is sys.stdout
    err = capsys.readouterr().err
    assert 'exported 2 race(s), 3 session(s)' in err


def test_export_legacy_writes_to_the_output_file(monkeypatch, tmp_path):
    from lemongrass import db as db_mod
    out_path = tmp_path / 'races.lp'
    monkeypatch.setattr(sys, 'argv',
                        ['lemongrass-db', 'export-legacy', '--output', str(out_path)])
    def fake_export(out):
        out.write('race,race_id=1 x=1i 0\n')
        return {'races': 1, 'sessions': 0}

    with patch('lemongrass._legacy_migration.export_legacy', side_effect=fake_export):
        assert db_mod.main() == 0
    assert out_path.read_text() == 'race,race_id=1 x=1i 0\n'
