import sys

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


def test_current_reports_head_after_upgrade(monkeypatch, clean_db, postgres_url, capsys):
    from lemongrass import _db, db
    monkeypatch.setattr(_db, "database_url", lambda: postgres_url)
    monkeypatch.setattr(sys, "argv", ["lemongrass-db", "upgrade"])
    db.main()
    capsys.readouterr()
    monkeypatch.setattr(sys, "argv", ["lemongrass-db", "current"])
    assert db.main() == 0
    assert "0001" in capsys.readouterr().out
