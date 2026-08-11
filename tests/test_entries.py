import sys
from unittest.mock import patch


def _race(db, race_id="r1"):
    from datetime import UTC, datetime

    from lemongrass import _db
    _db.upsert_race(_db.RaceRow(race_id=race_id,
                                race_time=datetime(2024, 5, 1, tzinfo=UTC)))


def test_set_entry_upserts_and_trims_the_car_number(db):
    from lemongrass import _db
    _race(db)
    _db.upsert_team("a", "A")
    _db.upsert_team("b", "B")
    # ' 2' whitespace has reached the tag layer before; trim on write.
    _db.set_entry("r1", " 252 ", "a")
    assert [(e.car_number, e.team_id) for e in _db.list_entries()] == [
        ("252", "a")]
    _db.set_entry("r1", "252", "b")
    assert [(e.car_number, e.team_id) for e in _db.list_entries()] == [
        ("252", "b")]
    assert _db.get_entry("r1", " 252").team_id == "b"
    assert _db.get_entry("r1", "999") is None


def test_two_cars_in_one_race_and_the_same_number_across_races(db):
    from lemongrass import _db
    _race(db, "r1")
    _race(db, "r2")
    _db.upsert_team("us", "Us")
    _db.upsert_team("them", "Them")
    _db.set_entry("r1", "252", "us")
    _db.set_entry("r1", "253", "us")
    _db.set_entry("r2", "252", "them")
    assert len(_db.list_entries(team_id="us")) == 2
    assert len(_db.list_entries(race_id="r2")) == 1


def _run(argv):
    from lemongrass import entries
    with patch.object(sys, "argv", argv):
        return entries.main()


def test_cli_set_and_list(capsys):
    from lemongrass import _db
    with patch("lemongrass._db.set_entry") as write:
        assert _run(["lemongrass-entries", "set", "r1", "252",
                     "--team", "wot-lemons"]) == 0
    write.assert_called_once_with("r1", "252", "wot-lemons")

    with patch("lemongrass._db.list_entries",
               return_value=[_db.EntryRow("r1", "252", "wot-lemons")]):
        assert _run(["lemongrass-entries", "list"]) == 0
    assert "252" in capsys.readouterr().out


def test_cli_set_defaults_to_the_configured_team(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[team]\nid = "wot-lemons"\n', encoding="utf-8")
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    with patch("lemongrass._db.set_entry") as write:
        assert _run(["lemongrass-entries", "set", "r1", "252"]) == 0
    write.assert_called_once_with("r1", "252", "wot-lemons")


def test_cli_set_without_a_team_anywhere_fails(monkeypatch, capsys):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    assert _run(["lemongrass-entries", "set", "r1", "252"]) == 1
    assert "team" in capsys.readouterr().err


def test_entries_is_a_registered_command():
    from lemongrass import cli
    assert cli._COMMANDS["entries"] == "lemongrass.entries"
