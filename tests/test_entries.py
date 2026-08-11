import sys
from unittest.mock import patch

import pytest


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
    with patch("lemongrass._db.set_entry") as write, \
         patch("lemongrass._db.get_team",
               return_value=_db.TeamRow("wot-lemons", "WOT Lemons")):
        assert _run(["lemongrass-entries", "set", "r1", "252",
                     "--team", "wot-lemons"]) == 0
    write.assert_called_once_with("r1", "252", "wot-lemons")

    with patch("lemongrass._db.list_entries",
               return_value=[_db.EntryRow("r1", "252", "wot-lemons")]):
        assert _run(["lemongrass-entries", "list"]) == 0
    assert "252" in capsys.readouterr().out


def test_cli_set_defaults_to_the_configured_team(monkeypatch, tmp_path):
    from lemongrass import _db
    cfg = tmp_path / "c.toml"
    cfg.write_text('[team]\nid = "wot-lemons"\n', encoding="utf-8")
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    with patch("lemongrass._db.set_entry") as write, \
         patch("lemongrass._db.get_team",
               return_value=_db.TeamRow("wot-lemons", "WOT Lemons")):
        assert _run(["lemongrass-entries", "set", "r1", "252"]) == 0
    write.assert_called_once_with("r1", "252", "wot-lemons")


def test_cli_set_without_a_team_anywhere_fails(monkeypatch, capsys):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    assert _run(["lemongrass-entries", "set", "r1", "252"]) == 1
    assert "team" in capsys.readouterr().err


def test_cli_set_rejects_a_blank_car_number(db, capsys):
    # store_entry (laps.py) already screens whitespace-only car_number; the
    # `entries set` CLI path must mirror that guard instead of writing a row
    # with an empty-string primary-key component.
    from lemongrass import _db
    _race(db)
    _db.upsert_team("wot-lemons", "WOT Lemons")
    assert _run(["lemongrass-entries", "set", "r1", "  ",
                "--team", "wot-lemons"]) == 1
    assert "car number" in capsys.readouterr().err.lower()
    assert _db.list_entries() == []


def test_cli_set_rejects_an_unknown_team(db, capsys):
    # store_entry checks get_team first; the CLI path must too, rather than
    # letting the FK violation raise an unhandled IntegrityError.
    from lemongrass import _db
    _race(db)
    assert _run(["lemongrass-entries", "set", "r1", "252",
                "--team", "nosuch"]) == 1
    assert "nosuch" in capsys.readouterr().err
    assert _db.list_entries() == []


def test_set_entry_rejects_a_race_with_no_stored_row(db):
    # race_id is a foreign key, and `entries propose` sources race ids from
    # Influx lap data, which carries races captured before the cutover. A
    # ValueError is reportable; a raw IntegrityError is a traceback.
    from lemongrass import _db
    _db.upsert_team("wot-lemons", "WOT Lemons")
    with pytest.raises(ValueError, match="no race row"):
        _db.set_entry("never-imported", "252", "wot-lemons")


def test_cli_set_reports_a_race_with_no_stored_row(db, capsys):
    from lemongrass import _db
    _db.upsert_team("wot-lemons", "WOT Lemons")
    assert _run(["lemongrass-entries", "set", "never-imported", "252",
                "--team", "wot-lemons"]) == 1
    assert "no race row" in capsys.readouterr().err
    assert _db.list_entries() == []


def test_confirm_keeps_going_when_a_race_has_no_stored_row(capsys):
    # Aborting here would strand every proposal after this one, after the
    # operator has already answered for them.
    from lemongrass import entries
    proposals = [
        {"race_id": "101", "car_number": "252", "competitor_name": "WOT Lemons",
         "existing_team_id": None},
        {"race_id": "102", "car_number": "7", "competitor_name": "WOT Lemons",
         "existing_team_id": None},
    ]

    def write(race_id, car_number, team_id):
        if race_id == "101":
            raise ValueError("no race row for '101'")

    # y to both entries; only the second gets an alias offer, answered n.
    with patch("builtins.input", side_effect=["y", "y", "n"]), \
         patch("lemongrass._db.set_entry", side_effect=write):
        assert entries.confirm_proposals(proposals, "wot-lemons") == (1, 1)
    assert "no race row for '101'" in capsys.readouterr().err


def test_cli_propose_exits_nonzero_when_every_accepted_write_failed(capsys):
    # "recorded 0 entries" and exit 0 would read as "nothing to do" to a
    # script, when in fact the operator answered y and every write failed.
    from lemongrass import _db, entries
    proposals = [{"race_id": "101", "car_number": "252",
                  "competitor_name": "WOT Lemons", "existing_team_id": None}]
    with patch("lemongrass._db.get_team",
               return_value=_db.TeamRow("wot-lemons", "WOT Lemons")), \
         patch("lemongrass._influx.connect"), \
         patch.object(entries, "propose_entries", return_value=proposals), \
         patch("builtins.input", side_effect=["y"]), \
         patch("lemongrass._db.set_entry",
               side_effect=ValueError("no race row for '101'")):
        assert _run(["lemongrass-entries", "propose", "--team", "wot-lemons",
                    "--term", "wot"]) == 1
    assert "could not be recorded" in capsys.readouterr().err


def test_cli_list_can_be_filtered_by_race(db, capsys):
    from lemongrass import _db
    _race(db, "r1")
    _race(db, "r2")
    _db.upsert_team("us", "Us")
    _db.set_entry("r1", "252", "us")
    _db.set_entry("r2", "253", "us")
    assert _run(["lemongrass-entries", "list", "--race", "r2"]) == 0
    out = capsys.readouterr().out
    assert "253" in out and "252" not in out


def test_cli_propose_rejects_an_unknown_team(capsys):
    # Without this check, propose scans Influx, prints proposals, prompts the
    # operator, and then dies on an unhandled FK IntegrityError inside
    # confirm_proposals at the first accepted proposal. Validate before
    # opening the Influx connection at all.
    with patch("lemongrass._db.get_team", return_value=None), \
         patch("lemongrass._influx.connect") as connect:
        assert _run(["lemongrass-entries", "propose", "--team", "nosuch",
                    "--term", "wot"]) == 1
    assert "nosuch" in capsys.readouterr().err
    connect.assert_not_called()


def test_entries_is_a_registered_command():
    from lemongrass import cli
    assert cli._COMMANDS["entries"] == "lemongrass.entries"


class _Rec:
    def __init__(self, values, value):
        self.values = values
        self._value = value

    def get_value(self):
        return self._value


class _Table:
    def __init__(self, records):
        self.records = records


def _query_api(pairs):
    """A query_api returning one distinct competitor_name per (race, car)."""
    from unittest.mock import MagicMock
    api = MagicMock()
    api.query.return_value = [_Table([
        _Rec({"race_id": race_id, "car_number": car}, name)
        for race_id, car, name in pairs])]
    return api


def test_propose_matches_any_of_several_terms():
    from lemongrass import entries
    api = _query_api([
        ("101", "252", "WOT Lemons"),
        ("102", "253", "Wide Open Throttle"),
        ("103", "7", "Somebody Else"),
    ])
    with patch("lemongrass._db.list_entries", return_value=[]), \
         patch("lemongrass._db.list_team_aliases", return_value=[]):
        got = entries.propose_entries(api, ["wot lemons", "wide open"],
                                      "wot-lemons")
    assert [(p["race_id"], p["car_number"]) for p in got] == [
        ("101", "252"), ("102", "253")]


def test_propose_normalizes_both_sides():
    from lemongrass import entries
    api = _query_api([("101", " 252 ", "WOT  LEMONS!")])
    with patch("lemongrass._db.list_entries", return_value=[]), \
         patch("lemongrass._db.list_team_aliases", return_value=[]):
        got = entries.propose_entries(api, ["WOT   Lemons!"], "wot-lemons")
    # Case, punctuation, and runs of whitespace collapse on BOTH sides: the
    # term and the stored name each normalize to "wot lemons". Note this is
    # substring matching AFTER normalization, so "W.O.T. Lemons" (which
    # normalizes to "w o t lemons") is a genuinely different spelling and needs
    # its own term or alias — the loader's rules do not infer it.
    assert got[0]["competitor_name"] == "WOT  LEMONS!"
    # Car numbers are trimmed on match as well as on write.
    assert got[0]["car_number"] == "252"


def test_propose_flags_an_entry_that_already_exists():
    from lemongrass import _db, entries
    api = _query_api([("101", "252", "WOT Lemons")])
    with patch("lemongrass._db.list_entries",
               return_value=[_db.EntryRow("101", "252", "someone-else")]), \
         patch("lemongrass._db.list_team_aliases", return_value=[]):
        got = entries.propose_entries(api, ["wot lemons"], "wot-lemons")
    assert got[0]["existing_team_id"] == "someone-else"


def test_propose_skips_entries_already_pointing_at_this_team():
    from lemongrass import _db, entries
    api = _query_api([("101", "252", "WOT Lemons")])
    with patch("lemongrass._db.list_entries",
               return_value=[_db.EntryRow("101", "252", "wot-lemons")]), \
         patch("lemongrass._db.list_team_aliases", return_value=[]):
        assert entries.propose_entries(api, ["wot lemons"], "wot-lemons") == []


def test_propose_also_searches_the_teams_recorded_aliases():
    # Confirming a match records the spelling as an alias, so the next run
    # finds that season's races without the term having to be retyped.
    from lemongrass import entries
    api = _query_api([("101", "252", "Wide Open Throttle")])
    with patch("lemongrass._db.list_entries", return_value=[]), \
         patch("lemongrass._db.list_team_aliases",
               return_value=[("wot-lemons", "wide open throttle")]):
        got = entries.propose_entries(api, ["nothing matches this"],
                                      "wot-lemons")
    assert [p["race_id"] for p in got] == ["101"]


def test_confirm_writes_only_what_was_accepted(capsys):
    from lemongrass import entries
    proposals = [
        {"race_id": "101", "car_number": "252", "competitor_name": "WOT Lemons",
         "existing_team_id": None},
        {"race_id": "102", "car_number": "7", "competitor_name": "Not Us",
         "existing_team_id": None},
    ]
    # y to the first entry, n to its alias offer, n to the second entry.
    with patch("builtins.input", side_effect=["y", "n", "n"]), \
         patch("lemongrass._db.set_entry") as write, \
         patch("lemongrass._db.add_team_alias") as alias:
        assert entries.confirm_proposals(proposals, "wot-lemons") == (1, 0)
    write.assert_called_once_with("101", "252", "wot-lemons")
    assert not alias.called


def test_confirm_records_the_matched_spelling_as_an_alias():
    from lemongrass import entries
    proposals = [{"race_id": "101", "car_number": "252",
                  "competitor_name": "Wide Open Throttle",
                  "existing_team_id": None}]
    with patch("builtins.input", side_effect=["y", "y"]), \
         patch("lemongrass._db.set_entry"), \
         patch("lemongrass._db.add_team_alias") as alias:
        entries.confirm_proposals(proposals, "wot-lemons")
    alias.assert_called_once_with("wot-lemons", "Wide Open Throttle")


def test_confirm_reports_an_alias_conflict_without_aborting(capsys):
    # A different team already owns this spelling. add_team_alias raises
    # ValueError for that; confirm_proposals must report it and keep going
    # rather than losing every remaining proposal to an unhandled traceback.
    from lemongrass import entries
    proposals = [
        {"race_id": "101", "car_number": "252", "competitor_name": "WOT Lemons",
         "existing_team_id": None},
        {"race_id": "102", "car_number": "7", "competitor_name": "Not Us",
         "existing_team_id": None},
    ]
    with patch("builtins.input", side_effect=["y", "y", "y", "n"]), \
         patch("lemongrass._db.set_entry") as write, \
         patch("lemongrass._db.add_team_alias",
               side_effect=ValueError("already recorded for team someone-else")):
        written, failed = entries.confirm_proposals(proposals, "wot-lemons")
    # An alias conflict is not an entry failure: both entries were stored.
    assert failed == 0
    assert written == 2
    assert write.call_count == 2
    assert "someone-else" in capsys.readouterr().err


def test_propose_command_writes_nothing_when_every_answer_is_no(capsys):
    from lemongrass import _db
    proposals = [{"race_id": "101", "car_number": "252",
                  "competitor_name": "WOT Lemons", "existing_team_id": None}]
    with patch("lemongrass._db.get_team",
               return_value=_db.TeamRow("wot-lemons", "WOT Lemons")), \
         patch("lemongrass._influx.connect"), \
         patch("lemongrass.entries.propose_entries", return_value=proposals), \
         patch("builtins.input", return_value="n"), \
         patch("lemongrass._db.set_entry") as write, \
         patch("lemongrass._db.add_team_alias") as alias:
        assert _run(["lemongrass-entries", "propose", "--team", "wot-lemons",
                     "--term", "wot lemons"]) == 0
    assert not write.called
    assert not alias.called
    assert "recorded 0 entries" in capsys.readouterr().out


def test_propose_requires_at_least_one_term():
    # argparse exits rather than returning; --term is required=True.
    import pytest
    with pytest.raises(SystemExit) as excinfo:
        _run(["lemongrass-entries", "propose", "--team", "x"])
    assert excinfo.value.code == 2
