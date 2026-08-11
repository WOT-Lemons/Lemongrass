import sys
from unittest.mock import patch

import pytest
from sqlalchemy import text


def test_upsert_team_creates_and_renames(db):
    from lemongrass import _db
    _db.upsert_team("wot-lemons", "WOT Lemons")
    assert [(t.team_id, t.name) for t in _db.list_teams()] == [
        ("wot-lemons", "WOT Lemons")]
    _db.upsert_team("wot-lemons", "Wide Open Throttle Lemons")
    assert _db.get_team("wot-lemons").name == "Wide Open Throttle Lemons"
    assert _db.get_team("nope") is None


def test_aliases_are_stored_normalized(db):
    from lemongrass import _db
    _db.upsert_team("wot-lemons", "WOT Lemons")
    _db.add_team_alias("wot-lemons", "W.O.T. Lemons!")
    assert _db.list_team_aliases("wot-lemons") == [
        ("wot-lemons", "w o t lemons")]
    assert _db.list_team_aliases("nobody") == []


def test_one_alias_cannot_be_claimed_by_two_teams(db):
    from lemongrass import _db
    _db.upsert_team("a", "A")
    _db.upsert_team("b", "B")
    _db.add_team_alias("a", "shared name")
    with pytest.raises(ValueError, match="shared name"):
        _db.add_team_alias("b", "shared name")
    # The conflict must not have overwritten the existing mapping.
    assert _db.list_team_aliases("a") == [("a", "shared name")]
    assert _db.list_team_aliases("b") == []


def test_add_team_alias_repeating_the_same_team_is_a_no_op(db):
    # `entries propose` -> `confirm_proposals` offers to record an alias for
    # every matching proposal, even ones an earlier run already recorded —
    # re-answering "y" must not raise.
    from lemongrass import _db
    _db.upsert_team("wot-lemons", "WOT Lemons")
    _db.add_team_alias("wot-lemons", "WOT Lemons")
    _db.add_team_alias("wot-lemons", "WOT Lemons")
    assert _db.list_team_aliases("wot-lemons") == [("wot-lemons", "wot lemons")]


def test_add_team_alias_rejects_an_unknown_team(db):
    from lemongrass import _db
    with pytest.raises(ValueError, match="nosuch"):
        _db.add_team_alias("nosuch", "Some Name")
    assert _db.list_team_aliases() == []


def test_merge_moves_entries_and_aliases_and_drops_the_source(db):
    from datetime import UTC, datetime

    from lemongrass import _db
    _db.upsert_team("old-name", "Old Name")
    _db.upsert_team("wot-lemons", "WOT Lemons")
    _db.add_team_alias("old-name", "older still")
    _db.upsert_race(_db.RaceRow(race_id="r1",
                                race_time=datetime(2019, 5, 1, tzinfo=UTC)))
    with db.begin() as conn:
        conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                          "VALUES ('r1', '252', 'old-name')"))

    assert _db.merge_teams("old-name", "wot-lemons") == 1

    with db.begin() as conn:
        assert conn.execute(text(
            "SELECT team_id FROM entries WHERE race_id='r1'")).scalar() == (
            "wot-lemons")
        assert conn.execute(text(
            "SELECT count(*) FROM teams WHERE team_id='old-name'")).scalar() == 0
    aliases = {a: t for t, a in _db.list_team_aliases()}
    assert aliases["older still"] == "wot-lemons"
    # The source team's own name survives as an alias — it is the spelling
    # that made the merge necessary in the first place.
    assert aliases["old name"] == "wot-lemons"


def test_merge_is_atomic(db):
    # "in one transaction" is the whole point: a merge that reassigned entries
    # and then failed would leave history split across a team that no longer
    # exists in anyone's mental model but still exists in the table.
    from datetime import UTC, datetime

    from lemongrass import _db
    _db.upsert_team("old-name", "Old Name")
    _db.upsert_team("wot-lemons", "WOT Lemons")
    _db.upsert_race(_db.RaceRow(race_id="r1",
                                race_time=datetime(2019, 5, 1, tzinfo=UTC)))
    with db.begin() as conn:
        conn.execute(text("INSERT INTO entries (race_id, car_number, team_id) "
                          "VALUES ('r1', '252', 'old-name')"))

    # Fail partway through: merge_teams normalizes the source team's name for
    # the alias insert AFTER it has already reassigned the entries and
    # re-pointed the existing aliases, so raising there leaves a half-done
    # merge unless the whole thing is one transaction.
    with patch("lemongrass._tracks.normalize",
               side_effect=RuntimeError("boom")), \
         pytest.raises(RuntimeError):
        _db.merge_teams("old-name", "wot-lemons")

    with db.begin() as conn:
        # Nothing moved, and the source team is still there.
        assert conn.execute(text(
            "SELECT team_id FROM entries WHERE race_id='r1'")).scalar() == (
            "old-name")
        assert conn.execute(text(
            "SELECT count(*) FROM teams WHERE team_id='old-name'")).scalar() == 1


def test_merge_rejects_an_unknown_target(db):
    from lemongrass import _db
    _db.upsert_team("a", "A")
    with pytest.raises(ValueError, match="wot-lemons"):
        _db.merge_teams("a", "wot-lemons")


def test_merge_rejects_an_unknown_source(db):
    from lemongrass import _db
    _db.upsert_team("wot-lemons", "WOT Lemons")
    with pytest.raises(ValueError, match="ghost"):
        _db.merge_teams("ghost", "wot-lemons")


def test_merge_rejects_a_team_merged_into_itself(db):
    # Otherwise the final DELETE removes the team entries were just re-pointed
    # at — silently, if it has no entries yet, or with a raw FK IntegrityError
    # if it does. Neither reads as "merged into itself"; both are refused.
    from lemongrass import _db
    _db.upsert_team("a", "A")
    with pytest.raises(ValueError, match="itself"):
        _db.merge_teams("a", "a")
    assert _db.get_team("a") is not None


def _run(argv):
    from lemongrass import teams
    with patch.object(sys, "argv", argv):
        return teams.main()


def test_cli_add_and_list(capsys):
    with patch("lemongrass._db.upsert_team") as add:
        assert _run(["lemongrass-teams", "add", "wot-lemons", "WOT Lemons"]) == 0
    add.assert_called_once_with("wot-lemons", "WOT Lemons")

    from lemongrass import _db
    with patch("lemongrass._db.list_teams",
               return_value=[_db.TeamRow("wot-lemons", "WOT Lemons")]), \
         patch("lemongrass._db.list_team_aliases",
               return_value=[("wot-lemons", "old name")]):
        assert _run(["lemongrass-teams", "list"]) == 0
    out = capsys.readouterr().out
    assert "wot-lemons" in out and "WOT Lemons" in out and "old name" in out


def test_cli_alias_and_merge(capsys):
    with patch("lemongrass._db.add_team_alias") as alias:
        assert _run(["lemongrass-teams", "alias", "wot-lemons", "Old Name"]) == 0
    alias.assert_called_once_with("wot-lemons", "Old Name")

    with patch("lemongrass._db.merge_teams", return_value=3) as merge:
        assert _run(["lemongrass-teams", "merge", "old-name", "wot-lemons"]) == 0
    merge.assert_called_once_with("old-name", "wot-lemons")
    assert "3 entr" in capsys.readouterr().out


def test_cli_alias_reports_a_bad_team(capsys):
    with patch("lemongrass._db.add_team_alias",
               side_effect=ValueError("no team nosuch")):
        assert _run(["lemongrass-teams", "alias", "nosuch", "Old Name"]) == 1
    assert "no team nosuch" in capsys.readouterr().err


def test_cli_merge_reports_a_bad_target(capsys):
    with patch("lemongrass._db.merge_teams", side_effect=ValueError("no team x")):
        assert _run(["lemongrass-teams", "merge", "a", "x"]) == 1
    assert "no team x" in capsys.readouterr().err


def test_teams_is_a_registered_command():
    from lemongrass import cli
    assert cli._COMMANDS["teams"] == "lemongrass.teams"
