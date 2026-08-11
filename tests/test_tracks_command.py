import sys
from unittest.mock import patch

from lemongrass import tracks


def _summary(**overrides):
    """A sync_tracks summary with every key present, as the CLI expects."""
    summary = {"venues_created": 0, "venues_updated": 0, "layouts_created": 0,
               "layouts_updated": 0, "events_created": 0, "events_updated": 0,
               "orphan_venues": [], "orphan_layouts": [], "orphan_events": []}
    summary.update(overrides)
    return summary


def _run(argv):
    with patch.object(sys, "argv", argv):
        return tracks.main()


def test_sync_reports_what_it_wrote(capsys):
    summary = _summary(venues_created=9, layouts_created=2, events_created=1)
    with patch("lemongrass._db.sync_tracks", return_value=summary) as sync:
        assert _run(["lemongrass-tracks", "sync"]) == 0
    assert sync.call_args.kwargs["dry_run"] is False
    out = capsys.readouterr().out
    # Counts are right-aligned in 3 columns; the labels are padded to a fixed
    # width so the three lines form a column.
    assert "venues  : created   9  updated   0" in out
    assert "layouts : created   2  updated   0" in out
    assert "events  : created   1  updated   0" in out


def test_sync_dry_run_passes_the_flag_and_says_so(capsys):
    summary = _summary(venues_created=1)
    with patch("lemongrass._db.sync_tracks", return_value=summary) as sync:
        assert _run(["lemongrass-tracks", "sync", "--dry-run"]) == 0
    assert sync.call_args.kwargs["dry_run"] is True
    assert "dry run: nothing written" in capsys.readouterr().out


def test_sync_reports_orphans(capsys):
    summary = _summary(orphan_venues=["gone"],
                       orphan_layouts=[("njmp", "old")],
                       orphan_events=["retired"])
    with patch("lemongrass._db.sync_tracks", return_value=summary):
        assert _run(["lemongrass-tracks", "sync"]) == 0
    out = capsys.readouterr().out
    assert "stored but absent from tracks.toml" in out
    assert "venue gone" in out
    assert "layout njmp/old" in out
    assert "event retired" in out


def test_unknown_subcommand_is_rejected(capsys):
    assert _run(["lemongrass-tracks", "nope"]) == 1
    assert "Usage: lemongrass tracks" in capsys.readouterr().out


def test_db_upgrade_syncs_track_data_as_its_final_step():
    # Schema and curated data must arrive together: an upgrade that added a
    # venue to the file but no row would make store_race fail mid-race.
    from lemongrass import db
    calls = []

    def _record_sync(*a, **k):
        # Must return a real summary: _handle_upgrade feeds it straight to
        # print_sync_summary, and list.append (or a bare MagicMock) would blow
        # up on the subscript and the :3d format respectively.
        calls.append("sync")
        return _summary()

    with patch("alembic.command.upgrade",
               side_effect=lambda *a, **k: calls.append("upgrade")), \
         patch("lemongrass._db.alembic_config"), \
         patch("lemongrass._db.sync_tracks", side_effect=_record_sync) as sync:
        with patch.object(sys, "argv", ["lemongrass-db", "upgrade"]):
            assert db.main() == 0
    assert calls == ["upgrade", "sync"]
    assert sync.called


def test_tracks_is_a_registered_command():
    from lemongrass import cli
    assert cli._COMMANDS["tracks"] == "lemongrass.tracks"
