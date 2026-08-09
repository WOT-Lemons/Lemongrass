from datetime import UTC, datetime

import pytest
from sqlalchemy import text


def _race(race_id="101", **kw):
    from lemongrass import _db
    base = {
        "race_time": datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        "name": "Spring Thing",
        "track_name": "Thompson Speedway",
        "series_id": 7,
        "series_name": "Lemons",
        "end_time": datetime(2026, 5, 1, 20, 0, tzinfo=UTC),
        "expected_lap_count": 120,
        "session_count": 2,
        "lap_schema_version": 4,
    }
    base.update(kw)
    return _db.RaceRow(race_id=race_id, **base)


def test_upsert_race_inserts(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    got = _db.get_race("101")
    assert got.name == "Spring Thing"
    assert got.series_id == 7
    assert got.race_time == datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    assert got.expected_lap_count == 120


def test_upsert_race_updates_in_place(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_race(_race(name="Renamed"))
    assert _db.get_race("101").name == "Renamed"
    with db.begin() as conn:
        assert conn.execute(text("SELECT count(*) FROM races")).scalar() == 1


def test_upsert_race_preserves_completeness_on_a_live_write(db):
    # The live monitor path writes no expected_lap_count / session_count /
    # lap_schema_version. A naive DO UPDATE would null out a backfilled
    # race's completeness fields on the next live poll, and the backfill
    # would then redo the whole race under the 6 req/min limit.
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_race(_race(expected_lap_count=None, session_count=None,
                          lap_schema_version=None))
    got = _db.get_race("101")
    assert got.expected_lap_count == 120
    assert got.session_count == 2
    assert got.lap_schema_version == 4


def test_upsert_race_bumps_updated_at(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    with db.begin() as conn:
        first = conn.execute(text("SELECT updated_at FROM races")).scalar()
    _db.upsert_race(_race(name="Renamed"))
    with db.begin() as conn:
        second = conn.execute(text("SELECT updated_at FROM races")).scalar()
    assert second >= first


def test_upsert_race_accepts_empty_names(db):
    # _resolve_race_metadata returns race_name='' / track_name='' when the
    # details fetch fails; those races must still store.
    from lemongrass import _db
    _db.upsert_race(_race(name="", track_name=""))
    got = _db.get_race("101")
    assert got.name == ""
    assert got.track_name == ""


def test_get_race_returns_none_when_absent(db):
    from lemongrass import _db
    assert _db.get_race("nope") is None


def test_list_races_is_newest_first(db):
    from lemongrass import _db
    _db.upsert_race(_race("1", race_time=datetime(2026, 1, 1, tzinfo=UTC)))
    _db.upsert_race(_race("2", race_time=datetime(2026, 6, 1, tzinfo=UTC)))
    assert [r.race_id for r in _db.list_races()] == ["2", "1"]


def test_delete_race_reports_whether_it_deleted(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    assert _db.delete_race("101") is True
    assert _db.delete_race("101") is False
    assert _db.get_race("101") is None


def test_statements_join_a_caller_transaction(db):
    # Passing conn lets several statements share one transaction — and one
    # rollback. replace_sessions depends on this.
    from lemongrass import _db
    with pytest.raises(RuntimeError):
        with _db.connect() as conn:
            _db.upsert_race(_race(), conn=conn)
            raise RuntimeError("boom")
    assert _db.get_race("101") is None
