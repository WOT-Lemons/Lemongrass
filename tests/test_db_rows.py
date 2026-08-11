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
    # Back-date the insert's server_default stamp: without this, a conflict
    # update that never touches updated_at leaves second == first, which a
    # `>=` assertion would still pass.
    with db.begin() as conn:
        conn.execute(text("UPDATE races SET updated_at = now() - interval '1 day'"))
        first = conn.execute(text("SELECT updated_at FROM races")).scalar()
    _db.upsert_race(_race(name="Renamed"))
    with db.begin() as conn:
        second = conn.execute(text("SELECT updated_at FROM races")).scalar()
    assert second > first


def test_upsert_race_accepts_empty_names(db):
    # _resolve_race_metadata returns race_name='' / track_name='' when the
    # details fetch fails; those races must still store.
    from lemongrass import _db
    _db.upsert_race(_race(name="", track_name=""))
    got = _db.get_race("101")
    assert got.name == ""
    assert got.track_name == ""


def test_upsert_race_preserves_identity_on_a_blank_write(db):
    # Finding 3: a failed race.details fetch produces name='', track_name='',
    # series_id=None, series_name=None, end_time=None (see
    # _resolve_race_metadata's RaceMetadata('', '', None, 0)). A blanket
    # EXCLUDED conflict-update would wipe a previously good row's identity
    # down to a bare id. Postgres is the system of record for these columns
    # now, so the blank write must not overwrite the stored values.
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_race(_race(name="", track_name="", series_id=None,
                          series_name=None, end_time=None))
    got = _db.get_race("101")
    assert got.name == "Spring Thing"
    assert got.track_name == "Thompson Speedway"
    assert got.series_id == 7
    assert got.series_name == "Lemons"
    assert got.end_time == datetime(2026, 5, 1, 20, 0, tzinfo=UTC)


def test_upsert_race_updates_identity_on_a_non_blank_write(db):
    # The other direction: a race whose name/track/series/end_time legitimately
    # change (a later, successful details fetch) must still update — only a
    # blank/None value is protected, not every second write.
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_race(_race(name="Spring Thing II", track_name="Lime Rock",
                          series_id=9, series_name="ChampCar",
                          end_time=datetime(2026, 5, 2, 1, 0, tzinfo=UTC)))
    got = _db.get_race("101")
    assert got.name == "Spring Thing II"
    assert got.track_name == "Lime Rock"
    assert got.series_id == 9
    assert got.series_name == "ChampCar"
    assert got.end_time == datetime(2026, 5, 2, 1, 0, tzinfo=UTC)


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


def _session(session_id=10, race_id="101", **kw):
    from lemongrass import _db
    base = {
        "name": "Session 1",
        "start_time": datetime(2026, 5, 1, 13, 0, tzinfo=UTC),
    }
    base.update(kw)
    return _db.SessionRow(session_id=session_id, race_id=race_id, **base)


def test_upsert_session_inserts_and_updates(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_session(_session())
    _db.upsert_session(_session(name="Qualifying"))
    got = _db.list_sessions("101")
    assert len(got) == 1
    assert got[0].name == "Qualifying"


def test_upsert_session_allows_null_start_time(db):
    # The live path calls store_session(..., None): storing 0 rendered as 1970
    # in the picker, NULL is the honest representation.
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_session(_session(start_time=None))
    assert _db.list_sessions("101")[0].start_time is None


def test_replace_sessions_removes_ones_that_disappeared(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.replace_sessions("101", [_session(10), _session(11)])
    _db.replace_sessions("101", [_session(10)])
    assert [s.session_id for s in _db.list_sessions("101")] == [10]


def test_replace_sessions_leaves_other_races_alone(db):
    from lemongrass import _db
    _db.upsert_race(_race("101"))
    _db.upsert_race(_race("202"))
    _db.upsert_session(_session(99, race_id="202"))
    _db.replace_sessions("101", [_session(10)])
    assert [s.session_id for s in _db.list_sessions("202")] == [99]


def test_replace_sessions_refuses_to_steal_another_races_session(db):
    # session_id is the primary key on the assumption that RaceMonitor mints
    # ids globally, but nothing enforces that and the legacy import writes
    # whatever Influx held. Reassigning the row would take the session off the
    # other race's picker with no error and no way to notice.
    from lemongrass import _db
    _db.upsert_race(_race("101"))
    _db.upsert_race(_race("202"))
    _db.upsert_session(_session(99, race_id="202"))
    with pytest.raises(ValueError, match="99"):
        _db.replace_sessions("101", [_session(99)])
    assert [s.session_id for s in _db.list_sessions("202")] == [99]


def test_replace_sessions_with_an_empty_set_clears_the_race(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_session(_session(10))
    _db.replace_sessions("101", [])
    assert _db.list_sessions("101") == []


def test_replace_sessions_is_one_transaction(db):
    # A failing row must not leave the delete half-applied: the old set has to
    # survive intact so the next backfill can redo the whole rewrite.
    from sqlalchemy.exc import IntegrityError

    from lemongrass import _db
    _db.upsert_race(_race())
    _db.replace_sessions("101", [_session(10)])
    with pytest.raises(IntegrityError):
        _db.replace_sessions("101", [_session(11), _session(12, race_id="ghost")])
    assert [s.session_id for s in _db.list_sessions("101")] == [10]


def test_delete_race_cascades_to_sessions(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_session(_session())
    _db.delete_race("101")
    assert _db.list_sessions("101") == []


def test_list_sessions_orders_nulls_last(db):
    from lemongrass import _db
    _db.upsert_race(_race())
    _db.upsert_session(_session(12, start_time=None))
    _db.upsert_session(_session(
        11, start_time=datetime(2026, 5, 1, 15, 0, tzinfo=UTC)))
    _db.upsert_session(_session(
        10, start_time=datetime(2026, 5, 1, 13, 0, tzinfo=UTC)))
    assert [s.session_id for s in _db.list_sessions("101")] == [10, 11, 12]


def test_list_sessions_without_a_race_id_returns_all(db):
    from lemongrass import _db
    _db.upsert_race(_race("101"))
    _db.upsert_race(_race("202"))
    _db.upsert_session(_session(10, race_id="101"))
    _db.upsert_session(_session(20, race_id="202"))
    assert len(_db.list_sessions()) == 2
