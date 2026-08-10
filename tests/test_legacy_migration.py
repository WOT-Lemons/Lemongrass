from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from lemongrass import _legacy_migration as _mod


class _Rec:
    def __init__(self, values, time=None):
        self.values = values
        self._time = time

    def get_time(self):
        return self._time


def _tables(records):
    table = MagicMock()
    table.records = records
    return [table]


def test_reads_a_race_point_with_every_field():
    rec = _Rec({'race_id': '101', 'race_name': 'Spring', 'track_name': 'Thompson',
                'series_name': 'Lemons', 'end_time_epoc': 1_700_003_600,
                'expected_lap_count': 120, 'session_count': 2,
                'schema_version': 4},
               datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (row,) = _mod.read_legacy_races(query_api)
    assert row.race_id == '101'
    assert row.race_time == datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    assert row.end_time == datetime.fromtimestamp(1_700_003_600, tz=UTC)
    assert row.expected_lap_count == 120
    assert row.lap_schema_version == 4
    assert row.series_id is None   # never stored in Influx; unrecoverable


def test_absent_tags_become_empty_strings():
    # Influx drops empty tag values entirely, so a race written from an
    # unsuccessful details fetch comes back with the columns absent.
    rec = _Rec({'race_id': '101', 'end_time_epoc': 0},
               datetime(2026, 5, 1, tzinfo=UTC))
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (row,) = _mod.read_legacy_races(query_api)
    assert row.name == ''
    assert row.track_name == ''
    assert row.series_name is None
    assert row.end_time is None    # 0 is "unknown", not 1970


def test_missing_completeness_fields_stay_null():
    rec = _Rec({'race_id': '101'}, datetime(2026, 5, 1, tzinfo=UTC))
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (row,) = _mod.read_legacy_races(query_api)
    assert row.expected_lap_count is None
    assert row.session_count is None
    assert row.lap_schema_version is None


def test_race_query_has_an_explicit_far_future_stop():
    # range(start: 0) carries an implicit stop: now(), and a scheduled race is
    # timestamped in the future (race_ts_ms = start_epoc * 1000), so an
    # implicit stop would make it invisible to the import.
    query_api = MagicMock(query=MagicMock(return_value=_tables([])))
    _mod.read_legacy_races(query_api)
    assert _mod.FAR_FUTURE in query_api.query.call_args.args[0]


def test_session_query_has_an_explicit_far_future_stop():
    query_api = MagicMock(query=MagicMock(return_value=_tables([])))
    _mod.read_legacy_sessions(query_api)
    assert _mod.FAR_FUTURE in query_api.query.call_args.args[0]


def test_reads_a_session_point():
    rec = _Rec({'race_id': '101', 'session_id': '55',
                'session_name': 'Qualifying', 'start_epoc': 1_700_000_000})
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (row,) = _mod.read_legacy_sessions(query_api)
    assert row.session_id == 55        # BIGINT, not the stringified tag
    assert row.race_id == '101'
    assert row.name == 'Qualifying'
    assert row.start_time == datetime.fromtimestamp(1_700_000_000, tz=UTC)


def test_zero_start_epoch_becomes_null():
    rec = _Rec({'race_id': '101', 'session_id': '55', 'start_epoc': 0})
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (row,) = _mod.read_legacy_sessions(query_api)
    assert row.start_time is None


def test_import_writes_races_then_sessions(db):
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', name='Spring',
                        race_time=datetime(2026, 5, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101', name='Q')]):
        summary = _mod.import_legacy(query_api)
    assert summary['races_written'] == 1
    assert summary['sessions_written'] == 1
    assert _db.get_race('101').name == 'Spring'


def test_orphan_sessions_are_skipped_and_reported(db, caplog):
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='64202')]):
        summary = _mod.import_legacy(query_api)
    assert summary['sessions_written'] == 0
    assert summary['sessions_skipped'] == 1
    assert summary['orphan_race_ids'] == ['64202']
    assert _db.list_sessions() == []


def test_dry_run_writes_nothing(db):
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101',
                        race_time=datetime(2026, 5, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[]):
        summary = _mod.import_legacy(query_api, dry_run=True)
    assert summary['races_read'] == 1
    assert summary['races_written'] == 0
    assert _db.list_races() == []


def test_only_missing_does_not_clobber_a_newer_row(db):
    # The post-deploy catch-up run replays stale Influx values over rows the
    # new writer has since corrected unless it is scoped to inserts.
    from lemongrass import _db
    _db.upsert_race(_db.RaceRow(race_id='101', name='Corrected',
                                race_time=datetime(2026, 5, 1, tzinfo=UTC)))
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', name='Stale',
                        race_time=datetime(2026, 5, 1, tzinfo=UTC)),
            _db.RaceRow(race_id='102', name='New',
                        race_time=datetime(2026, 6, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[]):
        summary = _mod.import_legacy(query_api, only_missing=True)
    assert summary['races_written'] == 1
    assert _db.get_race('101').name == 'Corrected'
    assert _db.get_race('102').name == 'New'


def test_dry_run_reports_would_write_counts(db):
    # written is hard-zeroed under dry_run, but the runbook judges a healthy
    # rehearsal by "races read ≈ races written" — would_write is what makes
    # that check satisfiable without actually writing anything.
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC)),
            _db.RaceRow(race_id='102', race_time=datetime(2026, 6, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101')]):
        summary = _mod.import_legacy(query_api, dry_run=True)
    assert summary['races_written'] == 0
    assert summary['races_would_write'] == 2
    assert summary['sessions_written'] == 0
    assert summary['sessions_would_write'] == 1
    assert _db.list_races() == []
    assert _db.list_sessions() == []


def test_real_run_would_write_matches_written(db):
    # On a real, non-only_missing run every read row is written, so the
    # preview counter and the actual counter should agree.
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101')]):
        summary = _mod.import_legacy(query_api)
    assert summary['races_written'] == summary['races_would_write'] == 1
    assert summary['sessions_written'] == summary['sessions_would_write'] == 1


def test_only_missing_counts_reconcile(db):
    # A session or race skipped because it already exists must be accounted
    # for separately from an orphan skip, so read == written + the skip
    # buckets in every mode, including the post-deploy catch-up run.
    from lemongrass import _db
    _db.upsert_race(_db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC)))
    _db.upsert_session(_db.SessionRow(session_id=55, race_id='101'))
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC)),
            _db.RaceRow(race_id='102', race_time=datetime(2026, 6, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101'),
            _db.SessionRow(session_id=56, race_id='101'),
            _db.SessionRow(session_id=57, race_id='64202')]):
        summary = _mod.import_legacy(query_api, only_missing=True)
    assert summary['races_written'] == 1
    assert summary['races_skipped_existing'] == 1
    assert summary['races_read'] == summary['races_written'] + summary['races_skipped_existing']
    assert summary['sessions_written'] == 1
    assert summary['sessions_skipped_existing'] == 1
    assert summary['sessions_skipped'] == 1
    assert (summary['sessions_read']
            == summary['sessions_written'] + summary['sessions_skipped_existing']
            + summary['sessions_skipped'])
