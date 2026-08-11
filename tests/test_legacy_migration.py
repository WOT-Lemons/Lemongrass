import re
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

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


def test_series_id_survives_the_export_and_import_round_trip():
    # The export exists to make the cutover reversible. Every race written
    # after the cutover carries a real series_id, and dropping it on the way
    # out left those races permanently NULL on the way back in -- invisible,
    # because upsert_race COALESCEs the column -- with no way to recover it
    # short of re-fetching every race under the 6 req/min limit.
    from lemongrass import _db
    row = _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC),
                      name='Spring', series_id=145, series_name='Lemons')
    line = _mod.race_line(row)
    assert 'series_id=145i' in line

    rec = _Rec({'race_id': '101', 'series_id': 145},
               datetime(2026, 5, 1, tzinfo=UTC))
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (back,) = _mod.read_legacy_races(query_api)
    assert back.series_id == 145


def test_a_genuine_legacy_race_still_has_no_series_id():
    # Pre-cutover points never carried the field; absent must stay NULL rather
    # than becoming 0.
    rec = _Rec({'race_id': '101'}, datetime(2026, 5, 1, tzinfo=UTC))
    query_api = MagicMock(query=MagicMock(return_value=_tables([rec])))
    (row,) = _mod.read_legacy_races(query_api)
    assert row.series_id is None


def test_duplicate_race_points_collapse_to_one_row(caplog):
    # push_influx_race deleted with stop=now() before rewriting, so a race
    # point timestamped in the future -- a scheduled race whose start moved --
    # survived the delete and left two points for one race_id. Importing both
    # made the stored race_time depend on Flux table order, and the operator
    # got no hint that a race had been imported twice.
    stale = _Rec({'race_id': '101', 'race_name': 'Spring'},
                 datetime(2027, 1, 1, tzinfo=UTC))
    current = _Rec({'race_id': '101', 'race_name': 'Spring'},
                   datetime(2026, 5, 1, tzinfo=UTC))
    query_api = MagicMock(query=MagicMock(return_value=_tables([stale, current])))
    (row,) = _mod.read_legacy_races(query_api)
    assert row.race_time == datetime(2027, 1, 1, tzinfo=UTC)
    assert '101' in caplog.text


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


def test_a_session_id_claimed_by_two_races_is_skipped_and_reported(db, caplog):
    # sessions is keyed on session_id alone, so the second race's row would
    # overwrite the first's race_id instead of inserting -- the first race
    # would silently lose the session from its picker and the import summary
    # would still report both as written.
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC)),
            _db.RaceRow(race_id='202', race_time=datetime(2026, 6, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101', name='Q'),
            _db.SessionRow(session_id=55, race_id='202', name='Q')]):
        summary = _mod.import_legacy(query_api)
    assert summary['sessions_written'] == 1
    assert [s.session_id for s in _db.list_sessions('101')] == [55]
    assert _db.list_sessions('202') == []
    assert '55' in caplog.text


def test_reimport_does_not_overwrite_a_corrected_race_time(db):
    # A legacy race point's timestamp may itself be the live monitor's
    # wall-clock guess, so the import inserts it but never writes it over a
    # stored value the backfill has since corrected from StartDateEpoc.
    # Grafana derives race_from from race_time, so regressing it hides every
    # lap before the guess.
    from lemongrass import _db
    corrected = datetime(2026, 5, 1, 14, 30, tzinfo=UTC)
    _db.upsert_race(_db.RaceRow(race_id='101', race_time=corrected))
    query_api = MagicMock()
    # What read_legacy_races returns for a legacy point, flag included.
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101',
                        race_time=datetime(2026, 5, 1, 9, 0, tzinfo=UTC),
                        race_time_estimated=True)]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[]):
        _mod.import_legacy(query_api)
    assert _db.get_race('101').race_time == corrected


def test_read_legacy_races_flags_race_time_as_estimated(db):
    from lemongrass import _db
    query_api = MagicMock()
    record = MagicMock()
    record.values = {'race_id': '101'}
    record.get_time.return_value = datetime(2026, 5, 1, tzinfo=UTC)
    table = MagicMock()
    table.records = [record]
    query_api.query.return_value = [table]
    rows = _mod.read_legacy_races(query_api)
    assert [r.race_time_estimated for r in rows] == [True]
    # The row still inserts its race_time when nothing is stored yet.
    _db.upsert_race(rows[0])
    assert _db.get_race('101').race_time == datetime(2026, 5, 1, tzinfo=UTC)


def test_a_claimed_session_clears_the_losing_races_session_count(db):
    # The loser keeps a session_count that counts a session it will never own,
    # and no rewrite can ever satisfy it: replace_sessions raises on the
    # claimed id. Left set, _influx_only_skip sees the shortfall, refuses the
    # cheap skip, and every backfill re-fetches the race and then fails. NULL
    # is the honest answer -- this import does not know how many sessions the
    # race has -- and _influx_only_skip leaves a NULL count alone.
    from lemongrass import _db
    query_api = MagicMock()
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC),
                        session_count=1),
            _db.RaceRow(race_id='202', race_time=datetime(2026, 6, 1, tzinfo=UTC),
                        session_count=1)]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101', name='Q'),
            _db.SessionRow(session_id=55, race_id='202', name='Q')]):
        _mod.import_legacy(query_api)
    # 101 won the id and keeps its verified count; 202 lost it.
    assert _db.get_race('101').session_count == 1
    assert _db.get_race('202').session_count is None


def test_a_claimed_session_leaves_the_count_alone_under_dry_run(db):
    from lemongrass import _db
    query_api = MagicMock()
    _db.upsert_race(_db.RaceRow(race_id='202',
                                race_time=datetime(2026, 6, 1, tzinfo=UTC),
                                session_count=1))
    with patch.object(_mod, 'read_legacy_races', return_value=[
            _db.RaceRow(race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC)),
            _db.RaceRow(race_id='202', race_time=datetime(2026, 6, 1, tzinfo=UTC))]), \
         patch.object(_mod, 'read_legacy_sessions', return_value=[
            _db.SessionRow(session_id=55, race_id='101', name='Q'),
            _db.SessionRow(session_id=55, race_id='202', name='Q')]):
        _mod.import_legacy(query_api, dry_run=True)
    assert _db.get_race('202').session_count == 1


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


def test_race_line_matches_the_original_point_shape():
    from lemongrass import _db
    line = _mod.race_line(_db.RaceRow(
        race_id='101', name='Spring', track_name='Thompson',
        series_name='Lemons',
        race_time=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 1, 20, 0, tzinfo=UTC),
        expected_lap_count=120, session_count=2, lap_schema_version=4))
    assert line.startswith(
        'race,race_id=101,race_name=Spring,track_name=Thompson,'
        'series_name=Lemons ')
    assert 'end_time_epoc=1777665600i' in line
    assert 'expected_lap_count=120i' in line
    assert 'session_count=2i' in line
    assert 'schema_version=4i' in line
    assert line.endswith(' 1777636800000000000')


def test_race_line_escapes_and_omits_empty_tags():
    # Influx drops empty tag values, so emitting `race_name=` would not round
    # trip; spaces and commas inside a track name must be escaped.
    from lemongrass import _db
    line = _mod.race_line(_db.RaceRow(
        race_id='101', name='', track_name='Thompson Speedway, CT',
        race_time=datetime(2026, 5, 1, tzinfo=UTC)))
    assert 'race_name=' not in line
    assert 'track_name=Thompson\\ Speedway\\,\\ CT' in line
    assert 'series_name=' not in line


def test_race_line_omits_absent_fields_but_always_has_end_time_epoc():
    # end_time_epoc is the field every legacy reader filtered on; a point
    # without it is invisible to the old code.
    from lemongrass import _db
    line = _mod.race_line(_db.RaceRow(
        race_id='101', race_time=datetime(2026, 5, 1, tzinfo=UTC)))
    assert 'end_time_epoc=0i' in line
    assert 'expected_lap_count' not in line
    assert 'schema_version' not in line


def test_session_line_matches_the_original_point_shape():
    from lemongrass import _db
    line = _mod.session_line(_db.SessionRow(
        session_id=55, race_id='101', name='Qualifying',
        start_time=datetime(2026, 5, 1, 12, 0, tzinfo=UTC)))
    assert line.startswith('session,race_id=101,session_id=55 ')
    assert 'session_name="Qualifying"' in line
    assert 'start_epoc=1777636800i' in line
    assert line.endswith(' 1777636800000000000')


@pytest.mark.parametrize('break_char', ['\n', '\r'])
@pytest.mark.parametrize('field', ['name', 'track_name', 'series_name'])
def test_race_line_rejects_line_breaks_in_tags(field, break_char):
    # Line protocol delimits records with a newline and offers no escape, so a
    # tag carrying one would split the point into two corrupt records.
    from lemongrass import _db
    row = _db.RaceRow(race_id='101', name='Spring', track_name='Thompson',
                      series_name='Lemons',
                      race_time=datetime(2026, 5, 1, tzinfo=UTC))
    setattr(row, field, f'bad{break_char}value')
    with pytest.raises(ValueError, match='line break'):
        _mod.race_line(row)


@pytest.mark.parametrize('break_char', ['\n', '\r'])
def test_session_line_rejects_line_breaks_in_the_race_id_tag(break_char):
    from lemongrass import _db
    with pytest.raises(ValueError, match='line break'):
        _mod.session_line(_db.SessionRow(
            session_id=55, race_id=f'1{break_char}01', name='Qualifying'))


@pytest.mark.parametrize('break_char', ['\n', '\r'])
def test_session_line_rejects_line_breaks_in_the_name(break_char):
    from lemongrass import _db
    with pytest.raises(ValueError, match='line break'):
        _mod.session_line(_db.SessionRow(
            session_id=55, race_id='101', name=f'Qual{break_char}ifying'))


def test_session_line_null_start_time_is_zero_at_the_epoch():
    from lemongrass import _db
    line = _mod.session_line(_db.SessionRow(session_id=55, race_id='101'))
    assert 'start_epoc=0i' in line
    assert line.endswith(' 0')


def test_export_writes_races_before_sessions(db):
    import io

    from lemongrass import _db
    _db.upsert_race(_db.RaceRow(race_id='101', name='Spring',
                                race_time=datetime(2026, 5, 1, tzinfo=UTC)))
    _db.upsert_session(_db.SessionRow(session_id=55, race_id='101', name='Q'))
    out = io.StringIO()
    counts = _mod.export_legacy(out)
    lines = out.getvalue().splitlines()
    assert counts == {'races': 1, 'sessions': 1}
    assert lines[0].startswith('race,')
    assert lines[1].startswith('session,')


def test_export_round_trips_through_import(db):
    # The reverse of import_legacy: what comes out, put back in, is the same
    # set of rows. This is the rollback guarantee.
    import io

    from lemongrass import _db
    original = _db.RaceRow(
        race_id='101', name='Spring', track_name='Thompson Speedway',
        series_name='Lemons',
        race_time=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 1, 20, 0, tzinfo=UTC),
        expected_lap_count=120, session_count=2, lap_schema_version=4)
    _db.upsert_race(original)
    out = io.StringIO()
    _mod.export_legacy(out)
    records = _parse_line_protocol_as_records(out.getvalue())   # step 2
    query_api = MagicMock(query=MagicMock(return_value=_tables(records)))
    (back,) = _mod.read_legacy_races(query_api)
    # race_time_estimated is a write-policy flag, not a stored column: it says
    # how upsert_race should treat this row's race_time, and read_legacy_races
    # sets it on everything it reads. Every persisted field round trips.
    assert back == replace(original, race_time_estimated=True)


def _parse_line_protocol_as_records(text):
    """Turn exported line protocol back into pivoted-record stand-ins.

    Mirrors what a Flux pivot hands read_legacy_races: tags and fields in one
    values dict, with the point timestamp available via get_time().
    """
    records = []
    for line in text.splitlines():
        # Tag values can contain an escaped space (`\ `), which a plain
        # line.split(' ') would also split on; only the unescaped space that
        # separates tags from fields marks the real boundary.
        head_and_fields, ts = line.rsplit(' ', 1)
        head, fields = re.split(r'(?<!\\) ', head_and_fields, maxsplit=1)
        parts = head.split(',')
        values = {}
        for tag in parts[1:]:
            key, _, val = tag.partition('=')
            values[key] = val.replace('\\ ', ' ').replace('\\,', ',')
        for field in fields.split(','):
            key, _, val = field.partition('=')
            values[key] = (val.strip('"') if val.startswith('"')
                           else int(val.rstrip('i')))
        records.append(_Rec(values, datetime.fromtimestamp(
            int(ts) / 1_000_000_000, tz=UTC)))
    return records
