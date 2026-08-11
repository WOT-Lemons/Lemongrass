"""Move race and session rows between the legacy Influx buckets and Postgres.

Two directions, one shape: ``import_legacy`` reads the ``races`` and
``race_sessions`` buckets and writes through the same ``_db`` statements the
application uses, and ``export_legacy`` renders the tables back out as Influx
line protocol. The export is what makes the cutover deploy reversible — races
written to Postgres afterwards have no Influx counterpart, so reverting the
code without it would lose every one of them.
"""
import logging
from datetime import UTC, datetime

from lemongrass import _db, _influx

EPOCH_START = '1970-01-01T00:00:00Z'
# range(start: 0) carries an implicit stop: now(). A race point can be
# timestamped in the future — race_ts_ms is start_epoc * 1000 for a race that
# is scheduled but has not started — which an implicit stop would hide.
FAR_FUTURE = '2100-01-01T00:00:00Z'


def _epoch_to_dt(epoch_s):
    """Convert a whole-second epoch to an aware datetime; 0 or None -> None."""
    return datetime.fromtimestamp(epoch_s, tz=UTC) if epoch_s else None


def _int_or_none(value):
    """Coerce a pivoted Influx field to int, or None when it is absent."""
    return None if value is None else int(value)


def read_legacy_races(query_api):
    """Read every race point from the legacy races bucket as RaceRows.

    series_id is None for every row: _resolve_race_metadata read SeriesID and
    discarded it, so it was never stored. Re-fetching it for ~184 races under
    the 6 req/min limit is not worth it, and 1b stores it going forward.
    """
    rows = []
    for table in query_api.query(
        f'from(bucket: "{_influx.BUCKET_RACES}")\n'
        f'  |> range(start: {EPOCH_START}, stop: {FAR_FUTURE})\n'
        f'  |> filter(fn: (r) => r._measurement == "race")\n'
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
    ):
        for record in table.records:
            vals = record.values
            rows.append(_db.RaceRow(
                race_id=vals.get('race_id'),
                race_time=record.get_time(),
                # Influx drops empty-string tags entirely, so these come back
                # absent for a race written from an unsuccessful details fetch.
                name=vals.get('race_name') or '',
                track_name=vals.get('track_name') or '',
                series_id=None,
                series_name=vals.get('series_name') or None,
                end_time=_epoch_to_dt(vals.get('end_time_epoc')),
                expected_lap_count=_int_or_none(vals.get('expected_lap_count')),
                session_count=_int_or_none(vals.get('session_count')),
                lap_schema_version=_int_or_none(vals.get('schema_version')),
            ))
    return rows


def read_legacy_sessions(query_api):
    """Read every session point from the legacy sessions bucket as SessionRows.

    session_id was a string tag in Influx and is a BIGINT column here.
    """
    rows = []
    for table in query_api.query(
        f'from(bucket: "{_influx.BUCKET_SESSIONS}")\n'
        f'  |> range(start: {EPOCH_START}, stop: {FAR_FUTURE})\n'
        f'  |> filter(fn: (r) => r._measurement == "session")\n'
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
    ):
        for record in table.records:
            vals = record.values
            rows.append(_db.SessionRow(
                session_id=int(vals.get('session_id')),
                race_id=vals.get('race_id'),
                name=vals.get('session_name') or '',
                start_time=_epoch_to_dt(vals.get('start_epoc')),
            ))
    return rows


def import_legacy(query_api, dry_run=False, only_missing=False):
    """Copy legacy Influx races and sessions into Postgres. Returns a summary.

    Races are written before sessions because sessions.race_id is a foreign
    key. A session whose race has no point is an orphan: it is skipped and
    reported, never repaired by inventing a stub race — that would fabricate
    data, and at least one such race (64202) is genuinely unexplained.

    only_missing inserts rows absent from Postgres and touches nothing else.
    Upsert-on-primary-key already makes a re-run non-duplicating, but that is
    not the same as harmless: once the new writer is live, an unguarded re-run
    replays stale Influx values over rows the application has since corrected,
    and the importer wins. The post-deploy catch-up run must use it.

    races_written/sessions_written count rows actually written and are always
    0 under dry_run. races_would_write/sessions_would_write count rows that
    reach the write step regardless of dry_run, so a `--dry-run` rehearsal
    still previews what a real run would do (races_read ≈ races_would_write is
    the runbook's healthy-run check). races_skipped_existing/
    sessions_skipped_existing count rows only_missing left alone because
    Postgres already had them; sessions_skipped keeps its original,
    orphan-only meaning so read == written-or-would-write + skipped +
    skipped_existing reconciles in every mode.
    """
    races = read_legacy_races(query_api)
    sessions = read_legacy_sessions(query_api)
    summary = {
        'races_read': len(races), 'races_written': 0,
        'races_would_write': 0, 'races_skipped_existing': 0,
        'sessions_read': len(sessions), 'sessions_written': 0,
        'sessions_would_write': 0, 'sessions_skipped_existing': 0,
        'sessions_skipped': 0, 'orphan_race_ids': [],
    }

    existing_races = {r.race_id for r in _db.list_races()}
    for row in races:
        if only_missing and row.race_id in existing_races:
            summary['races_skipped_existing'] += 1
            continue
        summary['races_would_write'] += 1
        if not dry_run:
            _db.upsert_race(row)
            summary['races_written'] += 1
        existing_races.add(row.race_id)

    existing_sessions = {s.session_id for s in _db.list_sessions()}
    orphans = set()
    for row in sessions:
        if row.race_id not in existing_races:
            orphans.add(row.race_id)
            summary['sessions_skipped'] += 1
            logging.warning(
                "session %s: race %s has no race point, skipping",
                row.session_id, row.race_id)
            continue
        if only_missing and row.session_id in existing_sessions:
            summary['sessions_skipped_existing'] += 1
            continue
        summary['sessions_would_write'] += 1
        if not dry_run:
            _db.upsert_session(row)
            summary['sessions_written'] += 1

    summary['orphan_race_ids'] = sorted(orphans)
    return summary


def _reject_line_breaks(what, value):
    """Raise if `value` carries a line break.

    Line protocol has no escape for LF/CR — the newline *is* the record
    delimiter — so a value carrying one splits the point into two corrupt
    records that Influx would half-accept. Fail the export loudly instead of
    writing a file that silently loses data on restore.
    """
    if '\n' in value or '\r' in value:
        raise ValueError(
            f"{what} contains a line break, which Influx line protocol cannot "
            f"represent: {value!r}")


def _escape_tag(value):
    """Escape a line-protocol tag value: backslashes, commas, spaces, equals.

    Backslash first, and it has to be: escaping it after the others would go
    back over the backslashes this function just inserted and double them.
    """
    _reject_line_breaks('tag value', value)
    return (value.replace('\\', '\\\\').replace(',', '\\,')
                 .replace(' ', '\\ ').replace('=', '\\='))


def _tags(pairs):
    """Render tag pairs, omitting empty values.

    Influx drops an empty tag value rather than storing it, so emitting one
    would not round trip — the reader would see the key present-but-empty
    where the original point had it absent.
    """
    return ''.join(f',{k}={_escape_tag(v)}' for k, v in pairs if v)


def _dt_to_epoch(value):
    """Whole-second epoch for an aware datetime; None -> 0.

    Zero is what the Influx schema used for "unknown", so a NULL column
    exports back to the value the old readers expect.
    """
    return int(value.timestamp()) if value is not None else 0


def race_line(row):
    """Render one race row as an Influx line-protocol point.

    The shape push_influx_race wrote: race_id / race_name / track_name /
    series_name tags, end_time_epoc always present (every legacy reader
    filters on it, so a point without it is invisible), the three completeness
    fields only when stored, and the race time as the point timestamp in
    nanoseconds.
    """
    tags = _tags([('race_id', row.race_id), ('race_name', row.name),
                  ('track_name', row.track_name),
                  ('series_name', row.series_name or '')])
    fields = [f'end_time_epoc={_dt_to_epoch(row.end_time)}i']
    if row.lap_schema_version is not None:
        fields.append(f'schema_version={row.lap_schema_version}i')
    if row.expected_lap_count is not None:
        fields.append(f'expected_lap_count={row.expected_lap_count}i')
    if row.session_count is not None:
        fields.append(f'session_count={row.session_count}i')
    # int(timestamp() * 1e9) loses ~100ns to float64 rounding; splitting whole
    # seconds from microseconds keeps the conversion exact.
    ts = row.race_time
    ns = int(ts.timestamp()) * 1_000_000_000 + ts.microsecond * 1000
    return f"race{tags} {','.join(fields)} {ns}"


def session_line(row):
    """Render one session row as an Influx line-protocol point.

    session_id goes back to being a string tag, and the point timestamp is the
    start epoch in nanoseconds — zero when the start time was never known,
    exactly as push_influx_session wrote it.
    """
    tags = _tags([('race_id', row.race_id), ('session_id', str(row.session_id))])
    start = _dt_to_epoch(row.start_time)
    _reject_line_breaks('session name', row.name or '')
    name = (row.name or '').replace('\\', '\\\\').replace('"', '\\"')
    fields = f'session_name="{name}",start_epoc={start}i'
    return f"session{tags} {fields} {start * 1_000_000_000}"


def export_legacy(out):
    """Write every stored race and session to `out` as line protocol.

    Races first, then sessions, so the output can be split at the first
    `session,` line and fed to `influx write` per bucket. Returns the counts.
    """
    races = _db.list_races()
    sessions = _db.list_sessions()
    for row in races:
        out.write(race_line(row) + '\n')
    for row in sessions:
        out.write(session_line(row) + '\n')
    return {'races': len(races), 'sessions': len(sessions)}
