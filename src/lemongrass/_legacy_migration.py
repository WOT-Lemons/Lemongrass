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
