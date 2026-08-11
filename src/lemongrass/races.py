#!/usr/bin/env python
"""lemongrass races subcommand: inspect and manage stored race data.

Race/session attributes live in Postgres; laps, standings, and lap counts
stay in InfluxDB, so several reads and the prune below are hybrid.

Subcommands: list, prune, backfill, diagnose, identify.
Run `lemongrass races <subcommand> --help` for per-subcommand options.
"""

import argparse
import sys
from datetime import UTC, datetime

from lemongrass import _db, _influx, _prompt

EPOCH_START = '1970-01-01T00:00:00Z'

_SUBCOMMANDS = ('list', 'prune', 'backfill', 'diagnose', 'identify')


def main():
    """Entry point for `lemongrass races`. With no args on an interactive TTY,
    opens the unified races browser; otherwise dispatches to the appropriate
    subcommand handler (list, prune, backfill, diagnose) based on the first
    argument."""
    if len(sys.argv) == 1 and sys.stdin.isatty() and sys.stdout.isatty():
        from lemongrass._tui import launch_tui
        launch_tui(run_races_tui)

    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass races <subcommand> [args]")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        sys.exit(1)
    subcmd = sys.argv.pop(1)
    sys.argv[0] = f'lemongrass-races-{subcmd}'
    # Returned, not discarded: cli.main exits with this, and `identify` uses a
    # non-zero code to report a race id that has no stored row. The other
    # handlers return None, which sys.exit already treats as 0.
    return {'list': _handle_list, 'prune': _handle_prune,
            'backfill': _handle_backfill, 'diagnose': _handle_diagnose,
            'identify': _handle_identify}[subcmd]()


def run_races_tui(client):
    """Run the unified app opening directly on the races browser."""
    from lemongrass._home_tui import LemongrassApp
    from lemongrass._races_tui import RacesBrowserScreen
    from lemongrass._tui import _routed_output

    app = LemongrassApp(client, start_screen=RacesBrowserScreen())
    with _routed_output():
        app.run()
    return 0


def _count_laps_by_race(query_api, races, key, predicate):
    """Count lap points per race and store each total under `races[rid][key]`.

    `predicate` is the field-selecting half of the filter; the rest of the
    query — bucket, range, measurement, grouping, count — is identical for both
    counts fetch_race_rows needs, so it lives here once. Race ids with no entry
    in `races` are dropped: the laps bucket outlives the race rows, so it
    carries pre-cutover races that were never imported."""
    for table in query_api.query(
        f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "lap"\n'
        f'      and {predicate})\n'
        f'  |> group(columns: ["race_id"])\n'
        f'  |> count()'
    ):
        for record in table.records:
            rid = record.values.get('race_id')
            if rid in races:
                races[rid][key] = record.get_value()


def fetch_race_rows(query_api):
    """Return per-race rows for the stored races: id, name, date, total laps,
    current-schema lap count, and the schema version. Date-sorted, newest first.

    A hybrid read: race attributes come from Postgres, the two lap-count
    aggregates from Flux, joined here on race_id. Laps are genuinely
    time-series and stay in Influx, so there is no single store to ask.

    schema_version is the *current* SCHEMA_VERSION constant, not the value
    stored on the race — `races list` renders "stale (N/M at vX)" where X is
    the version the laps should be at.

    venue_name/event_name are the joined curated names, blank when the race
    resolved to nothing — which is itself the prompt to run `races identify`.
    (The dict keys deliberately match the spec's names and the `RaceListRow`
    attributes, because sub-project 3's dashboard work consumes this surface.)

    Shared by the CLI `races list` table and the interactive races browser so
    the two never drift."""
    from lemongrass.laps import SCHEMA_VERSION

    races = {
        row.race_id: {
            'race_id': row.race_id,
            'name': row.name or 'unknown',
            # psycopg hands back TIMESTAMPTZ in the connection's timezone, not
            # necessarily UTC, so normalize before formatting — otherwise a
            # host behind UTC renders a late-evening race on the day before.
            # race_backfill.validate_backfill normalizes for the same reason.
            'date': (row.race_time.astimezone(UTC).strftime('%Y-%m-%d')
                     if row.race_time else '?'),
            'venue_name': row.venue_name or '',
            'event_name': row.event_name or '',
            'total': 0,
            'current': 0,
            'schema_version': SCHEMA_VERSION,
        }
        for row in _db.list_races_with_venue()
    }

    _count_laps_by_race(query_api, races, 'total', 'r._field == "lap_no"')
    _count_laps_by_race(
        query_api, races, 'current',
        f'r._field == "schema_version" and r._value == {SCHEMA_VERSION}')

    return sorted(races.values(), key=lambda r: r['date'], reverse=True)


def _handle_list():
    """Print a table of all stored races with their total lap count and
    schema version status (current, stale, or no laps)."""
    with _influx.connect() as client:
        rows = fetch_race_rows(client.query_api())
        print(f"{'RACE ID':<10} {'NAME':<24} {'VENUE':<18} {'DATE':<12} "
              f"{'LAPS':<8} SCHEMA")
        print('-' * 91)
        for info in rows:
            if info['total'] == 0:
                schema_str = 'no laps'
            elif info['current'] == info['total']:
                schema_str = f'current (v{info["schema_version"]})'
            else:
                schema_str = (f'stale   ({info["current"]}/{info["total"]} '
                              f'at v{info["schema_version"]})')
            print(f"{info['race_id']:<10} {info['name'][:24]:<24} "
                  f"{info['venue_name'][:18]:<18} {info['date']:<12} "
                  f"{info['total']:<8} {schema_str}")


def prune_races(delete_api, race_ids, on_progress=None, on_error=None):
    """Delete all data for each race id across the session/lap/standings/race buckets
    and the races table.

    The Postgres race row is deleted LAST, and its cascade takes the sessions
    with it. The caller's not-found guard keys off that row, so as long as it
    survives, a retry after a partial failure can still find and clean up the
    orphaned Influx data. Deleting it first would invert that: an Influx
    delete that then failed would leave laps with nothing to find them by.

    The legacy Influx race and session deletes are kept: those buckets stop
    being written at cutover but still hold every pre-cutover race, and prune
    is the only thing that would ever clean them up.

    on_progress(message) is called after each successful delete. Per-race
    errors are reported via on_error(message) when provided; otherwise they
    fall back to on_progress so the message is never lost. Returns the ids
    that failed."""
    def _note(msg):
        if on_progress:
            on_progress(msg)

    def _note_error(msg):
        if on_error:
            on_error(msg)
        else:
            _note(msg)

    now = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    failed = []
    for rid in race_ids:
        try:
            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="session" AND race_id="{rid}"',
                              bucket=_influx.BUCKET_SESSIONS)
            _note(f"Deleted sessions for race {rid}")

            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="lap" AND race_id="{rid}"',
                              bucket=_influx.BUCKET_LAPS)
            _note(f"Deleted laps for race {rid}")

            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="standings" AND race_id="{rid}"',
                              bucket=_influx.BUCKET_LAPS)
            _note(f"Deleted standings for race {rid}")

            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="race" AND race_id="{rid}"',
                              bucket=_influx.BUCKET_RACES)
            _note(f"Deleted legacy race metadata for race {rid}")

            if _db.delete_race(rid):
                _note(f"Deleted race row for race {rid}")
            else:
                _note(f"No race row stored for race {rid}")
        except Exception as e:  # record-and-continue across races
            _note_error(f"error pruning race {rid}: {e}")
            failed.append(rid)
    return failed


def _influx_race_names(query_api, race_ids):
    """Names for the given races as recorded in the legacy Influx races bucket.

    Only prune needs this. Every pre-cutover race has laps, standings and a
    race point in Influx but no Postgres row until `db import-legacy` runs,
    and prune is the only thing that would ever clean that data up — so its
    not-found guard has to see both stores, not just the new one.

    A race that was renamed has two points carrying different race_name tags.
    Tags are part of the series key, so those land in different Flux tables
    and a bare ``last()`` — which runs per table — would leave the answer to
    table order. The query regroups on race_id and sorts before taking the
    last, and the newest ``_time`` wins again on the Python side, so the name
    shown is the current one however the records arrive.

    Callers pass ids already through ``_influx.invalid_flux_ids``, which is
    what makes the interpolation below safe.
    """
    from lemongrass._legacy_migration import FAR_FUTURE
    predicate = ' or '.join(f'r.race_id == "{rid}"' for rid in race_ids)
    best = {}
    for table in query_api.query(
            f'from(bucket: "{_influx.BUCKET_RACES}")\n'
            f'  |> range(start: {EPOCH_START}, stop: {FAR_FUTURE})\n'
            f'  |> filter(fn: (r) => r._measurement == "race")\n'
            f'  |> filter(fn: (r) => {predicate})\n'
            f'  |> group(columns: ["race_id"])\n'
            f'  |> sort(columns: ["_time"])\n'
            f'  |> last()'):
        for record in table.records:
            rid = record.values.get('race_id')
            if not rid:
                continue
            when = record.get_time() or datetime.min.replace(tzinfo=UTC)
            if rid not in best or when >= best[rid][0]:
                best[rid] = (when, record.values.get('race_name') or 'unknown')
    return {rid: name for rid, (_, name) in best.items()}


def _handle_prune():
    """Parse args and delete all data for the specified race(s) — the Postgres
    race/session rows plus the InfluxDB laps/standings/legacy race data,
    prompting for confirmation unless --yes is passed."""
    parser = argparse.ArgumentParser(
        prog='lemongrass-races-prune',
        description='Delete all data for one or more races')
    parser.add_argument('race_id', nargs='+')
    parser.add_argument('--yes', action='store_true', default=False,
                        help='Skip confirmation prompt')
    args = parser.parse_args()
    race_ids = list(dict.fromkeys(args.race_id))

    invalid_ids = _influx.invalid_flux_ids(race_ids)
    if invalid_ids:
        print("invalid race ID(s):", ", ".join(f'"{r}"' for r in invalid_ids), file=sys.stderr)
        sys.exit(1)

    with _influx.connect() as client:
        race_names = {}
        for rid in race_ids:
            row = _db.get_race(rid)
            if row is not None:
                race_names[rid] = row.name or 'unknown'

        missing = [rid for rid in race_ids if rid not in race_names]
        if missing:
            race_names.update(_influx_race_names(client.query_api(), missing))

        not_found = [rid for rid in race_ids if rid not in race_names]
        if not_found:
            print("race(s) not found:", " ".join(not_found), file=sys.stderr)
            sys.exit(1)

        if not args.yes:
            print(f"About to delete data for {len(race_ids)} race(s):")
            for rid in race_ids:
                print(f"  {rid}  {race_names[rid]}")
            if not _prompt.ask_yes("Proceed? [y/N] "):
                print("Aborted.")
                sys.exit(0)

        delete_api = client.delete_api()
        failed = prune_races(delete_api, race_ids, on_progress=print,
                             on_error=lambda m: print(m, file=sys.stderr))
        if failed:
            print("failed to prune:", " ".join(failed), file=sys.stderr)
            sys.exit(1)


def _handle_backfill():
    """Delegate to lemongrass race-backfill (race_backfill.main)."""
    from lemongrass import race_backfill
    race_backfill.main()


def _handle_diagnose():
    """Delegate to lemongrass race-diagnose (race_diagnose.main)."""
    from lemongrass import race_diagnose
    race_diagnose.main()


def identify_races(race_ids=None, dry_run=False):
    """Re-resolve stored races' identity columns from their stored text.

    Reads track_name, name, and series_id straight out of Postgres and runs
    them back through the curated resolver, so this makes no RaceMonitor calls
    at all: it is free of the 6 req/min limit and works offline. Only rows
    whose ids actually changed are written.

    Returns (changes, unresolved, missing): changes is a list of
    (race_id, before_ids, after_ids) triples; unresolved maps each track name
    that matched no venue to how many races carry it — the worklist for the
    next tracks.toml edit; missing lists the requested race ids that have no
    stored row, so a typo reads differently from "already correct".
    """
    from lemongrass import _tracks
    if not dry_run:
        # The identity columns are foreign keys; a file edit that added a venue
        # would otherwise fail every UPDATE that used it.
        _db.sync_tracks(_tracks.data())
    rows = _db.list_races()
    missing = []
    if race_ids:
        wanted = set(race_ids)
        rows = [row for row in rows if row.race_id in wanted]
        missing = sorted(wanted - {row.race_id for row in rows})
    changes, unresolved = [], {}
    for row in rows:
        identity = _tracks.resolve(row.track_name, row.name, row.series_id)
        before = (row.venue_id, row.layout_id, row.event_id)
        after = (identity.venue_id, identity.layout_id, identity.event_id)
        if identity.venue_id is None:
            unresolved[row.track_name] = unresolved.get(row.track_name, 0) + 1
        if before != after:
            changes.append((row.race_id, before, after))
            if not dry_run:
                _db.set_race_identity(row.race_id, *after)
    return changes, unresolved, missing


def _format_ids(ids):
    """Render an identity triple for the before/after report."""
    return '/'.join(part or '-' for part in ids)


def _handle_identify():
    """Parse args and re-tag stored races from the curated track data."""
    parser = argparse.ArgumentParser(
        prog='lemongrass-races-identify',
        description='Re-resolve stored races against the curated track data')
    parser.add_argument('race_id', nargs='*')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Report what would change without writing')
    args = parser.parse_args()

    changes, unresolved, missing = identify_races(
        race_ids=args.race_id or None, dry_run=args.dry_run)
    for race_id, before, after in changes:
        print(f"{race_id:<10} {_format_ids(before)} -> {_format_ids(after)}")
    verb = 'would change' if args.dry_run else 'changed'
    print(f"{len(changes)} race(s) {verb}")
    if unresolved:
        print("unresolved track names (add to tracks.toml):")
        for name, count in sorted(unresolved.items(),
                                  key=lambda kv: (-kv[1], kv[0])):
            print(f"  {count:4d}  {name or '(blank)'}")
    for race_id in missing:
        # Without this a typo'd id is indistinguishable from a race that was
        # already tagged correctly: both print "0 race(s) changed".
        print(f"No race row stored for race {race_id}", file=sys.stderr)
    return 1 if missing else 0
