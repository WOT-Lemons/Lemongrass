#!/usr/bin/env python
"""lemongrass races subcommand: inspect and manage race data stored in InfluxDB.

Subcommands: list, prune, backfill, diagnose.
Run `lemongrass races <subcommand> --help` for per-subcommand options.
"""

import argparse
import sys
from datetime import UTC, datetime

from lemongrass import _db, _influx

EPOCH_START = '1970-01-01T00:00:00Z'

_SUBCOMMANDS = ('list', 'prune', 'backfill', 'diagnose')


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
    {'list': _handle_list, 'prune': _handle_prune,
     'backfill': _handle_backfill, 'diagnose': _handle_diagnose}[subcmd]()


def run_races_tui(client):
    """Run the unified app opening directly on the races browser."""
    from lemongrass._home_tui import LemongrassApp
    from lemongrass._races_tui import RacesBrowserScreen
    from lemongrass._tui import _routed_output

    app = LemongrassApp(client, start_screen=RacesBrowserScreen())
    with _routed_output():
        app.run()
    return 0


def fetch_race_rows(query_api):
    """Return per-race rows for the stored races: id, name, date, total laps,
    current-schema lap count, and the schema version. Date-sorted, newest first.

    A hybrid read: race attributes come from Postgres, the two lap-count
    aggregates from Flux, joined here on race_id. Laps are genuinely
    time-series and stay in Influx, so there is no single store to ask.

    schema_version is the *current* SCHEMA_VERSION constant, not the value
    stored on the race — `races list` renders "stale (N/M at vX)" where X is
    the version the laps should be at.

    Shared by the CLI `races list` table and the interactive races browser so
    the two never drift."""
    from lemongrass.laps import SCHEMA_VERSION

    races = {
        row.race_id: {
            'race_id': row.race_id,
            'name': row.name or 'unknown',
            'date': row.race_time.strftime('%Y-%m-%d') if row.race_time else '?',
            'total': 0,
            'current': 0,
            'schema_version': SCHEMA_VERSION,
        }
        for row in _db.list_races()
    }

    for table in query_api.query(
        f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "lap" and r._field == "lap_no")\n'
        f'  |> group(columns: ["race_id"])\n'
        f'  |> count()'
    ):
        for record in table.records:
            rid = record.values.get('race_id')
            if rid in races:
                races[rid]['total'] = record.get_value()

    for table in query_api.query(
        f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "lap"\n'
        f'      and r._field == "schema_version" and r._value == {SCHEMA_VERSION})\n'
        f'  |> group(columns: ["race_id"])\n'
        f'  |> count()'
    ):
        for record in table.records:
            rid = record.values.get('race_id')
            if rid in races:
                races[rid]['current'] = record.get_value()

    return sorted(races.values(), key=lambda r: r['date'], reverse=True)


def _handle_list():
    """Print a table of all races in the races bucket with their total lap count and
    schema version status (current, stale, or no laps)."""
    with _influx.connect() as client:
        rows = fetch_race_rows(client.query_api())
        print(f"{'RACE ID':<10} {'NAME':<35} {'DATE':<12} {'LAPS':<8} SCHEMA")
        print('-' * 80)
        for info in rows:
            if info['total'] == 0:
                schema_str = 'no laps'
            elif info['current'] == info['total']:
                schema_str = f'current (v{info["schema_version"]})'
            else:
                schema_str = (f'stale   ({info["current"]}/{info["total"]} '
                              f'at v{info["schema_version"]})')
            print(f"{info['race_id']:<10} {info['name'][:35]:<35} {info['date']:<12} "
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


def _handle_prune():
    """Parse args and delete all data for the specified race(s) from InfluxDB,
    prompting for confirmation unless --yes is passed."""
    parser = argparse.ArgumentParser(
        prog='lemongrass-races-prune',
        description='Delete all data for one or more races from InfluxDB')
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

        not_found = [rid for rid in race_ids if rid not in race_names]
        if not_found:
            print("race(s) not found:", " ".join(not_found), file=sys.stderr)
            sys.exit(1)

        if not args.yes:
            print(f"About to delete data for {len(race_ids)} race(s):")
            for rid in race_ids:
                print(f"  {rid}  {race_names[rid]}")
            answer = input("Proceed? [y/N] ")
            if answer.strip().lower() != 'y':
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
