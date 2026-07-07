#!/usr/bin/env python
"""Discover and backfill historical 24 Hours of Lemons lap data.

Searches the RaceMonitor API for past Real Hoopties, GP du Lac, and Halloween
Hoop races and writes lap data for a given car number to the laps/races InfluxDB
buckets by invoking `lemongrass laps -n` for each race found.

Assumes `lemongrass` is installed as a CLI tool. If running from the repo, prefix
commands with `uv run` (e.g. `uv run lemongrass race-backfill`).

Usage:
    # Preview what would be backfilled (no writes)
    lemongrass race-backfill --dry-run

    # Run the backfill (default car 252, from 2017 onwards). Races whose laps are
    # already complete and written under the current schema version are skipped.
    lemongrass race-backfill

    # Force a re-backfill of every race, even ones already complete and current
    # (e.g. after bumping SCHEMA_VERSION in laps to migrate historical data)
    lemongrass race-backfill --force

    # Override car number for a specific race (e.g. 2022 Hoopties used car 253)
    lemongrass race-backfill --override 120037:253

    # Validate that backfilled races have data in InfluxDB
    lemongrass race-backfill --validate --override 120037:253

    # Backfill from a different year or for a different default car
    lemongrass race-backfill --start-year 2023 --car 82

    # Re-write laps stored under an older schema version without re-fetching from
    # RaceMonitor (faster than --force when only the schema tag changed)
    lemongrass race-backfill --upgrade-stored

Required environment variables:
    RACEMONITOR_TOKENS     — comma-separated RaceMonitor API tokens (preferred)
    RACEMONITOR_TOKEN      — single RaceMonitor API token (fallback)
    INFLUX_TELEMETRY_TOKEN — InfluxDB token (read-only sufficient for --validate;
                             full backfill requires write access via the laps command)
"""

import argparse
import logging
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta

from race_monitor import RaceMonitorClient

from lemongrass import _config, _env, _influx
from lemongrass._env import resolve_tokens

_backfill_cfg = _config.load_config().races.backfill
LEMONS_SEARCH_TERMS = _backfill_cfg.search_terms
DEFAULT_START_DATE = _backfill_cfg.default_start_date
EPOCH_START = '1970-01-01T00:00:00Z'

WINDOW_PAD_S = _influx.WINDOW_PAD_S


def _parse_start_date(value):
    """Parse a YYYY-MM-DD start date into a UTC-midnight epoch; exit 1 on bad input."""
    try:
        d = date.fromisoformat(value)
    except ValueError:
        logging.error("invalid --start-date %r: expected YYYY-MM-DD", value)
        sys.exit(1)
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())


def _build_parser():
    """Build and return the argument parser for lemongrass race-backfill."""
    parser = argparse.ArgumentParser(
        description='Discover and backfill historical Lemons lap data.')
    parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=False,
                        help='Print what would be run without writing anything')
    parser.add_argument('--start-date', dest='start_date', default=DEFAULT_START_DATE,
                        help=f'Earliest race date to include, YYYY-MM-DD '
                             f'(default: {DEFAULT_START_DATE})')
    parser.add_argument('--validate', dest='validate', action='store_true', default=False,
                        help='Check that every backfilled race has data in the new buckets')
    parser.add_argument('--force', dest='force', action='store_true', default=False,
                        help='Re-backfill every race even if its laps are already complete and '
                             'current; by default complete races are skipped')
    parser.add_argument('--upgrade-stored', dest='upgrade_stored', action='store_true',
                       default=False,
                       help='Re-backfill stored races with schema versions older than current; '
                            'mutually exclusive with --start-date and --validate. Combine with '
                            '--force to also re-fetch races already at the current schema '
                            '(re-queries every race from RaceMonitor, subject to its rate limit)')
    return parser


def find_matching_races(client, start_epoc):
    """Search for matching Lemons races at or after start_epoc.

    Makes one API call per search term and deduplicates by race ID.
    """
    seen = {}
    for term in LEMONS_SEARCH_TERMS:
        resp = client.results.search_results(term)
        for race in resp.get('Races', []):
            if race['StartDateEpoc'] >= start_epoc:
                seen[race['ID']] = race
    return sorted(seen.values(), key=lambda r: r['StartDateEpoc'])


def validate_backfill(race_ids, query_api):
    """Check every race has metadata and at least one lap in the field; show counts."""
    all_ok = True
    for race_id in sorted(set(race_ids)):
        race_tables = query_api.query(
            f'from(bucket: "{_influx.BUCKET_RACES}")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "race"\n'
            f'      and r.race_id == "{race_id}")\n'
            f'  |> filter(fn: (r) => r._field == "end_time_epoc")\n'
            f'  |> first()'
        )
        race_records = [r for t in race_tables for r in t.records]
        if not race_records:
            logging.warning("race %s: metadata MISSING", race_id)
            all_ok = False
            continue

        race_name = race_records[0].values.get('race_name', 'unknown')
        range_start = (
            race_records[0].get_time() - timedelta(seconds=WINDOW_PAD_S)
        ).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_epoc = race_records[0].get_value()
        if not end_epoc:
            logging.warning("race %s: end_time_epoc=0 in races bucket, using now() as range stop",
                            race_id)
        range_stop = (
            datetime.fromtimestamp(end_epoc + WINDOW_PAD_S, tz=UTC)
            .strftime('%Y-%m-%dT%H:%M:%SZ')
            if end_epoc else datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        )

        lap_tables = query_api.query(
            f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
            f'  |> range(start: {range_start}, stop: {range_stop})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{race_id}"\n'
            f'      and r._field == "lap_no")\n'
            f'  |> count()'
        )
        total = sum(r.get_value() for t in lap_tables for r in t.records)

        if total == 0:
            logging.warning("race %s (%s): NO laps in field", race_id, race_name)
            all_ok = False
        else:
            logging.info("race %s (%s): OK | %d laps", race_id, race_name, total)

    return all_ok


def run_backfill(races, dry_run=False, force=False):
    """Run `lemongrass laps -n` for each race (fieldwide historical import).

    Unless force is set, passes --skip-if-complete so `lemongrass laps` skips races
    whose laps are already complete and written under the current schema version.
    Returns the list of race IDs that failed.
    """
    failures = []
    for race in races:
        race_id = str(race['ID'])
        cmd = ['lemongrass', 'laps', '-n']
        if not force:
            cmd.append('--skip-if-complete')
        cmd.append(race_id)
        if dry_run:
            logging.info("Would backfill race %s (%s)", race_id, race['Name'])
            continue
        logging.info("Backfilling race %s (%s)", race_id, race['Name'])
        result = subprocess.run(cmd, capture_output=False)
        if result.returncode == 130:
            logging.info("laps was interrupted; stopping backfill.")
            break
        if result.returncode != 0:
            logging.error("Backfill failed for race %s", race_id)
            failures.append(race_id)
    if failures:
        logging.error("%d race(s) failed: %s", len(failures), failures)
    return failures


def run_upgrade_stored(query_api, dry_run=False, force=False):
    """Query InfluxDB for stored races with stale schema versions and re-backfill them.

    A race is skipped only when both its laps and its standings are at the current
    SCHEMA_VERSION; laps that are current but whose standings are stale or missing
    are re-backfilled. force=True re-backfills every stored race regardless.
    Re-backfill calls `lemongrass laps -n <race_id>` with no car_number (fieldwide mode).
    """
    from lemongrass.laps import SCHEMA_VERSION

    races_tables = query_api.query(
        f'from(bucket: "{_influx.BUCKET_RACES}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "race" and r._field == "end_time_epoc")\n'
    )
    stored_races = {}
    for table in races_tables:
        for record in table.records:
            race_id = record.values.get('race_id')
            stored_races[race_id] = record.values.get('race_name', 'unknown')

    failures = []
    for race_id, race_name in sorted(stored_races.items()):
        total_tables = query_api.query(
            f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{race_id}" and r._field == "lap_no")\n'
            f'  |> count()'
        )
        total = sum(r.get_value() for t in total_tables for r in t.records)

        current_tables = query_api.query(
            f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{race_id}"\n'
            f'      and r._field == "schema_version" and r._value == {SCHEMA_VERSION})\n'
            f'  |> count()'
        )
        current = sum(r.get_value() for t in current_tables for r in t.records)

        if total == 0:
            logging.info("race %s (%s): no laps stored, skipping", race_id, race_name)
            continue

        if current == total and not force:
            # Laps are current and we're not forcing; only skip if standings are
            # fresh too. A prior re-backfill whose standings phase failed leaves v4
            # laps but stale or missing standings, which a lap-only check would
            # wrongly treat as migrated. Standings are queried lazily, only once
            # laps are current. (--force bypasses this and re-backfills regardless.)
            std_total_tables = query_api.query(
                f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
                f'  |> range(start: {EPOCH_START})\n'
                f'  |> filter(fn: (r) => r._measurement == "standings"\n'
                f'      and r.race_id == "{race_id}" and r._field == "position")\n'
                f'  |> count()'
            )
            std_total = sum(r.get_value() for t in std_total_tables for r in t.records)

            std_current_tables = query_api.query(
                f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
                f'  |> range(start: {EPOCH_START})\n'
                f'  |> filter(fn: (r) => r._measurement == "standings"\n'
                f'      and r.race_id == "{race_id}"\n'
                f'      and r._field == "schema_version" and r._value == {SCHEMA_VERSION})\n'
                f'  |> count()'
            )
            std_current = sum(r.get_value() for t in std_current_tables for r in t.records)

            if std_total > 0 and std_current == std_total:
                logging.info("race %s (%s): already at schema v%d, skipping",
                            race_id, race_name, SCHEMA_VERSION)
                continue

            logging.info(
                "race %s (%s): laps current but standings stale/missing (%d/%d), %s",
                race_id, race_name, std_current, std_total,
                "would re-backfill" if dry_run else "re-backfilling")
        elif current == total:  # current == total and force
            logging.info("race %s (%s): already current, %s",
                        race_id, race_name,
                        "would force re-backfill" if dry_run else "force re-backfilling")
        else:
            logging.info("race %s (%s): stale (%d/%d at current schema), %s",
                        race_id, race_name, current, total,
                        "would re-backfill" if dry_run else "re-backfilling")
        if dry_run:
            continue

        result = subprocess.run(['lemongrass', 'laps', '-n', str(race_id)], capture_output=False)
        if result.returncode == 130:
            logging.info("laps was interrupted; stopping upgrade.")
            break
        if result.returncode != 0:
            logging.error("Re-backfill failed for race %s", race_id)
            failures.append(race_id)

    if failures:
        logging.error("%d race(s) failed to upgrade: %s", len(failures), failures)
    return failures


def main():
    """Entry point: parse args, discover races, then backfill or validate."""
    args = _build_parser().parse_args()

    if args.upgrade_stored:
        if args.start_date != DEFAULT_START_DATE or args.validate:
            logging.error(
                "--upgrade-stored is mutually exclusive with --start-date and --validate")
            sys.exit(1)
        try:
            with _influx.connect() as influx_client:
                failures = run_upgrade_stored(influx_client.query_api(),
                                              dry_run=args.dry_run, force=args.force)
        except KeyboardInterrupt:
            logging.info("Interrupted, exiting.")
            sys.exit(130)
        sys.exit(1 if failures else 0)

    tokens = resolve_tokens()
    if not tokens:
        logging.error("%s environment variable not set", _env.tokens_env_hint())
        sys.exit(1)

    start_epoc = _parse_start_date(args.start_date)

    try:
        with RaceMonitorClient(api_token=tokens) as client:
            races = find_matching_races(client, start_epoc)
            logging.info("Found %d matching races", len(races))

            if args.validate:
                race_ids = [str(r['ID']) for r in races]
                with _influx.connect() as influx_client:
                    ok = validate_backfill(race_ids, influx_client.query_api())
                sys.exit(0 if ok else 1)

            failures = run_backfill(races, dry_run=args.dry_run, force=args.force)
            if failures:
                sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
