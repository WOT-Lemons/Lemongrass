#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Discover and backfill historical 24 Hours of Lemons lap data.

Searches the RaceMonitor API for past Real Hoopties, GP du Lac, and Halloween
Hoop races and writes lap data for a given car number to the laps/races InfluxDB
buckets by shelling out to laps.py -n for each race found.

Usage:
    # Preview what would be backfilled (no writes)
    uv run python backfill.py --dry-run

    # Run the backfill (default car 252, from 2017 onwards). Races whose laps are
    # already complete and written under the current schema version are skipped.
    uv run python backfill.py

    # Force a re-backfill of every race, even ones already complete and current
    # (e.g. after bumping SCHEMA_VERSION in laps.py to migrate historical data)
    uv run python backfill.py --force

    # Override car number for a specific race (e.g. 2022 Hoopties used car 253)
    uv run python backfill.py --override 120037:253

    # Validate that backfilled races have data in InfluxDB
    uv run python backfill.py --validate --override 120037:253

    # Backfill from a different year or for a different default car
    uv run python backfill.py --start-year 2023 --car 82

Required environment variables:
    RACEMONITOR_TOKEN      — RaceMonitor API token (always required)
    INFLUX_TELEMETRY_TOKEN — InfluxDB token (read-only sufficient for --validate;
                             full backfill requires write access via laps.py)
"""

import argparse
import logging
import os
import pathlib
import subprocess
import sys
from datetime import datetime, timezone

from race_monitor import RaceMonitorClient

LEMONS_SEARCH_TERMS = ['Real Hoopties', 'GP du Lac', 'Halloween Hoop']
DEFAULT_CAR_NUMBER = '252'
DEFAULT_START_YEAR = 2017
EPOCH_START = '1970-01-01T00:00:00Z'


class _OverrideAction(argparse.Action):
    """argparse Action that accumulates RACE_ID:CAR_NUMBER pairs into a dict."""

    def __call__(self, parser, namespace, values, option_string=None):
        overrides = getattr(namespace, self.dest) or {}
        race_id, car = values.split(':', 1)
        overrides[race_id] = car
        setattr(namespace, self.dest, overrides)


def _build_parser():
    parser = argparse.ArgumentParser(
        description='Discover and backfill historical Lemons lap data.')
    parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=False,
                        help='Print what would be run without writing anything')
    parser.add_argument('--override', dest='overrides', metavar='RACE_ID:CAR_NUMBER',
                        action=_OverrideAction, default={},
                        help='Override car number for a specific race (repeatable)')
    parser.add_argument('--start-year', dest='start_year', type=int,
                        default=DEFAULT_START_YEAR,
                        help=f'Earliest year to include (default: {DEFAULT_START_YEAR})')
    parser.add_argument('--car', dest='car_number', default=DEFAULT_CAR_NUMBER,
                        help=f'Default car number (default: {DEFAULT_CAR_NUMBER})')
    parser.add_argument('--validate', dest='validate', action='store_true', default=False,
                        help='Check that every backfilled race has data in the new buckets')
    parser.add_argument('--force', dest='force', action='store_true', default=False,
                        help='Re-backfill every race even if its laps are already complete and '
                             'current; by default complete races are skipped')
    return parser


def find_matching_races(client, start_year_epoc):
    """Search for matching Lemons races at or after start_year_epoc.

    Makes one API call per search term and deduplicates by race ID.
    """
    seen = {}
    for term in LEMONS_SEARCH_TERMS:
        resp = client.results.search_results(term)
        for race in resp.get('Races', []):
            if race['StartDateEpoc'] >= start_year_epoc:
                seen[race['ID']] = race
    return sorted(seen.values(), key=lambda r: r['StartDateEpoc'])


def build_pairs(races, default_car, overrides):
    """Return (race_id, car_number) pairs for all races, applying overrides."""
    return [(str(race['ID']), resolve_car_number(str(race['ID']), default_car, overrides))
            for race in races]


def resolve_car_number(race_id, default, overrides):
    """Return the override car number for race_id, or default if none is set."""
    return overrides.get(str(race_id), default)


def validate_backfill(pairs, query_api):
    """Check every race has metadata and every expected car has laps; show counts."""
    by_race = {}
    for race_id, car_number in pairs:
        by_race.setdefault(race_id, []).append(car_number)

    all_ok = True
    for race_id, expected_cars in sorted(by_race.items()):
        race_tables = query_api.query(
            f'from(bucket: "races")\n'
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
        range_start = race_records[0].get_time().strftime('%Y-%m-%dT%H:%M:%SZ')
        end_epoc = race_records[0].get_value()
        if not end_epoc:
            logging.warning("race %s: end_time_epoc=0 in races bucket, using now() as range stop",
                            race_id)
        range_stop = (datetime.fromtimestamp(end_epoc, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                      if end_epoc else datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))

        lap_tables = query_api.query(
            f'from(bucket: "laps")\n'
            f'  |> range(start: {range_start}, stop: {range_stop})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{race_id}"\n'
            f'      and r._field == "lap_no")\n'
            f'  |> group(columns: ["car_number"])\n'
            f'  |> count()'
        )
        actual = {r.values['car_number']: r.values['_value']
                  for t in lap_tables for r in t.records}

        missing = sorted(set(expected_cars) - set(actual))
        if missing:
            logging.warning("race %s (%s): MISSING cars %s | have: %s",
                            race_id, race_name, missing,
                            [f'{c}={actual[c]}laps' for c in sorted(actual)])
            all_ok = False
        else:
            logging.info("race %s (%s): OK | cars: %s",
                         race_id, race_name,
                         [f'{c}={actual[c]}laps' for c in sorted(actual)])

    return all_ok


_LAPS_PY = str(pathlib.Path(__file__).parent / 'laps.py')


def run_backfill(races, default_car, overrides, dry_run=False, force=False):
    """Run laps.py -n for each race, using per-race car number overrides where set.

    Unless force is set, passes --skip-if-complete so laps.py skips races whose
    laps are already complete and written under the current schema version.
    """
    failures = []
    for race in races:
        race_id = str(race['ID'])
        car_number = resolve_car_number(race_id, default_car, overrides)
        cmd = [sys.executable, _LAPS_PY, '-n']
        if not force:
            cmd.append('--skip-if-complete')
        cmd += [race_id, car_number]
        if dry_run:
            logging.info("Would backfill race %s (%s) car %s",
                         race_id, race['Name'], car_number)
            continue
        logging.info("Backfilling race %s (%s) car %s",
                     race_id, race['Name'], car_number)
        result = subprocess.run(cmd, capture_output=False)
        if result.returncode != 0:
            logging.error("Backfill failed for race %s car %s", race_id, car_number)
            failures.append((race_id, car_number))
    if failures:
        logging.error("%d race(s) failed: %s", len(failures), failures)
    return failures


def main():
    """Entry point: parse args, discover races, then backfill or validate."""
    args = _build_parser().parse_args()

    token = os.environ.get('RACEMONITOR_TOKEN')
    if not token:
        logging.error("RACEMONITOR_TOKEN environment variable not set")
        sys.exit(1)

    start_year_epoc = int(datetime(args.start_year, 1, 1, tzinfo=timezone.utc).timestamp())

    with RaceMonitorClient(api_token=token) as client:
        races = find_matching_races(client, start_year_epoc)
        logging.info("Found %d matching races", len(races))

        if args.validate:
            influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
            if not influx_token:
                logging.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
                sys.exit(1)
            from influxdb_client import InfluxDBClient
            pairs = build_pairs(races, args.car_number, args.overrides)
            with InfluxDBClient(url='https://influxdb.focism.com',
                                token=influx_token, org='focism') as influx_client:
                ok = validate_backfill(pairs, influx_client.query_api())
            sys.exit(0 if ok else 1)

        failures = run_backfill(races, args.car_number, args.overrides,
                                dry_run=args.dry_run, force=args.force)
        if failures:
            sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
