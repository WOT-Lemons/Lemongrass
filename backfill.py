#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Discover and backfill historical 24 Hours of Lemons lap data.

Searches the RaceMonitor API for past Real Hoopties, GP du Lac, and Halloween
Hoop races and writes lap data for a given car number to the laps/races InfluxDB
buckets by shelling out to laps.py -n for each race found.

Usage:
    # Preview what would be backfilled (no writes)
    uv run python backfill.py --dry-run

    # Run the backfill (default car 252, from 2021 onwards)
    uv run python backfill.py

    # Override car number for a specific race (e.g. 2022 Hoopties used car 253)
    uv run python backfill.py --override 120037:253

    # Validate that backfilled races have data in InfluxDB
    uv run python backfill.py --validate --override 120037:253

    # Backfill from a different year or for a different default car
    uv run python backfill.py --start-year 2023 --car 82

Required environment variables:
    RACEMONITOR_TOKEN      — RaceMonitor API token (always required)
    INFLUX_TELEMETRY_TOKEN — InfluxDB write token (required for --validate)
"""

import argparse
import importlib.util
import logging
import os
import pathlib
import subprocess
import sys
from datetime import datetime, timezone

from race_monitor import RaceMonitorClient

_migrate = importlib.util.spec_from_file_location(
    "migrate", pathlib.Path(__file__).parent / "migrate.py")
_migrate_mod = importlib.util.module_from_spec(_migrate)
_migrate.loader.exec_module(_migrate_mod)
validate_migration = _migrate_mod.validate_migration

LEMONS_SEARCH_TERMS = ['Real Hoopties', 'GP du Lac', 'Halloween Hoop']
DEFAULT_CAR_NUMBER = '252'
DEFAULT_START_YEAR = 2021


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


def run_backfill(races, default_car, overrides, dry_run=False):
    """Run laps.py -n for each race, using per-race car number overrides where set."""
    for race in races:
        race_id = str(race['ID'])
        car_number = resolve_car_number(race_id, default_car, overrides)
        if dry_run:
            logging.info("Would backfill race %s (%s) car %s",
                         race_id, race['Name'], car_number)
            continue
        logging.info("Backfilling race %s (%s) car %s",
                     race_id, race['Name'], car_number)
        result = subprocess.run(
            [sys.executable, 'laps.py', '-n', race_id, car_number],
            capture_output=False,
        )
        if result.returncode != 0:
            logging.error("Backfill failed for race %s car %s", race_id, car_number)


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
                ok = validate_migration(pairs, influx_client.query_api())
            sys.exit(0 if ok else 1)

        run_backfill(races, args.car_number, args.overrides, dry_run=args.dry_run)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
