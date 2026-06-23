#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""lemongrass races subcommand dispatcher."""

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient

INFLUX_URL = 'https://influxdb.focism.com'
INFLUX_ORG = 'focism'
EPOCH_START = '1970-01-01T00:00:00Z'

_SUBCOMMANDS = ('list', 'prune', 'backfill', 'diagnose')
_RACE_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass races <subcommand> [args]")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        sys.exit(1)
    subcmd = sys.argv.pop(1)
    sys.argv[0] = f'lemongrass-races-{subcmd}'
    {'list': _handle_list, 'prune': _handle_prune,
     'backfill': _handle_backfill, 'diagnose': _handle_diagnose}[subcmd]()


def _require_influx_token():
    token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
    if not token:
        logging.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
        sys.exit(1)
    return token


def _handle_list():
    from lemongrass.laps import SCHEMA_VERSION

    token = _require_influx_token()
    with InfluxDBClient(url=INFLUX_URL, token=token, org=INFLUX_ORG) as client:
        query_api = client.query_api()

        races_tables = query_api.query(
            f'from(bucket: "races")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "race" and r._field == "end_time_epoc")\n'
        )
        races = {}
        for table in races_tables:
            for record in table.records:
                race_id = record.values.get('race_id')
                races[race_id] = {
                    'name': record.values.get('race_name', 'unknown'),
                    'date': record.get_time().strftime('%Y-%m-%d') if record.get_time() else '?',
                    'total': 0,
                    'current': 0,
                }

        total_tables = query_api.query(
            f'from(bucket: "laps")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap" and r._field == "lap_no")\n'
            f'  |> group(columns: ["race_id"])\n'
            f'  |> count()'
        )
        for table in total_tables:
            for record in table.records:
                rid = record.values.get('race_id')
                if rid in races:
                    races[rid]['total'] = record.get_value()

        current_tables = query_api.query(
            f'from(bucket: "laps")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r._field == "schema_version" and r._value == {SCHEMA_VERSION})\n'
            f'  |> group(columns: ["race_id"])\n'
            f'  |> count()'
        )
        for table in current_tables:
            for record in table.records:
                rid = record.values.get('race_id')
                if rid in races:
                    races[rid]['current'] = record.get_value()

        sorted_races = sorted(races.items(), key=lambda kv: kv[1]['date'], reverse=True)
        print(f"{'RACE ID':<10} {'NAME':<35} {'DATE':<12} {'LAPS':<8} SCHEMA")
        print('-' * 80)
        for race_id, info in sorted_races:
            if info['total'] == 0:
                schema_str = 'no laps'
            elif info['current'] == info['total']:
                schema_str = f'current (v{SCHEMA_VERSION})'
            else:
                schema_str = f'stale   ({info["current"]}/{info["total"]} at v{SCHEMA_VERSION})'
            print(f"{race_id:<10} {info['name'][:35]:<35} {info['date']:<12} "
                  f"{info['total']:<8} {schema_str}")


def _handle_prune():
    parser = argparse.ArgumentParser(prog='lemongrass-races-prune',
                                     description='Delete all data for one or more races from InfluxDB')
    parser.add_argument('race_id', nargs='+')
    parser.add_argument('--yes', action='store_true', default=False,
                        help='Skip confirmation prompt')
    args = parser.parse_args()
    race_ids = args.race_id

    invalid_ids = [rid for rid in race_ids if not _RACE_ID_RE.fullmatch(rid)]
    if invalid_ids:
        print("invalid race ID(s):", " ".join(invalid_ids), file=sys.stderr)
        sys.exit(1)

    token = _require_influx_token()

    with InfluxDBClient(url=INFLUX_URL, token=token, org=INFLUX_ORG) as client:
        query_api = client.query_api()

        ids_set = '["' + '", "'.join(race_ids) + '"]'
        races_tables = query_api.query(
            f'from(bucket: "races")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "race" and r._field == "end_time_epoc"\n'
            f'      and contains(value: r.race_id, set: {ids_set}))\n'
            f'  |> group(columns: ["race_id"])\n'
            f'  |> first()'
        )
        race_names = {}
        for table in races_tables:
            for record in table.records:
                rid = record.values.get('race_id')
                race_names[rid] = record.values.get('race_name', 'unknown')

        not_found = [rid for rid in race_ids if rid not in race_names]
        if not_found:
            print("race(s) not found in InfluxDB:", " ".join(not_found), file=sys.stderr)
            sys.exit(1)

        if not args.yes:
            print(f"About to delete data for {len(race_ids)} race(s):")
            for rid in race_ids:
                print(f"  {rid}  {race_names[rid]}")
            answer = input("Proceed? [y/N] ")
            if answer.strip().lower() != 'y':
                print("Aborted.")
                sys.exit(0)

        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        delete_api = client.delete_api()

        for rid in race_ids:
            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="lap" AND race_id="{rid}"',
                              bucket='laps')
            print(f"Deleted laps for race {rid}")

            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="race" AND race_id="{rid}"',
                              bucket='races')
            print(f"Deleted race metadata for race {rid}")

            delete_api.delete(start=EPOCH_START, stop=now,
                              predicate=f'_measurement="session" AND race_id="{rid}"',
                              bucket='race_sessions')
            print(f"Deleted sessions for race {rid}")


def _handle_backfill():
    from lemongrass import race_backfill
    race_backfill.main()


def _handle_diagnose():
    from lemongrass import race_diagnose
    race_diagnose.main()
