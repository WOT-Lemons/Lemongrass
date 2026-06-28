#!/usr/bin/env python
"""Diagnose lap data discrepancies for a specific race and car number.

Queries both the RaceMonitor API and InfluxDB and prints side-by-side lap
counts, session breakdown, and all stored lap numbers so you can pinpoint
whether a count mismatch is an API issue or a write issue.

Assumes `lemongrass` is installed as a CLI tool. If running from the repo, prefix
with `uv run` (e.g. `uv run lemongrass race-diagnose 144185 252`).

Usage:
    lemongrass race-diagnose <race_id> <car_number>

Example:
    lemongrass race-diagnose 144185 252

Required environment variables:
    RACEMONITOR_TOKENS     — comma-separated RaceMonitor API tokens (preferred)
    RACEMONITOR_TOKEN      — single RaceMonitor API token (fallback)
    INFLUX_TELEMETRY_TOKEN — InfluxDB read token
"""

import os
import sys
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient
from race_monitor import RaceMonitorClient

from lemongrass._env import resolve_tokens
from lemongrass._influx import INFLUX_ORG, INFLUX_RETRIES, INFLUX_URL

EPOCH_START = '1970-01-01T00:00:00Z'


def epoc_to_str(epoc):
    """Convert a Unix epoch integer to a human-readable UTC string."""
    if not epoc:
        return f'{epoc} (zero/null)'
    return datetime.fromtimestamp(epoc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def diagnose_api(client, race_id, car_number):
    """Print race metadata and per-session lap counts from the RaceMonitor API.

    Returns (start_epoc, end_epoc) from race details, or (0, 0) if unavailable.
    """
    print(f'\n=== RaceMonitor API: race {race_id}, car {car_number} ===')

    start_epoc, end_epoc = 0, 0
    race_details = client.race.details(race_id)
    if race_details.get('Successful'):
        race = race_details['Race']
        start_epoc = race.get('StartDateEpoc', 0)
        end_epoc = race.get('EndDateEpoc', 0)
        print(f"Race name:       {race['Name']}")
        print(f"StartDateEpoc:   {epoc_to_str(start_epoc)}")
        print(f"EndDateEpoc:     {epoc_to_str(end_epoc)}")

    sessions_resp = client.results.sessions_for_race(race_id)
    sessions = sessions_resp.get('Sessions', [])
    print(f"\nSessions ({len(sessions)} total):")

    total_laps = 0
    for s in sessions:
        session_id = s['ID']
        detail = client.results.session_details(session_id, include_lap_times=True)
        session = detail['Session']
        session_start = session.get('SessionStartDateEpoc')

        car_laps = []
        for competitor in session.get('SortedCompetitors', []):
            if competitor['Number'] == car_number:
                car_laps = competitor.get('LapTimes', [])
                break

        total_laps += len(car_laps)
        print(f"  Session {session_id}: SessionStartDateEpoc={epoc_to_str(session_start)}"
              f"  car {car_number} laps={len(car_laps)}")

    print(f"\nTotal laps for car {car_number} across all sessions: {total_laps}")
    return start_epoc, end_epoc


def diagnose_influx(query_api, race_id, car_number, start_epoc=0, end_epoc=0):
    """Print stored lap count, time range, and all lap numbers from InfluxDB."""
    print(f'\n=== InfluxDB: race {race_id}, car {car_number} ===')

    range_start = (
        datetime.fromtimestamp(start_epoc, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        if start_epoc else EPOCH_START
    )
    if not end_epoc:
        print(f"  Warning: end_time_epoc not set for race {race_id}, using now() as range stop")
    range_stop = (datetime.fromtimestamp(end_epoc, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                  if end_epoc else datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))

    tables = query_api.query(
        f'from(bucket: "laps")\n'
        f'  |> range(start: {range_start}, stop: {range_stop})\n'
        f'  |> filter(fn: (r) => r._measurement == "lap"\n'
        f'      and r.race_id == "{race_id}"\n'
        f'      and r.car_number == "{car_number}"\n'
        f'      and r._field == "lap_no")\n'
        f'  |> sort(columns: ["_time"])'
    )
    records = [r for t in tables for r in t.records]
    if not records:
        print("  No laps found in InfluxDB.")
        return

    print(f"  Stored laps: {len(records)}")
    print(f"  First lap timestamp: {records[0].get_time()}")
    print(f"  Last lap timestamp:  {records[-1].get_time()}")
    print(f"  Lap numbers stored: {sorted(int(r.get_value()) for r in records)}")


def main():
    """Entry point: read args and env vars, then run both diagnose functions."""
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <race_id> <car_number>")
        sys.exit(1)

    race_id, car_number = sys.argv[1], sys.argv[2]

    rm_token = resolve_tokens()
    influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')

    if not rm_token:
        print("RACEMONITOR_TOKENS or RACEMONITOR_TOKEN not set")
        sys.exit(1)
    if not influx_token:
        print("INFLUX_TELEMETRY_TOKEN not set")
        sys.exit(1)

    with RaceMonitorClient(api_token=rm_token) as client:
        start_epoc, end_epoc = diagnose_api(client, race_id, car_number)

    with InfluxDBClient(url=INFLUX_URL, token=influx_token, org=INFLUX_ORG,
                        retries=INFLUX_RETRIES) as influx:
        diagnose_influx(influx.query_api(), race_id, car_number, start_epoc, end_epoc)


if __name__ == '__main__':
    main()
