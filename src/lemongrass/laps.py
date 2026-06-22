#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Interact with the RaceMonitor lap timing system."""
#
# Timestamp anchoring (design decision):
#   Live mode anchors lap timestamps on Race['StartDateEpoc']; the historical view anchors on
#   SessionStartDateEpoc, so the two disagree by up to ~35 min. Aligning the live anchor was
#   ruled out: the live API never exposes SessionStartDateEpoc, and the live feed's cumulative
#   offset differs from historical, so even the same anchor would not produce matching points.
#   Instead, historical is treated as the source of truth: a post-race network-mode run
#   (old_race) deletes the tracked car's lap points and rewrites the complete historical set
#   (correct timestamps + class_position on every lap) via delete_existing_laps. Live/monitor
#   writes are the during-race approximation; the backfill makes the final record authoritative.

import argparse
import csv
import enum
import logging
import os
from collections import defaultdict
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from race_monitor import RaceMonitorClient

UNDERLINE = "-" * 80

EPOCH_START = '1970-01-01T00:00:00Z'

# Version of the lap write/normalization schema. Stamped on every lap point
# written by the historical backfill (push_influx) and used by --skip-if-complete
# to decide whether a race's existing laps are current.
#
# When to bump: increment this whenever the way laps are written or normalized
# changes such that previously-written laps would now come out differently —
# e.g. a new/renamed field, a changed flag-status mapping, a timestamp-anchoring
# fix, or any other change to the lap point shape or values.
#
# Effect of bumping: the next backfill run with --skip-if-complete will treat all
# previously-written races as stale (their stamp no longer matches) and re-backfill
# them, rewriting historical data under the new schema. That "rewrite everything"
# behavior is itself a useful migration tool — bump the version and re-run the
# backfill to bring all historical races up to the current schema.
SCHEMA_VERSION = 3

_WRITE_BATCH_SIZE = 5000


class MonitorStatus(enum.Enum):
    RACE_ENDED = "race_ended"
    INTERRUPTED = "interrupted"


@dataclass
class RaceMetadata:
    """Race-level metadata resolved once at startup."""
    race_name: str
    track_name: str
    series_name: str | None
    end_time_epoc: int


@dataclass
class RaceContext:
    """Fixed context for a run: race identity, API client, and optional InfluxDB handle."""
    race_id: str
    car_number: str
    client: object
    write_api: object
    start_epoc: int
    metadata: RaceMetadata | None = None
    delete_api: object = None
    query_api: object = None


@dataclass
class RaceOptions:
    """User-configured behaviour flags, built directly from CLI args."""
    network_mode: bool = False
    monitor_mode: bool = False
    save_file: bool = False
    selected_class: str | None = None
    interval: int = 30
    skip_if_complete: bool = False
    dry_run: bool = False


def _build_parser():
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(description='Interact with lap data')
    parser.add_argument('race_id', metavar='race_id', nargs=1, type=int, action='store')
    parser.add_argument('car_number', metavar='car_number', nargs=1, type=int, action='store')
    parser.add_argument('-c', '--class', metavar='A/B/C', dest='selected_class', nargs='?',
                        type=ascii, action='store', help='Group or filter by class (A/B/C)')
    parser.add_argument('-m', '--monitor', dest='monitor_mode', default=False,
                        action='store_true', help='Update when new data received')
    parser.add_argument('-n', '--network', dest='network_mode', default=False,
                        action='store_true', help='Forward lap data via influx')
    parser.add_argument(
        '-o',
        '--out',
        dest='save_file',
        default=False,
        action='store_true',
        help='Write lap times to CSV')
    parser.add_argument('--skip-if-complete', dest='skip_if_complete', default=False,
                        action='store_true',
                        help='Skip the backfill if this car already has all its laps written '
                             'under the current schema version (historical -n mode only)')
    parser.add_argument('--dry-run', dest='dry_run', default=False,
                        action='store_true',
                        help='Implies -n; show what would be written without touching InfluxDB '
                             '(historical mode only)')
    parser.add_argument('-v', '--verbose', help="Set debug logging", action='store_true')
    parser.add_argument(
        '--interval',
        dest='interval',
        default=30,
        type=int,
        metavar='SECONDS',
        help='Polling interval in seconds for monitor mode (default: 30)')
    parser.set_defaults(monitor_mode=False, network_mode=False)
    return parser


def main():
    """Parse arguments and orchestrate race data retrieval."""
    args = _build_parser().parse_args()

    if args.verbose:
        print(args)
        logging.basicConfig(
            level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(
            level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Pandas default max rows truncating lap times. I don't expect a team to do more than 1024 laps.
    pandas.set_option("display.max_rows", 1024)

    token = os.environ.get('RACEMONITOR_TOKEN')
    if not token:
        logging.error("RACEMONITOR_TOKEN environment variable not set")
        sys.exit(1)

    influx_token = None
    if args.network_mode and not args.dry_run:
        influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
        if not influx_token:
            logging.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
            sys.exit(1)

    race_id = str(args.race_id[0])
    car_number = str(args.car_number[0])

    opts = RaceOptions(
        network_mode=args.network_mode or args.dry_run,
        monitor_mode=args.monitor_mode,
        save_file=args.save_file,
        selected_class=args.selected_class,
        interval=args.interval,
        skip_if_complete=args.skip_if_complete,
        dry_run=args.dry_run,
    )

    try:
        with RaceMonitorClient(api_token=token) as client:
            race_details = client.race.details(race_id)

            start_epoc = 0
            if race_details['Successful']:
                race_name = race_details['Race']['Name']
                start_epoc = race_details['Race']['StartDateEpoc']
                logging.debug("StartDateEpoc: %s", start_epoc)
                start_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_epoc))
                end_epoc = race_details['Race']['EndDateEpoc']
                end_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_epoc))
                race_track = race_details['Race']['Track']
                print(UNDERLINE)
                print(f"Race {race_id}")
                print(
                    f"{race_name}\tStarted: {start_date:>}\n"
                    f"{race_track}\t\t\tEnds: {end_date:>}"
                )
                print(UNDERLINE)

            metadata = _resolve_race_metadata(race_details, client) if opts.network_mode else None

            if opts.selected_class:
                logging.info("Sorting results for class %s.", opts.selected_class.upper())

            response = client.race.is_live(race_id)

            if not response['Successful']:
                return 1

            if not opts.network_mode:
                return _run_race(
                    RaceContext(race_id, car_number, client, None, start_epoc, metadata=metadata), opts, response)

            if opts.dry_run:
                return _run_race(
                    RaceContext(race_id, car_number, client, None, start_epoc, metadata=metadata), opts, response)

            with InfluxDBClient(
                url='https://influxdb.focism.com', token=influx_token, org='focism'
            ) as influx_client:
                write_api = influx_client.write_api(write_options=SYNCHRONOUS)
                delete_api = influx_client.delete_api()
                query_api = influx_client.query_api()
                return _run_race(
                    RaceContext(race_id, car_number, client, write_api, start_epoc,
                                metadata=metadata, delete_api=delete_api,
                                query_api=query_api), opts, response)
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)


def _run_race(ctx, opts, response):
    """Dispatch to live_race or old_race based on race status."""
    try:
        if response['IsLive'] is not True:
            logging.info("Race %s is not live. Monitor mode disabled.", ctx.race_id)
            if opts.monitor_mode:
                return 0
            old_race(ctx, opts)
        else:
            logging.info("Race %s is currently live.", ctx.race_id)
            live_race(ctx, opts)
        return 0
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)


def live_race(ctx, opts):
    """Called if a race ID is live."""
    session_response = ctx.client.live.get_session(ctx.race_id)

    live_session_id = None
    live_session_name = None
    if session_response.get('Successful'):
        live_session_id = session_response['Session'].get('ID')
        live_session_name = session_response['Session'].get('Name')

    print_rankings([], True, opts.selected_class, {})

    competitor_details = {}
    laps = []

    # Get lap times from live racer
    logging.debug("Getting lap times for %s from race %s.", ctx.car_number, ctx.race_id)
    response = ctx.client.live.get_racer(ctx.race_id, ctx.car_number)

    if response['Successful']:
        laps = response['Details']['Laps']
        competitor_details = response['Details']['Competitor']

    # Make name
    competitor_details['Name'] = (
        competitor_details['FirstName'] + ' ' + competitor_details['LastName'])

    competitor_name = f"{competitor_details.get('FirstName', '')} {competitor_details.get('LastName', '')}".strip() or None
    car_info = competitor_details.get('AdditionalData') or None
    class_name, class_position = _resolve_class_live(session_response, ctx.car_number)

    print(UNDERLINE)
    # Print competitor detail block
    print(
        f"Team: {competitor_details['Name']:<6} "
        f"Car Number: {competitor_details['Number']:<4} "
        f"Class: {class_name} "
        f"Transponder: {competitor_details['Transponder']}"
    )
    print(
        f"Best Position:\t{competitor_details['BestPosition']:>}\n"
        f"Final Position:\t{competitor_details['Position']:>}\n"
        f"Final Class Position:\t{class_position if class_position is not None else 'N/A':>}\n"
        f"Total Laps:\t{competitor_details['Laps']:>}\n"
        f"Best Lap:\t{competitor_details['BestLap']:>}\n"
        f"Best Lap Time:\t{competitor_details['BestLapTime']:>}\n"
        f"Total Time:\t{competitor_details['TotalTime']:>}"
    )
    print(UNDERLINE)

    # Create pandas dataframe and print without index to remove row numbers
    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))
    print(UNDERLINE)

    if opts.network_mode:
        race_ts_ms = ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
        push_influx_race(ctx, race_ts_ms)
        if live_session_id is not None:
            push_influx_session(ctx, live_session_id, live_session_name, None)
        if laps:
            # class_position intentionally discarded: historical laps were completed before
            # launch so any position we compute now is stale. monitor_routine owns
            # class_position writes. class_name was resolved above from session_response.
            logging.info("Car %s: class %r", ctx.car_number, class_name)
            push_influx(ctx, laps, False, competitor_name=competitor_name, car_info=car_info,
                        class_name=class_name, class_positions=None, session_id=live_session_id)

    if opts.save_file:
        # Create filename and call function to write to CSV
        filename = f"{competitor_details['Name']}-{ctx.race_id}"
        write_csv(filename, laps)

    if opts.monitor_mode:
        monitor_routine(ctx, laps, opts, competitor_name=competitor_name, car_info=car_info,
                        session_id=live_session_id)


def old_race(ctx, opts):
    """Called if a race ID is not live."""
    logging.debug("Getting sessions for race for %s", ctx.race_id)
    race_details = ctx.client.results.sessions_for_race(ctx.race_id)

    session_ids_for_race = [s['ID'] for s in race_details['Sessions']]

    logging.debug(
        "Race %s has %s sessions, %s",
        ctx.race_id, len(session_ids_for_race), session_ids_for_race)

    laps = []
    competitor_details = {}
    competitor_missing = True
    display_class_name = None
    pending_writes = []

    # First pass: gather every session's laps for the tracked car. We accumulate
    # the write payloads instead of writing inline so the skip check below can see
    # the complete expected lap count before any delete/write happens.
    for sid in session_ids_for_race:
        logging.debug("Getting session details for %s including lap times.", sid)
        session_details = ctx.client.results.session_details(sid, include_lap_times=True)
        sorted_competitors = [dict(c) for c in session_details['Session']['SortedCompetitors']]

        flag_map = {0: "Green", 1: "Yellow", -1: "Finish"}
        session_laps = []

        for competitor in sorted_competitors:
            if competitor['Number'] == ctx.car_number:
                competitor_missing = False
                competitor_details = competitor
                category = competitor.get('Category')
                display_class_name = (
                    session_details['Session']['Categories']
                    .get(category, {}).get('Name', category)
                )
                session_laps = competitor['LapTimes'].copy()
                laps = laps + [dict(lap) for lap in session_laps]

        if opts.network_mode:
            session_id = session_details['Session']['ID']
            session_name = session_details['Session'].get('Name', '')
            start_epoc = session_details['Session'].get('SessionStartDateEpoc')
            session_entry = {
                'session_id': session_id,
                'session_name': session_name,
                'start_epoc': start_epoc,
                'competitors': [],
            }
            for competitor in sorted_competitors:
                comp_laps = competitor.get('LapTimes', [])
                if not comp_laps:
                    continue
                comp_number = competitor['Number']
                try:
                    int(comp_number)
                except (ValueError, TypeError):
                    logging.debug("Skipping non-integer competitor number %r", comp_number)
                    continue
                comp_name = (
                    f"{competitor.get('FirstName', '')} {competitor.get('LastName', '')}".strip()
                    or None
                )
                comp_car_info = competitor.get('AdditionalData') or None
                influx_laps = [
                    {**lap, 'FlagStatus': flag_map.get(lap['FlagStatus'], str(lap['FlagStatus']))}
                    for lap in comp_laps
                ]
                class_name, class_positions = _resolve_class_historical(comp_number, session_details)
                session_entry['competitors'].append({
                    'influx_laps': influx_laps,
                    'competitor_name': comp_name,
                    'car_info': comp_car_info,
                    'class_name': class_name,
                    'class_positions': class_positions,
                    'car_number': comp_number,
                })
            pending_writes.append(session_entry)

    if competitor_missing:
        logging.info('Car %s not found', ctx.car_number)
        return

    if opts.network_mode and (not pending_writes or len(laps) == 0):
        logging.warning(
            "Validation failed: no laps collected for race %s car %s — skipping write",
            ctx.race_id, ctx.car_number)
        return

    if opts.network_mode:
        expected = len(laps)

        if opts.dry_run:
            print(UNDERLINE)
            total_laps = sum(
                len(comp['influx_laps'])
                for session in pending_writes
                for comp in session['competitors']
            )
            total_competitors = sum(len(s['competitors']) for s in pending_writes)
            for session in pending_writes:
                for comp in session['competitors']:
                    print(f"  would write {len(comp['influx_laps'])} laps for car {comp['car_number']}")
            print(f"  {total_competitors} competitor(s), {total_laps} laps total")
            print(UNDERLINE)
            return

        if opts.skip_if_complete and expected > 0:
            total, current = existing_lap_counts(ctx)
            if total == expected and current == expected:
                race_ts_ms = ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
                push_influx_race(ctx, race_ts_ms)
                logging.info(
                    "SKIP: race %s car %s already complete and current "
                    "(%d laps, schema v%d)",
                    ctx.race_id, ctx.car_number, total, SCHEMA_VERSION)
                return

        delete_existing_laps(ctx)
        total_competitors = sum(len(s['competitors']) for s in pending_writes)
        logging.info(
            "Writing %d session(s), %d competitor(s)...",
            len(pending_writes), total_competitors)

        race_ts_ms = ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
        try:
            for session in pending_writes:
                session_points = []
                for comp in session['competitors']:
                    session_points.extend(_build_lap_points(
                        ctx, comp['influx_laps'], comp['competitor_name'], comp['car_info'],
                        comp['class_name'], comp['class_positions'], session['start_epoc'],
                        comp['car_number'], session['session_id']))
                _write_points_chunked(ctx.write_api, session_points)
            logging.info("All lap data written successfully")
            push_influx_race(ctx, race_ts_ms)
        except Exception as e:
            logging.error("Writing laps failed for race %s: %s", ctx.race_id, e)
            logging.warning("Skipping race stamp so next run will re-backfill")
            return

        for session in pending_writes:
            push_influx_session(
                ctx, session['session_id'], session['session_name'], session['start_epoc'])

    print_rankings(sorted_competitors, False, opts.selected_class,
                   session_details['Session']['Categories'])

    tracked_category = competitor_details.get('Category')
    try:
        tracked_pos = int(competitor_details.get('Position', 0))
        display_class_pos = 1 + sum(
            1 for c in sorted_competitors
            if c.get('Category') == tracked_category
            and c['Number'] != ctx.car_number
            and int(c.get('Position', 0)) < tracked_pos
        )
    except (ValueError, TypeError):
        display_class_pos = None

    print(
        f"Team: {competitor_details['FirstName']:<6}\t"
        f"Car Number: {competitor_details['Number']:<4}\t"
        f"Class: {display_class_name}\t"
        f"Transponder: {competitor_details['Transponder']}"
    )
    print(
        f"Best Position:\t{competitor_details['BestPosition']:>}\n"
        f"Final Position:\t{competitor_details['Position']:>}\n"
        f"Final Class Position:\t{display_class_pos if display_class_pos is not None else 'N/A':>}\n"
        f"Total Laps:\t{competitor_details['Laps']:>}\n"
        f"Best Lap:\t{competitor_details['BestLap']:>}\n"
        f"Best Lap Time:\t{competitor_details['BestLapTime']:>}\n"
        f"Total Time:\t{competitor_details['TotalTime']:>}"
    )
    print(UNDERLINE)

    for lap in laps:
        if lap['FlagStatus'] == 1:
            lap['FlagStatus'] = "Yellow"
        elif lap['FlagStatus'] == 0:
            lap['FlagStatus'] = "Green"
        elif lap['FlagStatus'] == -1:
            lap['FlagStatus'] = "Finish"

    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))
    print(UNDERLINE)

    for competitor in sorted_competitors:
        for key, value in competitor.items():
            try:
                if key == 'Position':
                    competitor[key] = int(value)
            except ValueError:
                value = None

    if opts.save_file:
        filename = f"{competitor_details['FirstName']}-{ctx.race_id}-results"
        write_csv(filename, laps)


def print_rankings(sorted_competitors, _race_live, selected_class, categories):
    """Take a dict of sorted competitors and print them in a nice table."""
    print(UNDERLINE)
    list_of_names = []

    for competitor in sorted_competitors:
        for item in competitor:
            if item == "FirstName" and item != '':
                list_of_names.append(competitor[item])
            elif item == "LastName" and item != '':
                list_of_names.append(competitor[item])

    for competitor in sorted_competitors:
        if competitor['FirstName'] == '':
            competitor['Name'] = competitor['LastName']
        else:
            competitor['Name'] = competitor['FirstName']

    category_map = {k: categories.get(k, {}).get('Name', k) for k in
                    {c.get('Category') for c in sorted_competitors if c.get('Category')}}

    if selected_class:
        upper_class = selected_class[1].upper()
        logging.info("Current rankings for class %s.", upper_class)
        print(UNDERLINE)
        sorted_competitors_df = pandas.DataFrame(
            sorted_competitors,
            columns=['Position', 'Number', 'Name', 'Laps', 'Category', 'Transponder'])
        sorted_competitors_df = sorted_competitors_df.replace({'Category': category_map})
        sorted_competitors_df = sorted_competitors_df[
            sorted_competitors_df['Category'].str.contains(upper_class, case=False)]
        sorted_competitors_df = sorted_competitors_df.rename(
            columns={'Category': 'Class', 'Number': '#', 'Position': 'Overall Pos.'})
        sorted_competitors_df = sorted_competitors_df.sort_values(
            'Overall Pos.', key=pandas.to_numeric, ignore_index=True)
        sorted_competitors_df['Class Pos.'] = (
            sorted_competitors_df.groupby('Class').cumcount() + 1)
        sorted_competitors_df = sorted_competitors_df[
            ['Overall Pos.', '#', 'Class', 'Class Pos.', 'Name', 'Laps', 'Transponder']]
        print(sorted_competitors_df.to_string(index=False))
    else:
        logging.info("Current overall rankings.")
        print(UNDERLINE)
        sorted_competitors_df = pandas.DataFrame(
            sorted_competitors,
            columns=['Position', 'Number', 'Name', 'Laps', 'Category', 'Transponder'])
        sorted_competitors_df = sorted_competitors_df.replace({'Category': category_map})
        sorted_competitors_df = sorted_competitors_df.rename(
            columns={'Category': 'Class', 'Number': '#', 'Position': 'Pos.'})
        sorted_competitors_df = sorted_competitors_df.sort_values(
            'Pos.', key=pandas.to_numeric, ignore_index=True)
        sorted_competitors_df['Class Pos.'] = (
            sorted_competitors_df.groupby('Class').cumcount() + 1)
        sorted_competitors_df = sorted_competitors_df[
            ['Pos.', '#', 'Class', 'Class Pos.', 'Name', 'Laps', 'Transponder']]
        print(sorted_competitors_df.to_string(index=False))

    print(UNDERLINE)

    return list_of_names


def write_csv(filename, competitor_lap_times):
    """Write laptimes for a competitor to a file."""
    logging.info("Writing lap times to %s.csv", filename)
    print(UNDERLINE)
    if not competitor_lap_times:
        return
    with open(f"./{filename}.csv", 'w', encoding='utf-8', newline='') as lap_csv_fh:
        writer = csv.DictWriter(lap_csv_fh, fieldnames=competitor_lap_times[0].keys())
        writer.writeheader()
        writer.writerows(competitor_lap_times)


def monitor_routine(ctx, laps, opts, competitor_name=None, car_info=None, _stop_event=None,
                    session_id=None):
    """Monitor mode: poll for new laps and display/push as they arrive."""
    logging.info("Monitoring car %s...", ctx.car_number)
    print(UNDERLINE)

    # Create pandas dataframe and print without index to remove row numbers
    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))

    stop = _stop_event if _stop_event is not None else threading.Event()
    while not stop.wait(timeout=opts.interval):
        if opts.network_mode and ctx.start_epoc == 0:
            race_details = ctx.client.race.details(ctx.race_id)
            if race_details.get('Successful'):
                new_epoc = race_details['Race'].get('StartDateEpoc', 0)
                if new_epoc != 0:
                    ctx.start_epoc = new_epoc
                    if ctx.metadata is not None:
                        ctx.metadata.end_time_epoc = race_details['Race'].get(
                            'EndDateEpoc', ctx.metadata.end_time_epoc)
                    push_influx_race(ctx, ctx.start_epoc * 1000)

        current_competitor_lap_times = refresh_competitor(ctx)
        if not current_competitor_lap_times:
            continue

        session_response = ctx.client.live.get_session(ctx.race_id)

        for lap in current_competitor_lap_times:
            if lap not in laps:
                current_competitor_lap_time_df = pandas.json_normalize(lap)
                print(current_competitor_lap_time_df.to_string(index=False, header=False))
                laps.append(lap)
                if opts.network_mode:
                    class_name, class_position = _resolve_class_live(
                        session_response, ctx.car_number)
                    new_lap_num = int(lap['Lap'])
                    class_positions = (
                        {new_lap_num: class_position} if class_position is not None else None)
                    push_influx(
                        ctx, [lap], True,
                        competitor_name=competitor_name,
                        car_info=car_info,
                        class_name=class_name, class_positions=class_positions,
                        session_id=session_id)


def refresh_competitor(ctx):
    """Get latest lap times for a competitor from the live API."""
    logging.debug("Refreshing lap times for car %s.", ctx.car_number)
    response = ctx.client.live.get_racer(ctx.race_id, ctx.car_number)

    laps = []
    if response['Successful']:
        laps = response['Details']['Laps']

    if laps:
        logging.debug(
            "Current lap is %s with time %s.", laps[-1]['Lap'], laps[-1]['LapTime'])
    return laps


def _time_to_ms(value):
    """Parse a RaceMonitor time string to milliseconds.

    Accepts variable precision: 'H:MM:SS.mmm', 'MM:SS.mmm', or 'SS.mmm', with the
    fractional '.mmm' part optional. RaceMonitor omits the hours component when a
    value is under an hour, so we right-align the colon-separated parts.

    Returns 0 for unparseable values; the API occasionally returns garbage for
    invalid or pit laps.
    """
    try:
        parts = value.split(':')
        sec, _, ms = parts[-1].partition('.')
        hours = int(parts[-3]) if len(parts) >= 3 else 0
        minutes = int(parts[-2]) if len(parts) >= 2 else 0
        return hours * 3600000 + minutes * 60000 + int(sec) * 1000 + int(ms or 0)
    except (ValueError, AttributeError):
        logging.warning("unparseable lap time %r; writing 0 ms", value)
        return 0


def _write_points_chunked(write_api, points, batch_size=_WRITE_BATCH_SIZE):
    chunks = range(0, len(points), batch_size)
    total = len(chunks)
    for batch_num, i in enumerate(chunks, 1):
        write_api.write(bucket='laps', record=points[i:i + batch_size])
        if total > 1:
            logging.info("Batch %d of %d written successfully", batch_num, total)


def _build_lap_points(ctx, laps, competitor_name, car_info, class_name, class_positions,
                      start_epoc, car_number, session_id=None):
    """Build InfluxDB Point objects for one competitor's laps."""
    effective_epoc = start_epoc if start_epoc is not None else ctx.start_epoc
    if effective_epoc == 0:
        logging.warning("Start epoch is 0; lap timestamps will be anchored to Unix epoch")
    start_epoc_ms = effective_epoc * 1000
    points = []
    for lap in laps:
        time_lap_completed_ms = start_epoc_ms + _time_to_ms(lap['TotalTime'])
        lap_time_ms = _time_to_ms(lap['LapTime'])
        lap_num = int(lap['Lap'])
        point = (
            Point("lap")
            .tag("race_id", ctx.race_id)
            .tag("competitor_name", competitor_name)
            .tag("car_info", car_info)
            .tag("class", class_name)
            .tag("car_number", car_number)
            .field("lap_no", lap_num)
            .field("lap_time", lap_time_ms)
            .field("position", int(lap['Position']))
            .field("flag_status", lap['FlagStatus'])
            .field("schema_version", SCHEMA_VERSION)
            .time(time_lap_completed_ms, WritePrecision.MS)
        )
        if session_id is not None:
            point = point.tag("session_id", str(session_id))
        if class_positions is not None:
            class_pos = class_positions.get(lap_num)
            if class_pos is not None:
                point = point.field("class_position", class_pos)
        logging.debug(point.to_line_protocol())
        points.append(point)
    return points


def push_influx(ctx, laps, monitor_mode, competitor_name=None, car_info=None,
                class_name=None, class_positions=None, start_epoc=None,
                car_number=None, session_id=None):
    """Push lap data to InfluxDB."""
    logging.debug("Entering network mode.")
    effective_car_number = car_number if car_number is not None else ctx.car_number

    if not monitor_mode:
        logging.info("Writing laps to influx...")

    points = _build_lap_points(
        ctx, laps, competitor_name, car_info, class_name, class_positions,
        start_epoc, effective_car_number, session_id)

    if points:
        try:
            _write_points_chunked(ctx.write_api, points)
            logging.debug("Wrote %d laps to influx.", len(points))
            if not monitor_mode:
                logging.info('All lap data written successfully')
        except Exception as e:
            logging.error("Writing %d laps failed: %s", len(points), e)
            print(UNDERLINE)
            return False

    print(UNDERLINE)
    return True


def push_influx_race(ctx, timestamp_ms):
    """Write one race metadata point to the races bucket, replacing any prior point."""
    if ctx.metadata is None:
        logging.warning("push_influx_race called with no metadata for race %s", ctx.race_id)
        return
    try:
        ctx.delete_api.delete(
            start='1970-01-01T00:00:00Z',
            stop=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="race" AND race_id="{ctx.race_id}"',
            bucket='races',
        )
        meta = ctx.metadata
        point = (
            Point("race")
            .tag("race_id", ctx.race_id)
            .tag("race_name", meta.race_name)
            .tag("track_name", meta.track_name)
            .tag("series_name", meta.series_name)
            .field("end_time_epoc", meta.end_time_epoc)
            .time(timestamp_ms, WritePrecision.MS)
        )
        ctx.write_api.write(bucket='races', record=point)
    except Exception as e:
        logging.error("Writing race failed: %s", e)


def push_influx_session(ctx, session_id, session_name, start_epoc):
    """Write one session metadata point to the race_sessions bucket, replacing any prior point."""
    try:
        ctx.delete_api.delete(
            start=EPOCH_START,
            stop=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="session" AND session_id="{session_id}"',
            bucket='race_sessions',
        )
        start_epoc_ms = (start_epoc or 0) * 1000
        point = (
            Point("session")
            .tag("race_id", ctx.race_id)
            .tag("session_id", str(session_id))
            .field("session_name", session_name or "")
            .field("start_epoc", start_epoc or 0)
            .time(start_epoc_ms, WritePrecision.MS)
        )
        ctx.write_api.write(bucket='race_sessions', record=point)
    except Exception as e:
        logging.error("Writing session failed: %s", e)


def existing_lap_counts(ctx):
    """Return (total_laps, current_laps) for the tracked car's laps in this race.

    Returns the count of existing laps for the tracked car. Used as a completeness
    proxy — if the tracked car's laps are present and current, the race is considered
    complete (the full field is not verified).

    total_laps  — number of lap points written for the tracked car.
    current_laps — number of those laps stamped with the current SCHEMA_VERSION.

    A race is safe to skip only when both equal RaceMonitor's reported lap total:
    total < expected means a partial/truncated backfill, current < total means
    some laps predate the current schema (written by an older laps.py).
    """
    def _count(field_filter):
        tables = ctx.query_api.query(
            f'from(bucket: "laps")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{ctx.race_id}"\n'
            f'      and r.car_number == "{ctx.car_number}")\n'
            f'  |> filter(fn: (r) => {field_filter})\n'
            f'  |> count()'
        )
        return sum(r.get_value() for t in tables for r in t.records)

    total = _count('r._field == "lap_no"')
    current = _count(f'r._field == "schema_version" and r._value == {SCHEMA_VERSION}')
    return total, current


def delete_existing_laps(ctx):
    """Delete all lap points for this race so a backfill can replace them."""
    try:
        ctx.delete_api.delete(
            start='1970-01-01T00:00:00Z',
            stop=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="lap" AND race_id="{ctx.race_id}"',
            bucket='laps',
        )
    except Exception as e:
        logging.error("Deleting existing laps failed: %s", e)


def _resolve_class_historical(car_number, session_details):
    """Return (class_name, {lap_num: class_position}) for the given car_number."""
    session = session_details['Session']
    competitors = session['SortedCompetitors']
    categories = session['Categories']

    tracked_category = None
    tracked_laps = {}
    for competitor in competitors:
        if competitor['Number'] == car_number:
            tracked_category = competitor['Category']
            for lap in competitor['LapTimes']:
                try:
                    tracked_laps[int(lap['Lap'])] = int(lap['Position'])
                except (ValueError, TypeError):
                    pass
            break

    if tracked_category is None:
        return None, {}

    class_name = (
        categories.get(tracked_category, {})
        .get('Name', tracked_category)
    )

    class_lap_positions = defaultdict(list)
    for competitor in competitors:
        if competitor['Number'] == car_number or competitor['Category'] != tracked_category:
            continue
        for lap in competitor['LapTimes']:
            try:
                class_lap_positions[int(lap['Lap'])].append(int(lap['Position']))
            except (ValueError, TypeError):
                pass

    class_positions = {
        lap_num: 1 + sum(1 for pos in class_lap_positions.get(lap_num, []) if pos < tracked_pos)
        for lap_num, tracked_pos in tracked_laps.items()
    }

    return class_name, class_positions


def _resolve_class_live(session_response, car_number):
    """Return (class_name, class_position) for the tracked car from a live session.

    Takes the response from ``client.live.get_session`` so the caller can fetch the
    session once and reuse it.
    """
    if not session_response['Successful']:
        return None, None
    session = session_response['Session']
    classes = session['Classes']
    competitors = session['Competitors']

    tracked = None
    for competitor in competitors.values():
        if competitor['Number'] == car_number:
            tracked = competitor
            break

    if tracked is None:
        logging.warning("_resolve_class_live: car %s not found in session competitors", car_number)
        return None, None

    class_id = tracked['ClassID']
    logging.debug(
        "_resolve_class_live: car=%s overall_pos=%s class_id=%r classes=%s",
        car_number, tracked.get('Position'), class_id,
        {k: v.get('Description') for k, v in classes.items()},
    )
    class_name = classes.get(class_id, {}).get('Description', class_id)

    try:
        tracked_pos = int(tracked['Position'])
    except (ValueError, TypeError):
        return class_name, None

    class_position = 1
    for competitor in competitors.values():
        if competitor['Number'] == car_number or competitor['ClassID'] != class_id:
            continue
        try:
            comp_pos = int(competitor['Position'])
            if comp_pos < tracked_pos:
                logging.debug(
                    "_resolve_class_live: same-class car %s at pos %s counts as ahead",
                    competitor['Number'], comp_pos,
                )
                class_position += 1
        except (ValueError, TypeError):
            pass

    logging.debug(
        "_resolve_class_live: car=%s class=%r overall_pos=%s class_pos=%s",
        car_number, class_name, tracked_pos, class_position,
    )
    return class_name, class_position


def _resolve_race_metadata(race_details, client):
    """Resolve race-level metadata from race details and a single series lookup."""
    if not race_details.get('Successful'):
        return RaceMetadata(race_name='', track_name='', series_name=None, end_time_epoc=0)
    race = race_details['Race']
    series_id = race.get('SeriesID')
    series_name = None
    if series_id is not None:
        try:
            resp = client.common.current_races(series_id=series_id)
            if resp.get('Races'):
                series_name = resp['Races'][0]['SeriesName']
            else:
                resp = client.common.past_races(series_id=series_id, max_results=1)
                if resp.get('Races'):
                    series_name = resp['Races'][0]['SeriesName']
        except Exception:
            logging.warning("Failed to resolve series name for series_id=%s", series_id)
    return RaceMetadata(
        race_name=race['Name'],
        track_name=race['Track'],
        series_name=series_name,
        end_time_epoc=race.get('EndDateEpoc', 0),
    )


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)
