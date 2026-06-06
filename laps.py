#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Interact with the RaceMonitor lap timing system."""
#
# Last tested with Python 3.12 on 2026-04-05
#
# TODO:
#   When in live race mode timestamps are tagging with an offset different than the historical view.
#   If this time offset can be adjusted it would be preferable to store the data in live view
#   format over the weekend.

import argparse
import csv
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from operator import itemgetter

import pandas
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from race_monitor import RaceMonitorClient

UNDERLINE = "-" * 80


@dataclass
class RaceContext:
    """Fixed context for a run: race identity, API client, and optional InfluxDB handle."""
    race_id: str
    car_number: str
    client: object
    write_api: object
    start_epoc: int


@dataclass
class RaceOptions:
    """User-configured behaviour flags, built directly from CLI args."""
    network_mode: bool = False
    monitor_mode: bool = False
    save_file: bool = False
    selected_class: str | None = None
    interval: int = 30


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


def main():  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
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
        sys.exit()

    influx_token = None
    if args.network_mode:
        influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
        if not influx_token:
            logging.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
            sys.exit()

    race_id = str(args.race_id[0])
    car_number = str(args.car_number[0])

    opts = RaceOptions(
        network_mode=args.network_mode,
        monitor_mode=args.monitor_mode,
        save_file=args.save_file,
        selected_class=args.selected_class,
        interval=args.interval,
    )

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

        if opts.selected_class:
            logging.info("Sorting results for class %s.", opts.selected_class.upper())

        response = client.race.is_live(race_id)

        if not response['Successful']:
            return 1

        if not opts.network_mode:
            return _run_race(
                RaceContext(race_id, car_number, client, None, start_epoc), opts, response)

        with InfluxDBClient(
            url='https://influxdb.focism.com', token=influx_token, org='focism'
        ) as influx_client:
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
            return _run_race(
                RaceContext(race_id, car_number, client, write_api, start_epoc), opts, response)


def _run_race(ctx, opts, response):
    """Dispatch to live_race or old_race based on race status."""
    if response['IsLive'] is not True:
        logging.info("Race %s is not live. Monitor mode disabled.", ctx.race_id)
        if opts.monitor_mode:
            return 0
        old_race(ctx, opts)
    else:
        logging.info("Race %s is currently live.", ctx.race_id)
        live_race(ctx, opts)
    return 0


def live_race(ctx, opts):
    """Called if a race ID is live."""
    ctx.client.live.get_session(ctx.race_id)

    list_of_competitors = []

    # for competitor in competitors:
    #    list_of_competitors.append(competitors[competitor])

    for competitor in list_of_competitors:
        for key, value in competitor.items():
            try:
                if key == 'Position':
                    competitor[key] = int(value)
            except ValueError:
                value = None

    # Remove competitors (LOSERS) with no position
    list_of_competitors = [racer for racer in list_of_competitors if racer['Number'] != '']

    for item in list_of_competitors:
        print(item)
        if item['Position'] == '':
            print("Dirty data")
            list_of_competitors.remove(item)
            break

    sorted_competitors = sorted(
        list_of_competitors, key=lambda k: int(itemgetter('Position')(k)))

    print_rankings(sorted_competitors, True, opts.selected_class)

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
        competitor_details['FirstName'] + competitor_details['LastName'])

    print(UNDERLINE)
    # Print competitor detail block
    print(
        f"Team: {competitor_details['Name']:<6} "
        f"Car Number: {competitor_details['Number']:<4} "
        f"Transponder: {competitor_details['Transponder']}"
    )
    print(
        f"Best Position:\t{competitor_details['BestPosition']:>}\n"
        f"Final Position:\t{competitor_details['Position']:>}\n"
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
        class_name, class_position = _resolve_class_live(ctx.client, ctx.race_id, ctx.car_number)
        class_positions = (
            {int(laps[-1]['Lap']): class_position}
            if class_position is not None else None
        )
        push_influx(ctx, laps, False, class_name=class_name, class_positions=class_positions)

    if opts.save_file:
        # Create filename and call function to write to CSV
        filename = f"{competitor_details['Name']}-{ctx.race_id}"
        write_csv(filename, laps)

    if opts.monitor_mode:
        monitor_routine(ctx, laps, opts)


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

    for session_id in session_ids_for_race:
        logging.debug("Getting session details for %s including lap times.", session_id)
        session_details = ctx.client.results.session_details(session_id, include_lap_times=True)
        sorted_competitors = [dict(c) for c in session_details['Session']['SortedCompetitors']]

        session_laps = []
        for competitor in sorted_competitors:
            if competitor['Number'] == ctx.car_number:
                competitor_missing = False
                competitor_details = competitor
                session_laps = competitor['LapTimes'].copy()
                laps = laps + [dict(lap) for lap in session_laps]

        if opts.network_mode and session_laps:
            flag_map = {0: "Green", 1: "Yellow", -1: "Finish"}
            influx_laps = [
                {**lap, 'FlagStatus': flag_map.get(lap['FlagStatus'], str(lap['FlagStatus']))}
                for lap in session_laps
            ]
            class_name, class_positions = _resolve_class_historical(ctx.car_number, session_details)
            push_influx(
                ctx, influx_laps, False,
                class_name=class_name, class_positions=class_positions)

    if competitor_missing:
        logging.info('Car %s not found', ctx.car_number)
        return

    print_rankings(sorted_competitors, False, opts.selected_class)

    print(
        f"Team: {competitor_details['FirstName']:<6}\t"
        f"Car Number: {competitor_details['Number']:<4}\t"
        f"Transponder: {competitor_details['Transponder']}"
    )
    print(
        f"Best Position:\t{competitor_details['BestPosition']:>}\n"
        f"Final Position:\t{competitor_details['Position']:>}\n"
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


def print_rankings(sorted_competitors, _race_live, selected_class):
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

    if selected_class:
        upper_class = selected_class[1].upper()
        logging.info("Current rankings for class %s.", upper_class)
        print(UNDERLINE)
        sorted_competitors_df = pandas.DataFrame(
            sorted_competitors,
            columns=['Position', 'Number', 'Name', 'Laps', 'Category', 'Transponder'])
        sorted_competitors_df = sorted_competitors_df.replace(
            {'Category': {'1': 'A', '2': 'DNQ', '3': 'B', '4': 'C'}})
        sorted_competitors_df = sorted_competitors_df[
            sorted_competitors_df['Category'].str.contains(upper_class)]
        sorted_competitors_df = sorted_competitors_df.rename(
            columns={'Category': 'Class', 'Number': '#', 'Position': 'Overall Pos.'})
        sorted_competitors_df = sorted_competitors_df.reset_index(drop=True)
        sorted_competitors_df.index += 1
        print(sorted_competitors_df.to_string(index=True))
    else:
        logging.info("Current overall rankings.")
        print(UNDERLINE)
        sorted_competitors_df = pandas.DataFrame(
            sorted_competitors,
            columns=['Position', 'Number', 'Name', 'Laps', 'Transponder'])
        sorted_competitors_df = sorted_competitors_df.rename(
            columns={'Number': '#', 'Position': 'Pos.'})
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


def monitor_routine(ctx, laps, opts, _stop_event=None):
    """Monitor mode: poll for new laps and display/push as they arrive."""
    logging.info("Monitoring car %s...", ctx.car_number)
    print(UNDERLINE)

    # Create pandas dataframe and print without index to remove row numbers
    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))

    stop = _stop_event if _stop_event is not None else threading.Event()
    while not stop.wait(timeout=opts.interval):
        current_competitor_lap_times = refresh_competitor(ctx)
        if current_competitor_lap_times[-1] not in laps:
            current_competitor_lap_time_df = pandas.json_normalize(
                current_competitor_lap_times[-1])
            print(current_competitor_lap_time_df.to_string(index=False, header=False))
            laps.append(current_competitor_lap_times[-1])
            if opts.network_mode:
                class_name, class_position = _resolve_class_live(
                    ctx.client, ctx.race_id, ctx.car_number)
                new_lap_num = int(current_competitor_lap_times[-1]['Lap'])
                class_positions = (
                    {new_lap_num: class_position} if class_position is not None else None)
                push_influx(
                    ctx, [current_competitor_lap_times[-1]], True,
                    class_name=class_name, class_positions=class_positions)


def refresh_competitor(ctx):
    """Get latest lap times for a competitor from the live API."""
    logging.debug("Refreshing lap times for car %s.", ctx.car_number)
    response = ctx.client.live.get_racer(ctx.race_id, ctx.car_number)

    laps = []
    if response['Successful']:
        laps = response['Details']['Laps']

    logging.debug(
        "Current lap is %s with time %s.", laps[-1]['Lap'], laps[-1]['LapTime'])
    return laps


def push_influx(ctx, laps, monitor_mode, class_name=None, class_positions=None):
    """Push lap data to InfluxDB."""
    logging.debug("Entering network mode.")
    logging.debug("Start epoch in seconds: %s", ctx.start_epoc)
    start_epoc_ms = ctx.start_epoc * 1000
    logging.debug("Start epoch in milliseconds: %s", start_epoc_ms)

    if not monitor_mode:
        logging.info("Writing laps to influx...")

    current_driver = "Driver" + str(ctx.car_number)
    tag_str = (
        f"class={class_name},driver={current_driver}"
        if class_name else f"driver={current_driver}"
    )

    write_success = True
    for lap in laps:
        h, m, s = lap['TotalTime'].split(':')
        s, ms = s.split('.')
        lap_finish_ms = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)
        time_lap_completed_ms = start_epoc_ms + lap_finish_ms
        lap_timestamp = str(time_lap_completed_ms).replace(".", '')

        h, m, s = lap['LapTime'].split(':')
        s, ms = s.split('.')
        lap_time_ms = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)

        lap_num = int(lap['Lap'])
        field_str = (
            f"lap_no={lap_num},lap_time={lap_time_ms},"
            f"position={lap['Position']},flag_status=\"{lap['FlagStatus']}\""
        )
        if class_positions is not None:
            class_pos = class_positions.get(lap_num)
            if class_pos is not None:
                field_str += f",class_position={class_pos}"

        data = [f"laps{ctx.race_id},{tag_str} {field_str} {lap_timestamp}"]
        logging.debug(data)
        try:
            ctx.write_api.write(bucket='laps_252/autogen', record=data, write_precision='ms')
            logging.debug("Lap %s written to influx.", lap['Lap'])
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error("Writing lap failed: %s", e)
            write_success = False

    if write_success and not monitor_mode:
        logging.info('All lap data written successfully')

    print(UNDERLINE)


def _resolve_class_historical(car_number, session_details):
    """Return (class_name, {lap_num: class_position}) for the tracked car."""
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
        .replace(' ', '_').replace(',', '').replace('=', '')
    )

    class_lap_positions = {}
    for competitor in competitors:
        if competitor['Number'] == car_number or competitor['Category'] != tracked_category:
            continue
        for lap in competitor['LapTimes']:
            try:
                lap_num = int(lap['Lap'])
                if lap_num not in class_lap_positions:
                    class_lap_positions[lap_num] = []
                class_lap_positions[lap_num].append(int(lap['Position']))
            except (ValueError, TypeError):
                pass

    class_positions = {
        lap_num: 1 + sum(1 for pos in class_lap_positions.get(lap_num, []) if pos < tracked_pos)
        for lap_num, tracked_pos in tracked_laps.items()
    }

    return class_name, class_positions


def _resolve_class_live(client, race_id, car_number):
    """Return (class_name, class_position) for the tracked car using current session state."""
    response = client.live.get_session(race_id)
    if not response['Successful']:
        return None, None
    session = response['Session']
    classes = session['Classes']
    competitors = session['Competitors']

    tracked = None
    for competitor in competitors.values():
        if competitor['Number'] == car_number:
            tracked = competitor
            break

    if tracked is None:
        return None, None

    class_id = tracked['ClassID']
    class_name = (
        classes.get(class_id, {}).get('Description', class_id)
        .replace(' ', '_').replace(',', '').replace('=', '')
    )

    try:
        tracked_pos = int(tracked['Position'])
    except (ValueError, TypeError):
        return class_name, None

    class_position = 1
    for competitor in competitors.values():
        if competitor['Number'] == car_number or competitor['ClassID'] != class_id:
            continue
        try:
            if int(competitor['Position']) < tracked_pos:
                class_position += 1
        except (ValueError, TypeError):
            pass

    return class_name, class_position


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(0)
