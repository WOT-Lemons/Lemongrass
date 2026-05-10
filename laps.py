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
import time
from operator import itemgetter

import pandas
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from race_monitor import RaceMonitorClient

UNDERLINE = "-" * 80


def main():  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
    """Parse arguments and orchestrate race data retrieval."""
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
    parser.set_defaults(monitor_mode=False, network_mode=False)
    args = parser.parse_args()

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
    selected_class = args.selected_class

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

        if selected_class:
            logging.info("Sorting results for class %s.", selected_class.upper())

        response = client.race.is_live(race_id)

        if not response['Successful']:
            return 1

        if args.network_mode:
            with InfluxDBClient(
                url='https://influxdb.focism.com', token=influx_token, org='focism'
            ) as influx_client:
                write_api = influx_client.write_api(write_options=SYNCHRONOUS)
                return _run_race(race_id, car_number, client, args, write_api, start_epoc, response)
        return _run_race(race_id, car_number, client, args, None, start_epoc, response)


def _run_race(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        race_id, car_number, client, args, write_api, start_epoc, response):
    """Dispatch to live_race or old_race based on race status."""
    if response['IsLive'] is not True:
        logging.info("Race %s is not live. Monitor mode disabled.", race_id)
        if args.monitor_mode:
            return 0
        old_race(race_id, car_number, client, args.network_mode,
                 start_epoc, write_api, args.save_file, args.selected_class)
    else:
        logging.info("Race %s is currently live.", race_id)
        live_race(
            race_id, car_number, client, args.network_mode, args.monitor_mode,
            write_api, start_epoc, args.save_file, args.selected_class)
    return 0


def live_race(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        race_id, car_number, client, network_mode, monitor_mode,
        write_api, start_epoc, save_file, selected_class):
    """Called if a race ID is live."""
    client.live.get_session(race_id)

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

    print_rankings(sorted_competitors, True, selected_class)

    racer_id = car_number

    competitor_details = {}
    laps = []

    # Get lap times from live racer
    logging.debug("Getting lap times for %s from race %s.", racer_id, race_id)
    response = client.live.get_racer(race_id, racer_id)

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

    if network_mode:
        push_influx(racer_id, laps, write_api, start_epoc, race_id, False, car_number)

    if save_file:
        # Create filename and call function to write to CSV
        filename = f"{competitor_details['Name']}-{race_id}"
        write_csv(filename, laps)

    if monitor_mode:
        monitor_routine(
            car_number, laps, race_id, racer_id, write_api, start_epoc, client, network_mode)


def old_race(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches  # noqa: E501
        race_id, car_number, client, network_mode, start_epoc, write_api, save_file, selected_class):
    """Called if a race ID is not live."""
    logging.debug("Getting sessions for race for %s", race_id)
    race_details = client.results.sessions_for_race(race_id)

    session_ids_for_race = []

    # Get only session IDs in session_ids_for_race
    for i in race_details['Sessions']:
        session_ids_for_race.append(i['ID'])

    logging.debug(
        "Race %s has %s sessions, %s",
        race_id, len(session_ids_for_race), session_ids_for_race)

    laps = []
    competitor_details = {}
    competitor_missing = True

    # Send request for all session_ids from a race, including lap times
    for session_id in session_ids_for_race:
        logging.debug("Getting session details for %s including lap times.", session_id)
        session_details = client.results.session_details(session_id, include_lap_times=True)
        sorted_competitors = session_details['Session']['SortedCompetitors'].copy()

        for competitor in sorted_competitors:
            if competitor['Number'] == car_number:
                competitor_missing = False
                competitor_details = competitor
                laps = laps + competitor['LapTimes'].copy()

    if competitor_missing:
        logging.info('Car %s not found', car_number)
        return

    print_rankings(sorted_competitors, False, selected_class)

    # Print competitor detail block
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

    # Create pandas dataframe and print without index to remove row numbers
    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))
    print(UNDERLINE)

    # Remove competitors (LOSERS) with no position
    for competitor in sorted_competitors:
        for key, value in competitor.items():
            try:
                if key == 'Position':
                    competitor[key] = int(value)
            except ValueError:
                value = None

    if network_mode:
        push_influx(car_number, laps, write_api, start_epoc, race_id, False, car_number)

    if save_file:
        # Create filename and call function to write to CSV
        filename = f"{competitor_details['Name']}-{race_id}-results"
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
    with open(f"./{filename}.csv", 'w', encoding='utf-8') as lap_csv_fh:
        csvwriter = csv.writer(lap_csv_fh)
        count = 0
        for lap in competitor_lap_times:
            if count == 0:
                header = lap.keys()
                csvwriter.writerow(header)
                count += 1
            csvwriter.writerow(lap.values())


def monitor_routine(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        car_number, laps, race_id, racer_id, write_api, start_epoc, client, network_mode):
    """Monitor mode: poll for new laps and display/push as they arrive."""
    logging.info("Monitoring car %s...", car_number)
    print(UNDERLINE)

    # Create pandas dataframe and print without index to remove row numbers
    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))

    while True:
        time.sleep(30)
        current_competitor_lap_times = refresh_competitor(race_id, racer_id, client)
        if current_competitor_lap_times[-1] not in laps:
            current_competitor_lap_time_df = pandas.json_normalize(
                current_competitor_lap_times[-1])
            print(current_competitor_lap_time_df.to_string(index=False, header=False))
            laps.append(current_competitor_lap_times[-1])
            if network_mode:
                push_influx(
                    racer_id, current_competitor_lap_times, write_api,
                    start_epoc, race_id, True, car_number)


def refresh_competitor(race_id, racer_id, client):
    """Get latest lap times for a competitor from the live API."""
    laps = []

    logging.debug("Refreshing lap times for car %s.", racer_id)
    response = client.live.get_racer(race_id, racer_id)

    if response['Successful']:
        laps = response['Details']['Laps']

    logging.debug(
        "Current lap is %s with time %s.", laps[-1]['Lap'], laps[-1]['LapTime'])
    response = []
    return laps


def push_influx(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        _racer_id, laps, write_api, start_epoc, race_id, monitor_mode, car_number):
    """Push lap data to InfluxDB."""
    logging.debug("Entering network mode.")
    logging.debug("Start epoch in seconds: %s", start_epoc)
    start_epoc = start_epoc * 1000
    logging.debug("Start epoch in milliseconds: %s", start_epoc)

    if not monitor_mode:
        logging.info("Writing laps to influx...")

    current_driver = "Driver" + str(car_number)

    # TODO: Concat driver from args
    write_success = True
    for lap in laps:
        # Convert HH:MM:SS.MS to get time lap completed
        h, m, s = lap['TotalTime'].split(':')
        s, ms = s.split('.')
        lap_finish_ms = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)
        time_lap_completed_ms = start_epoc + lap_finish_ms
        lap_timestamp = str(time_lap_completed_ms).replace(".", '')

        # Convert lap time to milliseconds
        h, m, s = lap['LapTime'].split(':')
        s, ms = s.split('.')
        lap_time_ms = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)

        data = []
        data.append(
            f"laps{race_id},driver={current_driver} "
            f"lap_no={lap['Lap']},lap_time={lap_time_ms},"
            f"position={lap['Position']},flag_status=\"{lap['FlagStatus']}\" "
            f"{lap_timestamp}"
        )
        logging.debug(data)
        try:
            write_api.write(bucket='laps_252/autogen', record=data, write_precision='ms')
            logging.debug("Lap %s written to influx.", lap['Lap'])
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error("Writing lap failed: %s", e)
            write_success = False

    if write_success and not monitor_mode:
        logging.info('All lap data written successfully')

    print(UNDERLINE)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(0)
