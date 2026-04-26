#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Last tested with Python 3.12 on 2026-04-05
#
# Laps.py
# Interact with the RaceMonitor lap timing system
# TODO:
#   When in live race mode timestamps are tagging with an offset different than the historical view.
#   If this time offset can be adjusted it would be preferable to store the data in live view format over the weekend.

from __future__ import print_function, unicode_literals
from operator import itemgetter
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import sys
import requests
import json
import time
import csv
import pandas
import logging
import argparse

underline = "-" * 80


def main():

  parser = argparse.ArgumentParser(description='Interact with lap data')
  parser.add_argument('race_id', metavar='race_id', nargs=1, type=int, action='store')
  parser.add_argument('car_number', metavar='car_number', nargs=1, type=int, action='store')
  parser.add_argument('-c', '--class', metavar='A/B/C', dest='selected_class', nargs='?',
                      type=ascii, action='store', help='Group or filter by class (A/B/C)')
  parser.add_argument('-m', '--monitor', dest='monitor_mode', default=False,
                      action='store_true', help='Update when new data received')
  parser.add_argument('-n', '--network', dest='network_mode', default=False,
                      action='store_true', help='Forward lap data via influx')
  parser.add_argument('-o', '--out', dest='save_file', default=False, action='store_true', help='Write lap times to CSV')
  parser.add_argument('-v', '--verbose', help="Set debug logging", action='store_true')
  parser.set_defaults(monitor_mode=False, network_mode=False)
  args = parser.parse_args()

  if args.verbose:
    print(args)
    # Set logging level - https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
  else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

  payload = {'apiToken': token, 'raceID': race_id}
  race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

  if race_details['Successful']:
    race_name = race_details['Race']['Name']
    start_epoc = race_details['Race']['StartDateEpoc']
    logging.debug("StartDateEpoc: {}".format(start_epoc))
    start_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_epoc))
    end_epoc = race_details['Race']['EndDateEpoc']
    end_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_epoc))
    race_track = race_details['Race']['Track']
    print(underline)
    print("Race {}".format(race_id))
    print("{}\tStarted: {:>}\n{}\t\t\tEnds: {:>}".format(race_name, start_date, race_track, end_date))
    print(underline)

  # Check if race is live
  payload = {'apiToken': token, 'RaceID': race_id}
  response = callRaceMonitor('/v2/Race/IsLive', payload)

  if selected_class:
    logging.info("Sorting results for class {}.".format(selected_class.upper()))

  if args.network_mode:
    with InfluxDBClient(url='https://influxdb.focism.com', token=influx_token, org='focism') as influx_client:
      write_api = influx_client.write_api(write_options=SYNCHRONOUS)
      if response['Successful']:
        if response['IsLive'] is not True:
          logging.info("Race {} is not live. Monitor mode disabled.".format(race_id))
          if args.monitor_mode:
            return
          else:
            oldRace(race_id, car_number, token, args.network_mode, start_epoc, write_api, args.save_file, selected_class)
        else:
          logging.info("Race {} is currently live.".format(race_id))
          liveRace(race_id, car_number, token, args.network_mode, args.monitor_mode, write_api, start_epoc, args.save_file, selected_class)
  else:
    write_api = None
    if response['Successful']:
      if response['IsLive'] is not True:
        logging.info("Race {} is not live. Monitor mode disabled.".format(race_id))
        if args.monitor_mode:
          return
        else:
          oldRace(race_id, car_number, token, args.network_mode, start_epoc, write_api, args.save_file, selected_class)
      else:
        logging.info("Race {} is currently live.".format(race_id))
        liveRace(race_id, car_number, token, args.network_mode, args.monitor_mode, write_api, start_epoc, args.save_file, selected_class)

  return 0


def liveRace(race_id, car_number, token, network_mode, monitor_mode, write_api, start_epoc, save_file, selected_class):
  """
  Function name: liveRace
  Arguments: race_id, token
  Description: Called if a race ID is live.
  """
  payload = {'apiToken': token, 'raceID': race_id}
  last_session_details = callRaceMonitor('/v2/Live/GetSession', payload)

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

  # Remove competitors (LOSERS)  with no position
  list_of_competitors = [racer for racer in list_of_competitors if racer['Number'] != '']
  # print(list_of_competitors)

  for i in range(len(list_of_competitors)):
    print(list_of_competitors[i])
    if list_of_competitors[i]['Position'] == '':
      print("Dirty data")
      list_of_competitors.remove(list_of_competitors[i])
      break

  sorted_competitors = sorted(list_of_competitors, key=lambda k: int(itemgetter('Position')(k)))

  printRankings(sorted_competitors, True, selected_class)

  # if session['ID']== session_ids_for_race[-1]:
  #    competitors = session

  racer_id = car_number
  # logging.debug("Racer ID: {}".format(racer_id))

  competitor_details = []
  competitor_lap_times = []

  # Get lap times from live racer
  logging.debug("Getting lap times for {} from race {}.".format(racer_id, race_id))
  payload = {'apiToken': token, 'RacerID': racer_id, 'RaceID': race_id}
  response = callRaceMonitor('/v2/Live/GetRacer', payload)

  if response['Successful']:
    laps = response['Details']['Laps']
    competitor_details = response['Details']['Competitor']

  # Make name
  competitor_details['Name'] = competitor_details['FirstName'] + competitor_details['LastName']

  print(underline)
  # Print competitor detail block
  print("Team: {:<6} Car Number: {:<4} Transponder: {}".format(
      competitor_details['Name'], competitor_details['Number'], competitor_details['Transponder']))
  print("Best Position:\t{:>}\nFinal Position:\t{:>}\nTotal Laps:\t{:>}\nBest Lap:\t{:>}\nBest Lap Time:\t{:>}\nTotal Time:\t{:>}".format(
      competitor_details['BestPosition'], competitor_details['Position'], competitor_details['Laps'], competitor_details['BestLap'], competitor_details['BestLapTime'], competitor_details['TotalTime']))
  print(underline)

  # Create pandas dataframe and print without index to remove row numbers
  lap_time_df = pandas.json_normalize(laps)
  print(lap_time_df.to_string(index=False))
  print(underline)

  # If we're going to be starting network mode, check for presence of existing data.
  if network_mode:
    network_status = pushInflux(racer_id, laps, write_api, start_epoc, race_id, False, car_number)

  if save_file:
    # Create filename and call function to write to CSV
    filename = "{}-{}".format(competitor_details['Name'], race_id)
    writeCSV(filename, laps)

  if monitor_mode:
    # Enter monitoring loop
    competitor_last_lap = laps[-1]['LapTime']
    monitorRoutine(car_number, laps, race_id, racer_id, write_api, start_epoc, token, network_mode)

  return


def oldRace(race_id, car_number, token, network_mode, start_epoc, write_api, save_file, selected_class):
  """
  Function name: oldRace
  Arguments: race_id, token
  Description: Called if a race ID is not live.
  """

  logging.debug("Getting sessions for race for {}".format(race_id))
  payload = {'apiToken': token, 'raceID': race_id}
  race_details = callRaceMonitor('/v2/Results/SessionsForRace', payload)

  session_ids_for_race = []
  competitor_lap_times = []

  # Get only session IDs in session_ids_for_race
  for i in race_details['Sessions']:
    session_ids_for_race.append(i['ID'])

  logging.debug("Race {} has {} sessions, {}".format(race_id, len(session_ids_for_race), session_ids_for_race))

  laps = []
  competitor_missing = True

  # Send request for all session_ids from a race, including lap times
  for session_id in session_ids_for_race:
    logging.debug("Getting session details for {} including lap times.".format(session_id))
    payload = {'apiToken': token, 'sessionID': session_id, 'includeLapTimes': True}
    # payload = { 'apiToken': token, 'sessionID': session_id, 'includeLapTimes': False}
    session_details = callRaceMonitor('/v2/Results/SessionDetails', payload)
    sorted_competitors = session_details['Session']['SortedCompetitors'].copy()

    for competitor in sorted_competitors:
      # print(competitor['Number'])
      if competitor['Number'] == car_number:
        competitor_missing = False
        competitor_details = competitor
        laps = laps + competitor['LapTimes'].copy()

  if competitor_missing:
    logging.info('Car {} not found'.format(car_number))
    return 1

  # lap_times = session_details['Session']['SortedCompetitors']

  printRankings(sorted_competitors, False, selected_class)

  # Print competitor detail block
  print("Team: {:<6}\tCar Number: {:<4}\tTransponder: {}".format(
      competitor_details['FirstName'], competitor_details['Number'], competitor_details['Transponder']))
  print("Best Position:\t{:>}\nFinal Position:\t{:>}\nTotal Laps:\t{:>}\nBest Lap:\t{:>}\nBest Lap Time:\t{:>}\nTotal Time:\t{:>}".format(
      competitor_details['BestPosition'], competitor_details['Position'], competitor_details['Laps'], competitor_details['BestLap'], competitor_details['BestLapTime'], competitor_details['TotalTime']))
  print(underline)

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
  print(underline)

  # Remove competitors (LOSERS)  with no position
  for competitor in sorted_competitors:
    for key, value in competitor.items():
      # print(competitor.keys())
      try:
        if key == 'Position':
          # print(value)
          competitor[key] = int(value)
      except ValueError:
        value = None

  # pprint(lap_times)

  # If we're going to be starting network mode, check for presence of existing data.
  if network_mode:
    network_status = pushInflux(car_number, laps, write_api, start_epoc, race_id, False, car_number)

  if save_file:
    # Create filename and call function to write to CSV
    filename = "{}-{}-results".format(competitor_details['Name'], race_id)
    writeCSV(filename, laps)

  return


def callRaceMonitor(endpoint, payload):
  """
  Function name: callRaceMonitor
  Arguments: endpoint, payload
  Description: Take a race monitor API endoint (https://www.race-monitor.com/APIDocs)
  and payload, and request the object from the API. Handles rate limiting with sleep.
  """
  api_base_url = 'https://api.race-monitor.com'
  api_endpoint = endpoint
  api_url = api_base_url + api_endpoint
  r = requests.post(api_url, data=payload)

  while r.status_code == 429:
    logging.error('{} - Too many requests, waiting 10 seconds...'.format(r.status_code))
    time.sleep(10)
    r = requests.post(api_url, data=payload)

  if r.status_code == 200:
    return json.loads(r.text)
  else:
    logging.error('Error {}'.format(r.status_code))
  return


def printRankings(sorted_competitors, race_live, selected_class):
  """
  Function name: printRankings
  Arguments: sorted_competitors
  Description: Take a dict of sorted competitors and print them
  in a nice table. Might be worth re-writing this with Pandas
  """
  global underline
  print(underline)
  list_of_names = []

  for competitor in sorted_competitors:
    # print(competitor)
    for item in competitor:
      # print(item)
      if item == "FirstName" and item != '':
        list_of_names.append(competitor[item])
      elif item == "LastName" and item != '':
        list_of_names.append(competitor[item])

  for competitor in sorted_competitors:
    if competitor['FirstName'] == '':
      competitor['Name'] = competitor['LastName']
    else:
      competitor['Name'] = competitor['FirstName']

  # Is class/category mode set? If so create a pandas dataframe accordingly.
  if selected_class:
    upper_class = selected_class[1].upper()
    logging.info("Current rankings for class {}.".format(upper_class))
    print(underline)
    sorted_competitors_df = pandas.DataFrame(
        sorted_competitors, columns=['Position', 'Number', 'Name', 'Laps', 'Category', 'Transponder'])
    sorted_competitors_df = sorted_competitors_df.replace({'Category': {'1': 'A', '2': 'DNQ', '3': 'B', '4': 'C'}})
    sorted_competitors_df = sorted_competitors_df[sorted_competitors_df['Category'].str.contains(upper_class)]
    sorted_competitors_df = sorted_competitors_df.rename(columns={'Category': 'Class', 'Number': '#', 'Position': 'Overall Pos.'})
    sorted_competitors_df = sorted_competitors_df.reset_index(drop=True)
    sorted_competitors_df.index += 1
    print(sorted_competitors_df.to_string(index=True))
  else:
    logging.info("Current overall rankings.")
    print(underline)
    sorted_competitors_df = pandas.DataFrame(
        sorted_competitors, columns=['Position', 'Number', 'Name', 'Laps', 'Transponder'])
    sorted_competitors_df = sorted_competitors_df.rename(columns={'Number': '#', 'Position': 'Pos.'})
    print(sorted_competitors_df.to_string(index=False))

  print(underline)

  return list_of_names


def writeCSV(filename, competitor_lap_times):
  """
  Function name: writeCSV
  Arguments: filename, competitor_lap_times
  Description: Write laptimes for a competitor to a file.
  """
  logging.info('Writing lap times to {}.csv'.format(filename))
  print(underline)
  lap_csv_fh = open("./%s.csv" % filename, 'w')
  csvwriter = csv.writer(lap_csv_fh)

  count = 0
  for lap in competitor_lap_times:
    if count == 0:
      header = lap.keys()
      csvwriter.writerow(header)
      count += 1

    csvwriter.writerow(lap.values())

  lap_csv_fh.close()
  return


def monitorRoutine(car_number, laps, race_id, racer_id, write_api, start_epoc, token, network_mode):
  """
  Function name: monitorRoutine
  Arguments: car_number, last_lap_time
  Description: Destination routine for monitor mode.
               Holds about the time of a lap and then checks
               to see if there's a new one. If there is none,
               hold until there is and then print a lap line. Repeat
  """

  logging.info("Monitoring car {}...".format(car_number))
  print(underline)

  # Create pandas dataframe and print without index to remove row numbers
  lap_time_df = pandas.json_normalize(laps)
  print(lap_time_df.to_string(index=False))

  # logging.debug("\nLast lap: {}\nLast lap: {} seconds\nSleep Time: {} seconds".format(last_lap_time, last_lap_seconds, sleep_interval))

  while True:
    time.sleep(30)
    current_competitor_lap_times = refreshCompetitor(race_id, racer_id, token)
    # if current_competitor_lap_times[-1] == laps[-1]:
    #    pass
    # else:
    if current_competitor_lap_times[-1] not in laps:
      current_competitor_lap_time_df = pandas.json_normalize(current_competitor_lap_times[-1])
      print(current_competitor_lap_time_df.to_string(index=False, header=False))
      # print(current_competitor_lap_times[-1])
      laps.append(current_competitor_lap_times[-1])
      if network_mode:
        pushInflux(racer_id, current_competitor_lap_times, write_api, start_epoc, race_id, True, car_number)
  return


def refreshCompetitor(race_id, racer_id, token):
  """
  Function name: refreshCompetitor
  Arguments: car_number, last_lap_time
  Description: If this is a new competitor, get all laps from all sessions.
               If not, check only for the last lap from the last session.
  """

  laps = []
  competitor_details = []

  logging.debug("Refreshing lap times for car {}.".format(racer_id))
  payload = {'apiToken': token, 'RaceID': race_id, 'RacerID': racer_id}
  response = callRaceMonitor('/v2/Live/GetRacer', payload)

  # try:
  if response['Successful']:
    laps = response['Details']['Laps']
    competitor_details = response['Details']['Competitor']
  # except TypeError: return

  logging.debug("Current lap is {} with time {}.".format(laps[-1]['Lap'], laps[-1]['LapTime']))
  response = []
  return laps


def pushInflux(racer_id, laps, write_api, start_epoc, race_id, monitor_mode, car_number):
  """
  Function name: pushInflux
  Arguments: laps
  Description: This function takes data and pushes it into influx.
               Accepts a list of lap dicts and attempts to match
               existing data. If there's new data, add it.
  """
  logging.debug("Entering network mode.")
  logging.debug('Start epoch in seconds: {}'.format(start_epoc))
  start_epoc = start_epoc * 1000
  logging.debug("Start epoch in milliseconds: {}".format(start_epoc))

  '''
    output = subprocess.check_output(['dig', '@8.8.8.8', '+short', 'TXT', 'driver.wotlemons.com'])
    dirty_driver = output.decode("utf-8")
    current_driver = dirty_driver.replace('"', '')
    current_driver = current_driver.rstrip()
    logging.info("Current driver: {}".format(current_driver))
    '''
  # logging.debug("Driver: {}".format(car_number))

  if not monitor_mode:
    logging.info("Writing laps to influx...")

  current_driver = "Driver" + str(car_number)

  # TODO: Concat driver from args
  write_success = True
  for lap in laps:
    '''
    if int(lap['Lap']) <= 72:
        current_driver = "Matt-Rotondo"
    elif int(lap['Lap']) > 72 and int(lap['Lap']) <= 150:
        current_driver = "Brian-Robideaux"
    elif int(lap['Lap']) > 150 and int(lap['Lap']) <= 210:
        current_driver = "Blair-Lichtenstein"
    elif int(lap['Lap']) > 210 and int(lap['Lap']) <= 264:
        current_driver = "Harrison-Co"
    elif int(lap['Lap']) > 264 and int(lap['Lap']) <= 331:
        current_driver = "Tom-McNulty"
    elif int(lap['Lap']) > 331 and int(lap['Lap']) <= 361:
        current_driver = "Brian-Robideaux"
    elif int(lap['Lap']) > 361 and int(lap['Lap']) <= 398:
        current_driver = "Blair-Lichtenstein"
    elif int(lap['Lap']) > 398:
        current_driver = "Harrison-Co"
     '''

    # Convert HH:MM:SS.MS1 to get time lap completed
    h, m, s = lap['TotalTime'].split(':')
    s, ms = s.split('.')
    lap_finish_time_milliseconds = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)
    # lap_seconds = int(h) * 3600 + int(m) * 60 + float(s)
    time_lap_completed_milliseconds = start_epoc + lap_finish_time_milliseconds
    lap_timestamp = str(time_lap_completed_milliseconds).replace(".", '')

    # Convert lap time to number of nanoseconds
    h, m, s = lap['LapTime'].split(':')
    s, ms = s.split('.')
    lap_time_in_milliseconds = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)

    # print(lap_timestamp)
    data = []
    data.append('laps{},driver={} lap_no={},lap_time={},position={},flag_status="{}" {}'.format(
        race_id, current_driver, lap['Lap'], lap_time_in_milliseconds, lap['Position'], lap['FlagStatus'], lap_timestamp))
    logging.debug(data)
    try:
      write_api.write(bucket='laps_252/autogen', record=data, write_precision='ms')
      logging.debug('Lap {} written to influx.'.format(lap['Lap']))
    except Exception as e:
      logging.error('Writing lap failed: {}'.format(e))
      write_success = False

  if write_success and not monitor_mode:
    logging.info('All lap data written successfully')

  print(underline)
  # query_data = influx.query('SELECT LAST(total_laps) FROM laps_252 BY *')
  # print(query_data.raw)

  return


if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    logging.info("Interrupted, exiting.")
    sys.exit(0)
