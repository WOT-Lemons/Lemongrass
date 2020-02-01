#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals
from PyInquirer import prompt, print_json
from pprint import pprint
import os
import sys
import requests
import json
import pickle
import time
import signal
import csv
import pandas as pd
import logging
import argparse
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.client import flow_from_clientsecrets

new_competitor = True
underline = "-" * 80

parser = argparse.ArgumentParser(description='Interact with lap data')
parser.add_argument('race_id', metavar='race_id', nargs=1, type=int, action='store')
parser.add_argument('car_number', metavar='car_number', nargs=1, type=int, action='store')
parser.add_argument('--monitor', dest='monitor_mode', action='store_true', help='Update when new data receieved')
parser.add_argument('--network', dest='network_mode', action='store_true', help='Forward lap data to network dest')
parser.add_argument("-v", "--verbose", help="Set debug logging", action="store_true")
parser.set_defaults(monitor_mode=False, network_mode=False)
args = parser.parse_args()

def main():

    if args.verbose:
        print(args)
        # Set logging level - https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # Pandas default max rows truncating lap times. I don't expect a team to do more than 1024 laps.
    pd.set_option("display.max_rows", 1024)

    # Load tokenfile
    if os.path.exists('./.token'):
        f = open('.token', 'r')
        token = f.readline().rstrip()
        if token != "":
            logging.debug("Tokenfile opened and read")
    else:
        logging.error("Didn't open ./.token")
        sys.exit()

    # Get race_id from first argument or prompt user for it. 
    # May add table of current races in the future to browse from the app.
    while True:
        try: 
            race_id = sys.argv[1]
            break
        except IndexError:
            #payload = { 'apiToken': token}
            #current_races = callRaceMonitor('/v2/Account/CurrentRaces', payload)
            #print(current_races['Races'])
            race_id = input("Race ID: ")
            break
        try: race_id
        except NameError: race_id = None
   
    """
    # Originally race_details was needed for series_id and race_type_id. 
    # This code may be useful in the future so it's staying. 

    logging.debug("Getting details for {}".format(race_id))
    payload = { 'apiToken': token, 'raceID': race_id}
    race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

    series_id = race_details['Race']['SeriesID']
    race_type_id = race_details['Race']['RaceTypeID']
    """

    logging.debug("Getting sessions for {}".format(race_id))
    payload = { 'apiToken': token, 'raceID': race_id}
    sessions_for_race = callRaceMonitor('/v2/Results/SessionsForRace', payload)

    session_ids_for_race = []

    # Get only session IDs in session_ids_for_race
    for i in sessions_for_race['Sessions']:
        session_ids_for_race.append(['ID'])

    logging.debug("Race {} has {} sessions, {}".format(race_id, len(session_ids_for_race), session_ids_for_race))

    last_session_id = sessions_for_race['Sessions'][-1]['ID']
    logging.debug("Getting rankings from last session, {}.".format(sessions_for_race['Sessions'][-1]['ID']))
    payload = { 'apiToken': token, 'sessionID': last_session_id}
    last_session_details = callRaceMonitor('/v2/Results/SessionDetails', payload)

    sorted_competitors = last_session_details['Session']['SortedCompetitors']

    list_of_names = printRankings(sorted_competitors)
    
    while('' in list_of_names):
        list_of_names.remove('')

    #print(list_of_names)

    # Get car number from second argument or user input. 
    while True:
        try: 
            car_number = sys.argv[2]
            break
        except IndexError:
            car_number = input("Car Number: ")
            break
        try: car_number
        except NameError: car_number = None

    competitor_details = []
    competitor_lap_times = []

    # Send request for all session_ids from a race, including lap times
    for session_id in session_ids_for_race:
        logging.debug("Getting session details for {} including lap times.".format(session_id))
        payload = { 'apiToken': token, 'sessionID': session_id, 'includeLapTimes': True}
        lap_times = callRaceMonitor('/v2/Results/SessionDetails', payload)

        # For a specific competitor, extract lap times from all recieved sessions and concatenate into one list
        lap_times = callRaceMonitor('/v2/Results/SessionDetails', payload)
        for competitor in lap_times['Session']['SortedCompetitors']:
            if competitor['Number'] == car_number:
                competitor_lap_times = competitor_lap_times + competitor['LapTimes']
                if session_id == last_session_id:
                    competitor_details = competitor

    #Make name 
    competitor_details['Name'] = competitor_details['FirstName'] + competitor_details['LastName']

    if args.monitor_mode == True:
        #Enter monitoring loop
        competitor_last_session = session_ids_for_race[-1]
        competitor_last_lap = competitor_lap_times[-1]
        monitorRoutine(car_number, competitor_last_session, competitor_last_lap)
    else:
        # Print competitor detail block
        print(underline)
        print("Team: {:<6} Car Number: {:<4} Transponder: {}".format(competitor_details['Name'], competitor_details['Number'], competitor_details['Transponder']))
        print("Best Position:\t{:>}\nFinal Position:\t{:>}\nTotal Laps:\t{:>}\nBest Lap:\t{:>}\nBest Lap Time:\t{:>}\nTotal Time:\t{:>}".format(competitor_details['BestPosition'],competitor_details['Position'], competitor_details['Laps'], competitor_details['BestLap'], competitor_details['BestLapTime'], competitor_details['TotalTime']))
        print(underline)
        
        # Create pandas dataframe and print without index to remove row numbers
        lap_time_df = pd.io.json.json_normalize(competitor_lap_times)
        print(lap_time_df.to_string(index=False))

    # Create filename and call function to write to CSV
    filename = "{}-{}-{}".format(competitor_details['Name'], race_id, last_session_id)
    writeCSV(filename, competitor_lap_times)

    return 0

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
    r = requests.post(api_url, data = payload)
    if r.status_code == 200:
        return json.loads(r.text)
    elif r.status_code == 429:
        logging.error('{} - Too many requests, waiting 60 seconds...'.format(r.status_code))
        time.sleep(60)
        r = requests.post(api_url, data = payload)
    else:
        logging.error('Error {}'.format(r.status_code))
    return

def printRankings(sorted_competitors):
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
        #print(competitor)
        for item in competitor:
          #print(item)
            if item == "FirstName" and item != '':
                list_of_names.append(competitor[item])
            elif item == "LastName" and item != '':
                list_of_names.append(competitor[item])

    print(f'{"Pos.": <4} {"#":<4} {"First Name": <32} {"Laps": <4} {"Competitor ID":<15} {"Transponder ID":<6}')
    underline = "-" * 80
    print(underline)

    for competitor in sorted_competitors:
        if competitor['FirstName'] == '':
            competitor['Name'] = competitor['LastName']
        else:
            competitor['Name'] = competitor['FirstName']
        #print(competitor['Position'], competitor['Laps'], competitor['FirstName'], competitor['ID'], competitor['Transponder'])
        print(f"{competitor['Position']: <4} {competitor['Number']:<4} {competitor['Name']:<32} {competitor['Laps']: <4} {competitor['ID']:<15} {competitor['Transponder']:<6}")
        
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
    lap_csv_fh = open('./%s.csv' % filename, 'w')
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

def monitorRoutine(car_number, session_id, last_lap):
    """
    Function name: monitorRoutine
    Arguments: car_number, last_lap_time
    Description: Destination routine for monitor mode.
                 Holds about the time of a lap and then checks
                 to see if there's a new one. If there is none, 
                 hold until there is and then print a lap line. Repeat
    """
    logging.info('Starting monitor mode...')
    print(underline)

    print("Monitoring car {}\nLast lap time: {}".format(car_number, last_lap))
    last_lap_time = last_lap['LapTime']
    #Convert HH:MM:SS.MS
    h, m, s = last_lap_time.split(':')
    last_lap_seconds = int(h) * 3600 + int(m) * 60 + float(s)
    sleep_interval = last_lap_seconds + 15
    logging.debug("Last lap: {}\nLast lap: {} seconds\nSleep Time: {} seconds".format(last_lap_time, last_lap_seconds, sleep_interval))

    while True:
        time.sleep(sleep_interval)
        current_competitor_lap_times = refreshCompetitor(session_id)
        current_last_lap = current_competitor_lap_times[-1]
        print(last_lap_time,current_last_lap)
    
    return

def refreshCompetitor(car_number, session_id, payload):
    """
    Function name: refreshCompetitor
    Arguments: car_number, last_lap_time
    Description: If this is a new competitor, get all laps from all sessions. 
                 If not, check only for the last lap from the last session. 
    """

    competitor_lap_times = []
    competitor_details = []
    
    logging.debug("Getting session details for {} including lap times.".format(session_id))
    payload = { 'apiToken': token, 'sessionID': session_id, 'includeLapTimes': True}
    lap_times = callRaceMonitor('/v2/Results/SessionDetails', payload)

    lap_times = callRaceMonitor('/v2/Results/SessionDetails', payload)
    for competitor in lap_times['Session']['SortedCompetitors']:
        if competitor['Number'] == car_number:
            competitor_lap_times = competitor_lap_times + competitor['LapTimes']
    
    return competitor_lap_times

if __name__ == '__main__':
    main()



