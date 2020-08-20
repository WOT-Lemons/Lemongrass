#!/usr/bin/python3
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals
from PyInquirer import prompt, print_json
from pprint import pprint
from operator import itemgetter
from influxdb import InfluxDBClient
import os
import sys
import subprocess
import requests
import json
import pickle
import time
import signal
import csv
import pandas 
import logging
import argparse

new_competitor = True
underline = "-" * 80
race_id = ''
racer_id = ''
car_number = ''
race_live = True

parser = argparse.ArgumentParser(description='Interact with lap data')
parser.add_argument('race_id', metavar='race_id', nargs=1, type=int, action='store')
parser.add_argument('car_number', metavar='car_number', nargs=1, type=int, action='store')
parser.add_argument('-m', '--monitor', dest='monitor_mode', default=False, action='store_true', help='Update when new data received')
parser.add_argument('-n', '--network', dest='network_mode', default=False, action='store_true', help='Forward lap data to network dest')
parser.add_argument('-o', '--out', dest='save_file', default=False, action='store_true', help='Write lap times to CSV')
parser.add_argument('-v', '--verbose', help="Set debug logging", action='store_true')
parser.set_defaults(monitor_mode=False, network_mode=False)
args = parser.parse_args()

def main():

    if args.verbose:
        print(args)
        # Set logging level - https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Pandas default max rows truncating lap times. I don't expect a team to do more than 1024 laps.
    pandas.set_option("display.max_rows", 1024)

    # Load tokenfile
    if os.path.exists('./.token'):
        f = open('.token', 'r')
        token = f.readline().rstrip()
        if token != "":
            logging.debug("Tokenfile opened and read")
    else:
        logging.error("Didn't open ./.token")
        sys.exit()

    influx = ''
    # Load influx password
    if args.network_mode:
        os.stat('/home/tom/.influxcred')
        if os.path.exists('/home/tom/.influxcred'):
            f = open('/home/tom/.influxcred', 'r')
            influx_pass = f.readline().rstrip()
            if influx_pass != "":
                logging.debug("Influx password opened and read")
                influx = InfluxDBClient('comms.wotlemons.com', 8086, 'car_252', influx_pass, 'laps_252')
        else:
            logging.error("Didn't open ~/.influxcred")
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

    payload = { 'apiToken': token, 'raceID': race_id}
    race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

    if race_details['Successful'] == True:
        race_name = race_details['Race']['Name']
        start_epoc = race_details['Race']['StartDateEpoc']
        start_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_epoc))
        end_epoc = race_details['Race']['EndDateEpoc']
        end_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_epoc))
        race_track = race_details['Race']['Track']
        print(underline)
        print("Race {}".format(race_id))
        print("{}\tStarted: {:>}\n{}\t\t\tEnds: {:>}".format(race_name, start_date, race_track, end_date))
        print(underline)

    # Check if race is live
    payload = { 'apiToken': token, 'RaceID': race_id}
    response = callRaceMonitor('/v2/Race/IsLive', payload)
    
    if response['Successful']:
        if response['IsLive'] is not True:
            race_live = False
            logging.info("Race {} is not live. Monitor mode disabled.".format(race_id))
            if args.monitor_mode:
                 return
            else:
                oldRace(race_id, token, args.network_mode, start_epoc, influx, args.save_file)
        else:
            logging.info("Race {} is currently live.".format(race_id))
            liveRace(race_id, token, args.network_mode, args.monitor_mode, args.save_file)

    return 0

def liveRace(race_id, token, network_mode, monitor_mode, save_file):
    """
    Function name: liveRace
    Arguments: race_id, token
    Description: Called if a race ID is live. 
    """
    payload = { 'apiToken': token, 'raceID': race_id}
    last_session_details = callRaceMonitor('/v2/Live/GetSession', payload)
    
    list_of_competitors = []

    for competitor in competitors:
        list_of_competitors.append(competitors[competitor])

    for competitor in list_of_competitors:
        for key, value in competitor.items():
            try: 
                if key == 'Position':
                    competitor[key] = int(value) 
            except ValueError: value = None 

    # Remove competitors (LOSERS)  with no position
    list_of_competitors = [racer for racer in list_of_competitors if racer['Number'] != '']
    #print(list_of_competitors)
        
    for i in range(len(list_of_competitors)): 
        print(list_of_competitors[i])
        if list_of_competitors[i]['Position'] == '': 
            print("Dirty data")
            list_of_competitors.remove(list_of_competitors[i]) 
            break
        

    sorted_competitors = sorted(list_of_competitors, key=lambda k: int(itemgetter('Position')(k))) 

    printRankings(sorted_competitors, race_live)

    if session['ID']== session_ids_for_race[-1]:
        competitors = session
        
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

    racer_id = car_number

    competitor_details = []
    competitor_lap_times = []

    # Get lap times from live racer
    logging.debug("Getting lap times for {} from race {}.".format(racer_id, race_id))
    payload = { 'apiToken': token, 'RacerID': racer_id, 'RaceID': race_id}
    response = callRaceMonitor('/v2/Live/GetRacer', payload)

    if response['Successful'] == True:
        laps = response['Details']['Laps']
        competitor_details = response['Details']['Competitor']

    #Make name 
    competitor_details['Name'] = competitor_details['FirstName'] + competitor_details['LastName']

    print(underline)
    # Print competitor detail block
    print("Team: {:<6} Car Number: {:<4} Transponder: {}".format(competitor_details['Name'], competitor_details['Number'], competitor_details['Transponder']))
    print("Best Position:\t{:>}\nFinal Position:\t{:>}\nTotal Laps:\t{:>}\nBest Lap:\t{:>}\nBest Lap Time:\t{:>}\nTotal Time:\t{:>}".format(competitor_details['BestPosition'],competitor_details['Position'], competitor_details['Laps'], competitor_details['BestLap'], competitor_details['BestLapTime'], competitor_details['TotalTime']))
    print(underline)
    
    # Create pandas dataframe and print without index to remove row numbers
    lap_time_df = pandas.json_normalize(laps)
    print(lap_time_df.to_string(index=False))
    print(underline)
    
    # If we're going to be starting network mode, check for presence of existing data.
    if network_mode:
        network_status = pushInflux(racer_id, laps, influx, start_epoc)

    if save_file:
        # Create filename and call function to write to CSV
        filename = "{}-{}".format(competitor_details['Name'], race_id)
        writeCSV(filename, laps)

    if monitor_mode:
        #Enter monitoring loop
        competitor_last_lap = laps[-1]['LapTime']
        #if args.network_mode == True:
            #monitorRoutine(car_number, laps, race_id, racer_id, token, influx=influx, start_epoc=start_epoc)
        #else:
        monitorRoutine(car_number, laps, race_id, racer_id, token)
    
    return

def oldRace(race_id, token, network_mode, start_epoc, influx, save_file):
    """
    Function name: oldRace
    Arguments: race_id, token
    Description: Called if a race ID is not live. 
    """

    logging.debug("Getting sessions for race for {}".format(race_id))
    payload = { 'apiToken': token, 'raceID': race_id}
    race_details = callRaceMonitor('/v2/Results/SessionsForRace', payload)

    session_ids_for_race = []
    competitor_lap_times = []

    #Get only session IDs in session_ids_for_race
    for i in race_details['Sessions']:
        session_ids_for_race.append(i['ID'])

    logging.debug("Race {} has {} sessions, {}".format(race_id, len(session_ids_for_race), session_ids_for_race))

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
   
    laps = []
    competitor_missing = True

    # Send request for all session_ids from a race, including lap times
    for session_id in session_ids_for_race:
        logging.debug("Getting session details for {} including lap times.".format(session_id))
        payload = { 'apiToken': token, 'sessionID': session_id, 'includeLapTimes': True}
        #payload = { 'apiToken': token, 'sessionID': session_id, 'includeLapTimes': False}
        session_details = callRaceMonitor('/v2/Results/SessionDetails', payload)
        try:
            sorted_competitors = session_details['Session']['SortedCompetitors'].copy()
        except TypeError: logging.error('session_details for {} empty: {}'.format(session_id, session_details))

        for competitor in sorted_competitors:
            #print(competitor['Number'])
            if competitor['Number'] == car_number:
                competitor_missing = False
                competitor_details = competitor
                laps = laps + competitor['LapTimes'].copy()
        
    if competitor_missing:
        logging.info('Car {} not found'.format(car_number))
        print(underline)
        return 1
                
    #lap_times = session_details['Session']['SortedCompetitors']
    
    race_live = False
    printRankings(sorted_competitors, race_live)
    
    # Print competitor detail block
    print("Team: {:<6}\tCar Number: {:<4}\tTransponder: {}".format(competitor_details['FirstName'], competitor_details['Number'], competitor_details['Transponder']))
    print("Best Position:\t{:>}\nFinal Position:\t{:>}\nTotal Laps:\t{:>}\nBest Lap:\t{:>}\nBest Lap Time:\t{:>}\nTotal Time:\t{:>}".format(competitor_details['BestPosition'],competitor_details['Position'], competitor_details['Laps'], competitor_details['BestLap'], competitor_details['BestLapTime'], competitor_details['TotalTime']))
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
            #print(competitor.keys())
            try: 
                if key == 'Position':
                    #print(value)
                    competitor[key] = int(value) 
            except ValueError: value = None 
    
    #pprint(lap_times)
    
    # If we're going to be starting network mode, check for presence of existing data.
    if args.network_mode:
        network_status = pushInflux(racer_id, laps, influx, start_epoc, race_id)
   
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
    r = requests.post(api_url, data = payload)
    if r.status_code == 200:
        return json.loads(r.text)
    elif r.status_code == 429:
        logging.error('{} - Too many requests, waiting 20 seconds...'.format(r.status_code))
        time.sleep(20)
        r = callRaceMonitor(api_endpoint, payload)
        #r = requests.post(api_url, data = payload)
    else:
        logging.error('Error {}'.format(r.status_code))
        return json.loads(r.text)
    return

def printRankings(sorted_competitors, race_live):
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
        if race_live is True:
            print(f"{competitor['Position']: <4} {competitor['Number']:<4} {competitor['Name']:<32} {competitor['Laps']: <4} {competitor['RacerID']:<15} {competitor['Transponder']:<6}")
        else:
            print(f"{competitor['Position']: <4} {competitor['Number']:<4} {competitor['Name']:<32} {competitor['Laps']: <4} {competitor['ID']:<15} {competitor['Transponder']:<6}")
            #print(competitor)
        
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

def monitorRoutine(car_number, laps, race_id, racer_id, token):
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

    #logging.debug("\nLast lap: {}\nLast lap: {} seconds\nSleep Time: {} seconds".format(last_lap_time, last_lap_seconds, sleep_interval))

    while True:
        time.sleep(30)
        current_competitor_lap_times = refreshCompetitor(race_id, racer_id, token)
        #if current_competitor_lap_times[-1] == laps[-1]:
        #    pass
        #else:
        if current_competitor_lap_times not in laps:
            current_competitor_lap_time_df = pandas.json_normalize(current_competitor_lap_times[-1])
            print(current_competitor_lap_time_df.to_string(index=False, header=False))
            #print(current_competitor_lap_times[-1])
            laps.append(current_competitor_lap_times)
            if args.network_mode == True:
                pushInflux(racer_id, current_competitor_lap_times, influx, start_epoc)
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
    payload = { 'apiToken': token, 'RaceID': race_id, 'RacerID': racer_id}
    response = callRaceMonitor('/v2/Live/GetRacer', payload)

    #try:
    if response['Successful'] == True:
        laps = response['Details']['Laps']
        competitor_details = response['Details']['Competitor']
    #except TypeError: return 
    
    logging.debug("Current lap is {} with time {}.".format(laps[-1]['Lap'], laps[-1]['LapTime']))
    response = []
    return laps

def pushInflux(racer_id, laps, influx, start_epoc, race_id):
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

    logging.info("Writing laps to influx...")
    
    for lap in laps:
        current_driver = "Driver252"

        # Convert HH:MM:SS.MS1 to get time lap completed
        try:
            h, m, s = lap['TotalTime'].split(':')
        except ValueError: h, m = 0
        
        try:
            s, ms = s.split('.')
        except ValueError: s, ms = 0
        
        lap_finish_time_milliseconds = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)
        #lap_seconds = int(h) * 3600 + int(m) * 60 + float(s)
        time_lap_completed_milliseconds = start_epoc + lap_finish_time_milliseconds
        lap_timestamp = str(time_lap_completed_milliseconds).replace(".", '')
        
        # Convert lap time to number of nanoseconds
        try:
            h, m, s = lap['LapTime'].split(':')
        except ValueError: h, m = 0
        
        try:
            s, ms = s.split('.')
        except ValueError: s, ms = 0

        lap_time_in_milliseconds = int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)
        
        #print(lap_timestamp)
        data = []
        data.append('laps{},driver={} lap_no={},lap_time={},position={},flag_status="{}" {}'.format(race_id,current_driver,lap['Lap'],lap_time_in_milliseconds,lap['Position'],lap['FlagStatus'],lap_timestamp))
        logging.debug(data)
        write_success = True
        if influx.write_points(data, database='laps_252', protocol='line', time_precision='ms'):
            logging.debug('Lap {} written to influx.'.format(lap['Lap']))
        else:
            logging.debug('Writing lap failed.')
            write_success = False

    if write_success:
        logging.info('All lap data written successfully')
    
    print(underline)
    #query_data = influx.query('SELECT LAST(total_laps) FROM laps_252 BY *')
    #print(query_data.raw)
    
    return

if __name__ == '__main__':
    main()


    """
    # Originally race_details was needed for series_id and race_type_id. 
    # This code may be useful in the future so it's staying. 
    
    # Every time I work on this lap the structure of the data changes as the 
    # race monitor guys continue work on their platform. They've begun working
    # more towards a live model so this results focused code may be useful after
    # the race but is currently contrary to hooking this up to influx.

    logging.debug("Getting details for {}".format(race_id))
    payload = { 'apiToken': token, 'raceID': race_id}
    race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

    series_id = race_details['Race']['SeriesID']
    race_type_id = race_details['Race']['RaceTypeID']

    logging.debug("Getting sessions for {}".format(race_id))
    payload = { 'apiToken': token, 'raceID': race_id}
    sessions_for_race = callRaceMonitor('/v2/Results/SessionsForRace', payload)

    payload = { 'apiToken': token, 'raceID': race_id}
    race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

    payload = { 'apiToken': token, 'raceID': race_id}
    get_session = callRaceMonitor('/v2/Live/GetSession', payload)
    
    print(sessions_for_race)
    print(race_details)
    print(get_session)
    return

    session_ids_for_race = []

    # Get only session IDs in session_ids_for_race
    #for i in sessions_for_race['Sessions']:
        #session_ids_for_race.append(['ID'])

    logging.debug("Race {} has {} sessions, {}".format(race_id, len(session_ids_for_race), session_ids_for_race))

    last_session_id = sessions_for_race['Sessions'][-1]['ID']
    logging.debug("Getting rankings from last session, {}.".format(sessions_for_race['Sessions'][-1]['ID']))
    """    
    """
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
    """
