#!/usr/bin/python3
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals
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
from influxdb import InfluxDBClient
import logging
#from googleapiclient.discovery import build
#from google_auth_oauthlib.flow import InstalledAppFlow
#from google.auth.transport.requests import Request
#from oauth2client.client import flow_from_clientsecrets

underline = "-" * 80

def main():

    # Set logging level - https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    payload = { 'apiToken': token, 'raceID': race_id, 'racerID': '252'}

    while True:
        laps_for_racer = callRaceMonitor('/v2/Live/GetRacer', payload)

        for laps_for_racer['Laps']:
            print Lap

        current_time = sessions_for_race['Session']['CurrentTime']

        our_position = sessions_for_race['Session']['Competitors']['252']['Position']
        our_laps = sessions_for_race['Session']['Competitors']['252']['Laps']

        client = InfluxDBClient('localhost', 8086, 'car_252', 'oA6&Li*#1le3aRE@99uxf^JCm', 'laps_252')

        json_body = [
            {
                "measurement": "total_laps",
                "time": current_time,
                "fields": {
                    "value": our_laps
                }
            }]

        client.write_points(json_body)

        json_body = [
            {
                "measurement": "position",
                "time": current_time,
                "fields": {
                    "value": our_position
                }
            }]

        client.write_points(json_body)
    
        time.sleep(15)
    
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

if __name__ == '__main__':
    main()
