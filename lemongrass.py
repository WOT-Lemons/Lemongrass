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
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.client import flow_from_clientsecrets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

pd.set_option("display.max_rows", 1024)
underline = "-" * 80

def callRaceMonitor(endpoint, payload):
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
    underline = "-" * 80
    print(underline)
    list_of_names = []

    for competitor in sorted_competitors:
        for item in competitor:
            if item == "FirstName":
                list_of_names.append(competitor[item])

    print(f'{"Pos.": <4} {"#":<4} {"First Name": <32} {"Laps": <4} {"Competitor ID":<15} {"Transponder ID":<6}')
    underline = "-" * 80
    print(underline)

    for competitor in sorted_competitors:
        #print(competitor['Position'], competitor['Laps'], competitor['FirstName'], competitor['ID'], competitor['Transponder'])
        print(f"{competitor['Position']: <4} {competitor['Number']:<4} {competitor['FirstName']:<32} {competitor['Laps']: <4} {competitor['ID']:<15} {competitor['Transponder']:<6}")
        
    print(underline)
    return

creds = None

if os.path.exists('./.token'):
    f = open('.token', 'r')
    token = f.readline().rstrip()
    if token != "":
        logging.debug("Tokenfile opened and read")
else:
    logging.error("Didn't open ./.token")
    sys.exit()

while True:
    try: 
        race_id = sys.argv[1]
        break
    except IndexError:
        payload = { 'apiToken': token}
        current_races = callRaceMonitor('/v2/Account/CurrentRaces', payload)
        print(current_races['Races'])
        race_id = input("Race ID: ")
        break
    try: race_id
    except NameError: x = None

logging.debug("Getting details for {}".format(race_id))
payload = { 'apiToken': token, 'raceID': race_id}
race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

series_id = race_details['Race']['SeriesID']
race_type_id = race_details['Race']['RaceTypeID']

logging.debug("Getting sessions for {}".format(race_id))
payload = { 'apiToken': token, 'raceID': race_id}
sessions_for_race = callRaceMonitor('/v2/Results/SessionsForRace', payload)

session_ids_for_race = []

for i in sessions_for_race['Sessions']:
    session_ids_for_race.append(i['ID'])

logging.debug("Race {} has {} sessions, {}".format(race_id, len(session_ids_for_race), session_ids_for_race))

last_session_id = sessions_for_race['Sessions'][-1]['ID']
logging.debug("Getting rankings from last session, {}.".format(sessions_for_race['Sessions'][-1]['ID']))
payload = { 'apiToken': token, 'sessionID': last_session_id}
last_session_details = callRaceMonitor('/v2/Results/SessionDetails', payload)

col_width = max(len(word) for row in last_session_details['Session']['SortedCompetitors'] for word in row) + 2
sorted_competitors = last_session_details['Session']['SortedCompetitors']

printRankings(sorted_competitors)

while True:
    try: 
        car_number = sys.argv[2]
        break
    except IndexError:
        car_number = input("Car Number: ")
        break

competitor_details = []
competitor_lap_times = []

for session_id in session_ids_for_race:
    logging.debug("Getting session details for {} including lap times.".format(session_id))
    payload = { 'apiToken': token, 'sessionID': session_id, 'includeLapTimes': True}
    lap_times = callRaceMonitor('/v2/Results/SessionDetails', payload)

    for competitor in lap_times['Session']['SortedCompetitors']:
        if competitor['Number'] == car_number:
            competitor_lap_times = competitor_lap_times + competitor['LapTimes']
            if session_id == last_session_id:
                competitor_details = competitor

print("Team: {:<6} Car Number: {:<4} Transponder: {}".format(competitor_details['FirstName'], competitor_details['Number'], competitor_details['Transponder']))
print("Best Position:\t{:>}\nFinal Position:\t{:>}\nTotal Laps:\t{:>}\nBest Lap:\t{:>}\nBest Lap Time:\t{:>}\nTotal Time:\t{:>}".format(competitor_details['BestPosition'],competitor_details['Position'], competitor_details['Laps'], competitor_details['BestLap'], competitor_details['BestLapTime'], competitor_details['TotalTime']))
print(underline)

filename = "{}-{}-{}".format(competitor_details['FirstName'], race_id, last_session_id)
logging.info('Saving lap times to {}.csv'.format(filename))
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

#printLapTimes(competitor_lap_times)

lap_time_df = pd.io.json.json_normalize(competitor_lap_times)
print(lap_time_df.to_string(index=False))

"""
def writeToSheets

##TODO
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1qWZ3U5el_bRqxSP4du04A_ZiO5S93m7OsFrsU3oRBLE'
RANGE_NAME = 'Sheet1'

if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

# Call the Sheets API
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
values = result.get('values', [])

if not values:
    print('No data found.')


"""
