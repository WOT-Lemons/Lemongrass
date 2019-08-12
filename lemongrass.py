#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals
from PyInquirer import prompt
from pprint import pprint
from eprint import eprint
import os
import sys
import requests
import json
import pickle
import logging
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.client import flow_from_clientsecrets

def callRaceMonitor(endpoint, payload):
    api_base_url = 'https://api.race-monitor.com'
    api_endpoint = endpoint
    api_url = api_base_url + api_endpoint
    r = requests.post(api_url, data = payload)
    return json.loads(r.text)

creds = None

#parser = argparse.ArgumentParser(description='Process some integers.')
#parser.add_argument('--raceid', dest='race_id', action='store',
#                    help='Race ID from https://www.race-monitor.com/Live/Race/')

#args = parser.parse_args(['--raceid'])
#print(args)
#race_id = args[]

if os.path.exists('./.token'):
    f = open('.token', 'r')
    token = f.readline().rstrip()
    if token != "":
        eprint("SETUP: Tokenfile opened and read")
else:
    eprint("ERROR: Didn't open ./.token")
    sys.exit("ERROR: Didn't open ./.token")

while True:
    try: 
        race_id = sys.argv[1]
        break
    except IndexError:
        race_id = input("Race ID: ")
        break

try: race_id
except NameError: x = None

eprint("INFO: Getting details for {}".format(race_id))
payload = { 'apiToken': token, 'raceID': race_id}
race_details = callRaceMonitor('/v2/Race/RaceDetails', payload)

series_id = race_details['Race']['SeriesID']
race_type_id = race_details['Race']['RaceTypeID']

payload = { 'apiToken': token, 'raceID': race_id}
sessions_for_race = callRaceMonitor('/v2/Results/SessionsForRace', payload)

print(sessions_for_race)

print("\t\tSession ID: {}".format(sessions_for_race['Sessions'][0]['ID']))
r = requests.post('https://api.race-monitor.com/v2/Results/SessionDetails', data = {'apiToken': token, 'sessionID': sessions_for_race['Sessions'][0]['ID']})
SessionDetails = json.loads(r.text)
for i in SessionDetails['Session']['SortedCompetitors']:
    if i['FirstName'] == 'WOT LEMONS':
        #print(i)
        competitor_id = i['ID']
        print("\t\tCompetitor ID: {}".format(competitor_id))
print("\tINFO: Retrieving /v2/Results/CompetitorDetails for {}".format(competitor_id))
r = requests.post('https://api.race-monitor.com/v2/Results/CompetitorDetails', data = {'apiToken': token, 'competitorID': competitor_id})
CompetitorDetails = json.loads(r.text)
print(CompetitorDetails)

    
    #print("\tINFO: {}".format(data['Sessions']['ID']))



def testConnection():
    #print("\tSETUP: Testing connection and authorization...")
    r = requests.post('https://api.race-monitor.com/v2/Account/AllRaces', data = {'apiToken': token})
    AllRaces = json.loads(r.text)
    if AllRaces['Successful'] != 'true':
        print()
    else:
        print(r.status_code)
        sys.exit("\tERROR: Endpoint 'v2/Account/AllRaces' returned the above error.")
    return r.status_code

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
