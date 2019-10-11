#!/usr/bin/python
# -*- coding: utf-8 -*-

from pprint import pprint
from influxdb import InfluxDBClient
import os
import sys

import obd

connection = obd.OBD() 

cmd = obd.commands.SPEED 

print 'Getting PIDs [1-20]...'
pids_a = connection.query(PIDS_A)
print 'Getting PIDS [21-40]...'
pids_b = connection.query(PIDS_B)
print 'Getting PIDs [41-60]...'
pids_c = connection.query(PIDS_C)
print '-----------------------'
print(pids_a.value)
print(pids_b.value)
print(pids_c.value)

json_body = [
    {
        "measurement": "speed",
        "tags": {
            "speed": "mph",
        },
        "time": $curr_time,
        "fields": {
            "value": $speed_value
        }
    }
]

client = InfluxDBClient('comms.wotlemons.com', 8086, 'car_252', 'o083@3!04Q7sC7l1A%owKD4w9', 'stats_252')

client.write_points(json_body)

result = client.query('select value from cpu_load_short;')

print("Result: {0}".format(result))
