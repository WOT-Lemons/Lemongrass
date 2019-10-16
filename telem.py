#!/usr/bin/python
# -*- coding: utf-8 -*-

from pprint import pprint
from influxdb import InfluxDBClient
import os
import sys
import time
from time import sleep
import obd
import datetime

def new_value(r):
    ts = datetime.datetime.now()
    measurement = str(r.command).split(":")[1]
    measurement = measurement.replace(" ", "-")
    print measurement
    try:
        print r.value.magnitude
        json_body = [
        {
            "measurement": measurement,
            "time": ts,
            "fields": {
                "value": r.value.magnitude
            }
        }]
    except AttributeError:
        print r.value[1]

        json_body = [
        {
            "measurement": measurement,
            "time": ts,
            "fields": {
                "value": r.value[1]
            }
        }]
    client.write_points(json_body)


def new_fuel_status(r):
    ts = datetime.datetime.now()
    measurement = str(r.command).split(":")[1]
    measurement = measurement.replace(" ", "-")
    if "Open loop due to insufficient engine temperature" in r.value:
        fuel_status = 0
    elif "Closed loop, using oxygen sensor feedback to determine fuel mix" in r.value:
        fuel_status = 1
    elif "Open loop due to engine load OR fuel cut due to deceleration" in r.value:
        fuel_status = 2
    elif "Open loop due to system failure" in r.value:
        fuel_status = 3
    elif "Closed loop, using at least one oxygen sensor but there is a fault in the feedback system" in r.value:
        fuel_status =4
    json_body = [
    {
        "measurement": measurement,
        "time": ts,
        "fields": {
                "value": fuel_status
            }
    }]
    client.write_points(json_body)

obd.logger.setLevel(obd.logging.DEBUG)
connection = obd.Async()

connection.watch(obd.commands.FUEL_STATUS, callback=new_fuel_status )
connection.watch(obd.commands.ENGINE_LOAD, callback=new_value )
connection.watch(obd.commands.COOLANT_TEMP, callback=new_value )
connection.watch(obd.commands.SHORT_FUEL_TRIM_1, callback=new_value )
connection.watch(obd.commands.LONG_FUEL_TRIM_1, callback=new_value )
connection.watch(obd.commands.FUEL_PRESSURE, callback=new_value )
connection.watch(obd.commands.INTAKE_PRESSURE, callback=new_value )
connection.watch(obd.commands.RPM, callback=new_value )
connection.watch(obd.commands.SPEED, callback=new_value )
connection.watch(obd.commands.TIMING_ADVANCE, callback=new_value )
connection.watch(obd.commands.INTAKE_TEMP, callback=new_value )
connection.watch(obd.commands.MAF, callback=new_value )
connection.watch(obd.commands.THROTTLE_POS, callback=new_value )
connection.watch(obd.commands.O2_B1S1, callback=new_value )
connection.watch(obd.commands.O2_B1S2, callback=new_value )
connection.start()

obd.logger.setLevel(obd.logging.INFO)

while True:
    sleep(0.05)
#except Exception as e:
#    print(e)
#    connection.close()
