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
    #print(r)
    ts = datetime.datetime.now()
    measurement = str(r.command).split(":")[1]
    measurement = measurement.replace(" ", "-")
    #print measurement
    try:
        if r.value.magnitude is None:
            r.value.magnitude = 0
        json_body = [
        {
            "measurement": measurement,
            "time": ts,
            "fields": {
                "value": r.value.magnitude
            }
        }]
    except AttributeError:
        if r.value[1] is None:
            r.value[1] = 0
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
        fuel_status = 4
    if fuel_status is None:
        fuel_status = 255
    json_body = [
    {
        "measurement": measurement,
        "time": ts,
        "fields": {
                "value": fuel_status
            }
    }]
    client.write_points(json_body)

#obd.logger.setLevel(obd.logging.INFO)

connection = obd.Async()
supported_commands = connection.supported_commands
watch_commands = {}

for command in supported_commands:
    if "DTC" not in command.name:
        if "MIDS" not in command.name:
            if "PIDS" not in command.name:
                if "O2_SENSORS" not in command.name:
                    if command.name != "STATUS":
                        if "ELM" not in command.name:
                            if "OBD" not in command.name:
                                if "FUEL_STATUS" in command.name:
                                    print(command.name, " supported, watching...")
                                    connection.watch(command, callback=new_fuel_status)
                                else:
                                    print(command.name, " supported, watching...")
                                    connection.watch(command, callback=new_value)

connection.start()

#obd.logger.setLevel(obd.logging.INFO)

while True:
    sleep(0.05)
    
    ts = datetime.datetime.now()
    measurement = "heartbeat"

    json_body = [
        {
            "measurement": measurement,
            "time": ts,
            "fields": {
                "value": 1
            }
        }]

    client.write_points(json_body)
#except Exception as e:
#    print(e)
#    connection.close()
