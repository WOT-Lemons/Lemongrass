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

# Load tokenfile
if os.path.exists('./.influxcred'):
    f = open('.influxcred', 'r')
    influx_pass = f.readline().rstrip()
        if influx_pass != "":
            logging.debug("Influx cred opened and read")

client = InfluxDBClient('comms.wotlemons.com', 8086, 'car_252', influx_pass, 'stats_252')

def new_value(r):
    ts = datetime.datetime.now()
    measurement = str(r.command).split(":")[1]
    measurement = measurement.replace(" ", "-")
    try:
        json_body = [
        {
            "measurement": measurement,
            "time": ts,
            "fields": {
                "value": r.value.magnitude
            }
        }]
        client.write_points(json_body)
    except TypeError:
        print("Caught TypeError in new_value")
        main()
    except AttributeError:
        print("Caught AttributeError in new_value")
        main()

def new_fuel_status(r):
    try:
        if not r.value[1]:
            raise TypeError
    except TypeError:
        print("Caught TypeError in new_fuel_status")
        main()
    
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

def main():

    connection = obd.Async()
    status = connection.status()
    while "Car Connected" not in status:
        print("No car connected, sleeping...")
        connection = obd.Async()
        status = connection.status()
        
    print(connection.status())
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

    while True:
        sleep(0.5)

    #obd.logger.setLevel(obd.logging.INFO)

if __name__== "__main__":
  main()
