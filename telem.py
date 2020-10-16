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
import socket
import logging
import logging.handlers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('telem')

syslogHandler = logging.handlers.SysLogHandler(address=('localhost', 6514), facility='user', socktype=socket.SOCK_DGRAM)
#stdoutHandler = logging.StreamHandler(sys.stdout) 

logger.addHandler(syslogHandler)
#logger.addHandler(stdoutHandler)


# Load tokenfile
if os.path.exists('/home/pi/.influxcred'):
  #logger.debug("Opening secret...")
  f = open('/home/pi/.influxcred', 'r')
  influx_pass = f.readline().rstrip()
  if influx_pass != "":
    logger.debug("Influx cred opened and read")
  else:
    logger.debug("Failed to open ~/.influxcred")

client = InfluxDBClient('comms.wotlemons.com', 8086, 'car_252', influx_pass, 'stats_252')

def new_value(r):
    client = InfluxDBClient('comms.wotlemons.com', 8086, 'car_252', influx_pass, 'stats_252')
    ts = datetime.datetime.utcnow()
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
        logger.debug("Caught TypeError in new_value")
        main()
    except AttributeError:
        logger.debug("Caught AttributeError in new_value")
        main()

def new_fuel_status(r):
    logger.debug(r.value)
    try:
        if not r.value[0]:
            raise TypeError
    except TypeError:
        logger.debug("Caught TypeError in new_fuel_status")
        main()
    
    ts = datetime.datetime.utcnow()
    measurement = str(r.command).split(":")[0]
    measurement = measurement.replace(" ", "-")
    
    if "Open loop due to insufficient engine temperature" in r.value:
        fuel_status = 0
    elif "Closed loop, using oxygen sensor feedback to determine fuel mix" in r.value:
        fuel_status = 1
    elif "Open loop due to engine load OR fuel cut due to deceleration" in r.value:
        fuel_status = 2
    elif "Open loop due to system failure" in r.value:
        print("Caught open loop due to system failure")
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
    
    obd.logger.setLevel(obd.logging.DEBUG)
    connection = obd.Async()
    status = connection.status()
    while "Car Connected" not in status:
        connection.close()
        logger.debug("No car connected, sleeping...")
        sleep(1)
        connection = obd.Async()
        status = connection.status()
        
    logger.debug(connection.status())
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
                                        #logger.info(command.name, " supported, watching...")
                                        connection.watch(command, callback=new_fuel_status)
                                    else:
                                        connection.watch(command, callback=new_value)

    connection.start()

    while True:
        sleep(0.5)


if __name__== "__main__":
  main()
