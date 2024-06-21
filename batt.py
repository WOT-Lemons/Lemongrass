#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pprint import pprint
from influxdb import InfluxDBClient
import os
import sys
import time
from time import sleep
import datetime
import socket
import logging
import logging.handlers
from power_api import SixfabPower

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('batt')

syslogHandler = logging.handlers.SysLogHandler(address=('localhost', 6514), facility='user', socktype=socket.SOCK_DGRAM)
#stdoutHandler = logging.StreamHandler(sys.stdout)

logger.addHandler(syslogHandler)
api = SixfabPower()
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

client = InfluxDBClient('race.focism.com', 8086, 'car_252', influx_pass, 'stats_252')

def sendValue(measurement, value):

    ts = datetime.datetime.utcnow()

    json_body = [
    {
        "measurement": measurement,
        "time": ts,
        "fields": {
                "value": value
            }
    }]
    print(json_body)
    client.write_points(json_body)


def main():

    while True:
        sendValue("input-temp", api.get_input_temp())
        sendValue("input-voltage", api.get_input_voltage())
        sendValue("input-current", api.get_input_current())
        sendValue("input-power", api.get_input_power())
        sendValue("system-temp", api.get_system_temp())
        sendValue("system-voltage", api.get_system_voltage())
        sendValue("system-current", api.get_system_current())
        sendValue("system-power", api.get_system_power())
        sendValue("battery-temp", api.get_battery_temp())
        sendValue("battery-voltage", api.get_battery_voltage())
        sendValue("battery-current", api.get_battery_current())
        sendValue("battery-power", api.get_battery_power())
        sendValue("battery-level", api.get_battery_level())
        sendValue("battery-health", api.get_battery_health())
        sendValue("fan-health", api.get_fan_health())
        sendValue("fan-speed", api.get_fan_speed())
        sendValue("working-mode", api.get_working_mode())

        sleep(0.5)

if __name__== "__main__":
  main()
