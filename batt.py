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
        InputTemp = api.get_input_temp()
        sendValue("input-temp", InputTemp)

        InputVoltage = api.get_input_voltage()
        sendValue("input-voltage", InputVoltage)

        InputCurrent = api.get_input_current()
        sendValue("input-current", InputCurrent)

        InputPower = api.get_input_power()
        sendValue("input-power", InputPower)

        SystemTemp = api.get_system_temp()
        sendValue("system-temp", SystemTemp)

        SystemVoltage = api.get_system_voltage()
        sendValue("system-voltage", SystemVoltage)

        SystemCurrent = api.get_system_current()
        sendValue("system-current", SystemCurrent)

        SystemPower = api.get_system_power()
        sendValue("system-power", SystemPower)

        BatteryTemp = api.get_battery_temp()
        sendValue("battery-temp", BatteryTemp)

        BatteryVoltage = api.get_battery_voltage()
        sendValue("battery-voltage", BatteryVoltage)

        BatteryCurrent = api.get_battery_current()
        sendValue("battery-current", BatteryCurrent)

        BatteryPower = api.get_battery_power()
        sendValue("battery-power", BatteryPower)

        BatteryLevel = api.get_battery_level()
        sendValue("battery-level", BatteryLevel)

        BatteryHealth = api.get_battery_health()
        sendValue("battery-health", BatteryHealth)

        FanHealth = api.get_fan_health()
        sendValue("fan-health", FanHealth)

        FanSpeed = api.get_fan_speed()
        sendValue("fan-speed", FanSpeed)

        WorkingMode = api.get_working_mode()
        sendValue("working-mode", WorkingMode)

        sleep(0.5)

if __name__== "__main__":
  main()
