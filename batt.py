#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sends power measurements to InfluxDB."""

import logging
import logging.handlers
import os
import socket

from datetime import datetime, timezone
from time import sleep
from influxdb import InfluxDBClient
from power_api import SixfabPower

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('batt')

syslogHandler = logging.handlers.SysLogHandler(
    address=('localhost', 6514),
    facility='user',
    socktype=socket.SOCK_DGRAM
    )
# stdoutHandler = logging.StreamHandler(sys.stdout)

logger.addHandler(syslogHandler)
# logger.addHandler(stdoutHandler)

# Configure Sixfab Power API
api = SixfabPower()
api.set_fan_mode(1)
api.set_battery_design_capacity(3400)
api.set_battery_max_charge_level(80)

# Load tokenfile
if os.path.exists('/home/pi/.influxcred'):
  with open('/home/pi/.influxcred', 'r', encoding='utf-8') as f:
    influx_pass = f.readline().rstrip()
    if influx_pass != "":
      logger.debug("Influx cred opened and read")
    else:
      logger.debug("Failed to open ~/.influxcred")

client = InfluxDBClient('race.focism.com', 8086, 'car_252', influx_pass, 'stats_252')


def send_value(measurement, value):
  """Function that sends a measurement to InfluxDB."""
  ts = datetime.now(timezone.utc)

  json_body = [{
      "measurement": measurement,
      "time": ts,
      "fields": {"value": value}
      }]
  print(json_body)
  client.write_points(json_body)


def main():
  """Main loop of metrics collection."""
  while True:
    send_value("battery-current", api.get_battery_current())
    send_value("battery-design-capacity", api.get_battery_design_capacity())
    send_value("battery-health", api.get_battery_health())
    send_value("battery-level", api.get_battery_level())
    send_value("battery-max-charge-level", api.get_battery_max_charge_level())
    send_value("battery-power", api.get_battery_power())
    send_value("battery-temp", api.get_battery_temp())
    send_value("battery-voltage", api.get_battery_voltage())
    send_value("fan-health", api.get_fan_health())
    send_value("fan-mode", api.get_fan_mode())
    send_value("fan-speed", api.get_fan_speed())
    send_value("input-current", api.get_input_current())
    send_value("input-power", api.get_input_power())
    send_value("input-temp", api.get_input_temp())
    send_value("input-voltage", api.get_input_voltage())
    send_value("system-current", api.get_system_current())
    send_value("system-power", api.get_system_power())
    send_value("system-temp", api.get_system_temp())
    send_value("system-voltage", api.get_system_voltage())
    send_value("working-mode", api.get_working_mode())

    sleep(0.5)


if __name__ == "__main__":
  main()
