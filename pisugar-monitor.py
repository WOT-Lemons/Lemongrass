#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sends PiSugar measurements to InfluxDB."""

import logging
import logging.handlers
import os
import socket

from datetime import datetime, timezone
from time import sleep
from influxdb import InfluxDBClient
import pisugar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('pisugar-monitor')

syslogHandler = logging.handlers.SysLogHandler(
    address=('localhost', 6514),
    facility='user',
    socktype=socket.SOCK_DGRAM
    )

logger.addHandler(syslogHandler)

# Configure PiSugar Server API
pisugar_conn, pisugar_event_conn = pisugar.connect_tcp(socket.gethostname())
pisugar_server = pisugar.PiSugarServer(pisugar_conn, pisugar_event_conn)

# Load tokenfile
if os.path.exists('/home/pi/.influxcred'):
  with open('/home/pi/.influxcred', 'r', encoding='utf-8') as f:
    influx_pass = f.readline().rstrip()
    if influx_pass != "":
      logger.debug("Influx cred opened and read")
    else:
      logger.debug("Failed to open ~/.influxcred")

influx_client = InfluxDBClient('race.focism.com', 8086, 'car_252', influx_pass, 'stats_252')


def send_value(measurement, value):
  """Function that sends a measurement to InfluxDB."""
  ts = datetime.now(timezone.utc)

  json_body = [{
      "measurement": measurement,
      "time": ts,
      "fields": {"value": value}
      }]
  print(json_body)
  influx_client.write_points(json_body)


def main():
  """Main loop of metrics collection."""
  while True:
    send_value("pisugar-battery-charging", pisugar_server.get_battery_charging())
    send_value("pisugar-battery-current", pisugar_server.get_battery_current())
    send_value("pisugar-battery-level", pisugar_server.get_battery_level())
    send_value("pisugar-battery-power-plugged", pisugar_server.get_battery_power_plugged())
    send_value("pisugar-battery-voltage", pisugar_server.get_battery_voltage())
    send_value("pisugar-temperature", pisugar_server.get_temperature())

    sleep(0.5)


if __name__ == "__main__":
  main()
