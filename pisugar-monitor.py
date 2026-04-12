#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sends PiSugar measurements to InfluxDB."""

import logging
import logging.handlers
import os
import socket

from datetime import datetime, timezone
from time import sleep
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pisugar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('pisugar-monitor')

syslogHandler = logging.handlers.SysLogHandler(
    address=('localhost', 6514),
    facility='user',
    socktype=socket.SOCK_DGRAM
    )

logger.addHandler(syslogHandler)


def send_value(write_api, measurement, value):
  """Send a measurement to InfluxDB."""
  ts = datetime.now(timezone.utc)
  point = Point(measurement).field("value", value).time(ts)
  logger.debug(point)
  try:
    write_api.write(bucket='stats_252/autogen', record=point)
  except Exception:
    logger.exception("Failed to write %s to InfluxDB", measurement)


def main():
  """Main loop of metrics collection."""
  if os.path.exists('/home/pi/.influxcred'):
    with open('/home/pi/.influxcred', 'r', encoding='utf-8') as f:
      influx_pass = f.readline().rstrip()
    if influx_pass:
      logger.debug("Influx cred opened and read")
    else:
      logger.error("Failed to read ~/.influxcred")
      return
  else:
    logger.error("~/.influxcred not found")
    return

  with InfluxDBClient(url='https://influxdb.focism.com', token=influx_pass, org='focism') as influx_client:
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    pisugar_conn, pisugar_event_conn = pisugar.connect_tcp(socket.gethostname())
    pisugar_server = pisugar.PiSugarServer(pisugar_conn, pisugar_event_conn)

    while True:
      try:
        send_value(write_api, "pisugar-battery-charging", pisugar_server.get_battery_charging())
        send_value(write_api, "pisugar-battery-current", pisugar_server.get_battery_current())
        send_value(write_api, "pisugar-battery-level", pisugar_server.get_battery_level())
        send_value(write_api, "pisugar-battery-power-plugged", pisugar_server.get_battery_power_plugged())
        send_value(write_api, "pisugar-battery-voltage", pisugar_server.get_battery_voltage())
        send_value(write_api, "pisugar-temperature", pisugar_server.get_temperature())
      except Exception:
        logger.exception("Error reading from PiSugar")

      sleep(0.5)


if __name__ == "__main__":
  main()
