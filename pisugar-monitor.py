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


def send_value(influx_client, measurement, value):
  """Send a measurement to InfluxDB."""
  ts = datetime.now(timezone.utc)
  json_body = [{
      "measurement": measurement,
      "time": ts,
      "fields": {"value": value}
      }]
  logger.debug(json_body)
  try:
    influx_client.write_points(json_body)
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

  influx_client = InfluxDBClient('race.focism.com', 8086, 'car_252', influx_pass, 'stats_252')

  pisugar_conn, pisugar_event_conn = pisugar.connect_tcp(socket.gethostname())
  pisugar_server = pisugar.PiSugarServer(pisugar_conn, pisugar_event_conn)

  while True:
    try:
      send_value(influx_client, "pisugar-battery-charging", pisugar_server.get_battery_charging())
      send_value(influx_client, "pisugar-battery-current", pisugar_server.get_battery_current())
      send_value(influx_client, "pisugar-battery-level", pisugar_server.get_battery_level())
      send_value(influx_client, "pisugar-battery-power-plugged", pisugar_server.get_battery_power_plugged())
      send_value(influx_client, "pisugar-battery-voltage", pisugar_server.get_battery_voltage())
      send_value(influx_client, "pisugar-temperature", pisugar_server.get_temperature())
    except Exception:
      logger.exception("Error reading from PiSugar")

    sleep(0.5)


if __name__ == "__main__":
  main()
