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


def send_value(write_api, measurement, value, tags=None):
  """Send a measurement to InfluxDB."""
  ts = datetime.now(timezone.utc)
  point = Point(measurement)
  for k, v in (tags or {}).items():
    point = point.tag(k, v)
  point = point.field("value", value).time(ts)
  logger.debug(point)
  try:
    write_api.write(bucket='stats_252/autogen', record=point)
    logger.info("Wrote %s: %s", measurement, value)
  except Exception:
    logger.exception("Failed to write %s to InfluxDB", measurement)


def main():
  """Main loop of metrics collection."""
  influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
  if not influx_token:
    logger.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
    return

  with InfluxDBClient(url='https://influxdb.focism.com', token=influx_token, org='focism') as influx_client:
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    pisugar_conn, pisugar_event_conn = pisugar.connect_tcp(socket.gethostname())
    pisugar_server = pisugar.PiSugarServer(pisugar_conn, pisugar_event_conn)

    device_tags = {
      "server_version": pisugar_server.get_version(),
      "model": pisugar_server.get_model(),
      "firmware_version": pisugar_server.get_firmware_version(),
    }
    logger.info("PiSugar device: %s", device_tags)

    while True:
      try:
        send_value(write_api, "pisugar-battery-charging", pisugar_server.get_battery_charging(), device_tags)
        send_value(write_api, "pisugar-battery-current", pisugar_server.get_battery_current(), device_tags)
        send_value(write_api, "pisugar-battery-level", pisugar_server.get_battery_level(), device_tags)
        send_value(write_api, "pisugar-battery-power-plugged", pisugar_server.get_battery_power_plugged(), device_tags)
        send_value(write_api, "pisugar-battery-voltage", pisugar_server.get_battery_voltage(), device_tags)
        send_value(write_api, "pisugar-temperature", pisugar_server.get_temperature(), device_tags)
      except Exception:
        logger.exception("Error reading from PiSugar")

      sleep(0.5)


if __name__ == "__main__":
  main()
