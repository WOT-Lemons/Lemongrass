#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sends OBD-II measurements to InfluxDB."""

from datetime import datetime, timezone
import logging
import logging.handlers
import os
import socket
import threading
from time import sleep

import obd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('telem')

syslogHandler = logging.handlers.SysLogHandler(
    address=('localhost', 6514),
    facility='user',
    socktype=socket.SOCK_DGRAM
    )

logger.addHandler(syslogHandler)

EXCLUDED_PATTERNS = ["DTC", "MIDS", "PIDS", "O2_SENSORS", "ELM", "OBD"]

FUEL_STATUS_MAP = {
    "Open loop due to insufficient engine temperature": 0,
    "Closed loop, using oxygen sensor feedback to determine fuel mix": 1,
    "Open loop due to engine load OR fuel cut due to deceleration": 2,
    "Open loop due to system failure": 3,
    "Closed loop, using at least one oxygen sensor but there is a fault in the feedback system": 4,
    }

pending_points = []
pending_lock = threading.Lock()


def new_value(r):
  """Queue new measurement for batch write to InfluxDB."""
  ts = datetime.now(timezone.utc)
  try:
    measurement = str(r.command).split(":")[1]
    measurement = measurement.replace(" ", "-")
  except IndexError:
    logger.debug("Caught IndexError in new_value")
    return
  try:
    point = Point(measurement).field("value", r.value.magnitude).time(ts)
  except TypeError:
    logger.debug("Caught TypeError in new_value")
    return
  except AttributeError:
    logger.debug("Caught AttributeError in new_value")
    return
  with pending_lock:
    pending_points.append(point)


def new_fuel_status(r):
  """Queue new fuel status for batch write to InfluxDB."""
  logger.debug(r.value)
  try:
    if not r.value[0]:
      raise TypeError
  except TypeError:
    logger.debug("Caught TypeError in new_fuel_status")
    return

  ts = datetime.now(timezone.utc)
  measurement = str(r.command).split(":", maxsplit=1)[0]
  measurement = measurement.replace(" ", "-")

  fuel_status = next((v for k, v in FUEL_STATUS_MAP.items() if k in r.value), 255)
  if fuel_status == 3:
    logger.warning("Caught open loop due to system failure")

  with pending_lock:
    pending_points.append(
        Point(measurement).field("value", fuel_status).time(ts)
        )


def flush_points(write_api):
  """Write all pending points to InfluxDB in a single request."""
  with pending_lock:
    if not pending_points:
      return
    batch = pending_points.copy()
    pending_points.clear()
  try:
    write_api.write(bucket='stats_252/autogen', record=batch)
    logger.info("Flushed %d points to InfluxDB", len(batch))
  except Exception as e:
    logger.error('Failed to write %d points to InfluxDB: %s', len(batch), e)
    with pending_lock:
      pending_points[:0] = batch


def main():
  """Main loop of OBD-II scraping"""
  influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
  if not influx_token:
    logger.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
    return

  with InfluxDBClient(url='https://influxdb.focism.com', token=influx_token, org='focism') as influx_client:
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    obd.logger.setLevel(obd.logging.DEBUG)
    connection = obd.Async()
    status = connection.status()
    while "Car Connected" not in status:
      connection.close()
      logger.info("No car connected, sleeping...")
      sleep(1)
      connection = obd.Async()
      status = connection.status()

    logger.debug(connection.status())

    for command in connection.supported_commands:
      if any(pattern in command.name for pattern in EXCLUDED_PATTERNS):
        continue
      if command.name == "STATUS":
        continue
      if "FUEL_STATUS" in command.name:
        connection.watch(command, callback=new_fuel_status)
      else:
        connection.watch(command, callback=new_value)

    try:
      connection.watch(obd.commands.ELM_VOLTAGE, callback=new_value)
    except (AttributeError, KeyError):
      logger.warning("Could not find voltage monitoring command - skipping")

    connection.start()

    while True:
      sleep(0.5)
      flush_points(write_api)


if __name__ == "__main__":
  main()
