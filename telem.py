#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sends OBD-II measurements to InfluxDB."""

from datetime import datetime, timezone
import logging
import logging.handlers
import os
import socket
from time import sleep

import obd
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('telem')

syslogHandler = logging.handlers.SysLogHandler(
    address=('localhost', 6514),
    facility='user',
    socktype=socket.SOCK_DGRAM
    )

logger.addHandler(syslogHandler)


# Load tokenfile
if os.path.exists('/home/pi/.influxcred'):
  f = open('/home/pi/.influxcred', 'r', encoding='utf-8')
  influx_pass = f.readline().rstrip()
  if influx_pass != "":
    logger.debug("Influx cred opened and read")
  else:
    logger.debug("Failed to open ~/.influxcred")

client = InfluxDBClient('race.focism.com', 8086, 'car_252', influx_pass, 'stats_252')


def new_value(r):
  """Store new measurement in InfluxDB."""
  ts = datetime.now(timezone.utc)
  try:
    measurement = str(r.command).split(":")[1]
    measurement = measurement.replace(" ", "-")
  except IndexError:
    logger.debug("Caught IndexError in new_value")
    main()
  try:
    json_body = [{
        "measurement": measurement,
        "time": ts,
        "fields": {"value": r.value.magnitude}
        }]
    client.write_points(json_body)
  except TypeError:
    logger.debug("Caught TypeError in new_value")
    main()
  except AttributeError:
    logger.debug("Caught AttributeError in new_value")
    main()


def new_fuel_status(r):
  """Store new fuel status in InfluxDB."""
  logger.debug(r.value)
  try:
    if not r.value[0]:
      raise TypeError
  except TypeError:
    logger.debug("Caught TypeError in new_fuel_status")
    main()

  ts = datetime.now(timezone.utc)
  measurement = str(r.command).split(":", maxsplit=1)[0]
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

  json_body = [{
      "measurement": measurement,
      "time": ts,
      "fields": {
          "value": fuel_status
          }
      }]
  client.write_points(json_body)


def main():
  """Main loop of OBD-II scraping"""
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

  for command in supported_commands:
    excluded_patterns = ["DTC", "MIDS", "PIDS", "O2_SENSORS", "ELM", "OBD"]
    if not any(pattern in command.name for pattern in excluded_patterns):
      if command.name != "STATUS":
        if "FUEL_STATUS" in command.name:
          # logger.info(f"{command.name} supported, watching...")
          connection.watch(
              command, callback=new_fuel_status)
        else:
          connection.watch(
              command, callback=new_value)

  try:
    connection.watch(obd.commands.ELM_VOLTAGE, callback=new_value)
  except (AttributeError, KeyError):
    logger.warning("Could not find voltage monitoring command - skipping")

  connection.start()

  while True:
    sleep(0.5)


if __name__ == "__main__":
  main()
