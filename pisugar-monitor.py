#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sends PiSugar measurements to InfluxDB."""

import json
import logging
import logging.handlers
import os
import socket
import urllib.error
import urllib.parse
import urllib.request

from datetime import datetime, timezone
from time import sleep
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('pisugar-monitor')

syslogHandler = logging.handlers.SysLogHandler(
    address=('localhost', 6514),
    facility='user',
    socktype=socket.SOCK_DGRAM
    )

logger.addHandler(syslogHandler)

PISUGAR_API = "http://localhost:8421"
PISUGAR_CONFIG = "/etc/pisugar-server/config.json"


def read_credentials():
  try:
    with open(PISUGAR_CONFIG) as f:
      config = json.load(f)
    return config.get('auth_user'), config.get('auth_password')
  except (FileNotFoundError, json.JSONDecodeError):
    return None, None


def login(username, password):
  params = urllib.parse.urlencode({"username": username, "password": password})
  req = urllib.request.Request(
      f"{PISUGAR_API}/login?{params}",
      data=b"",
      method="POST",
      )
  with urllib.request.urlopen(req) as resp:
    return resp.read().decode().strip()


def exec_command(command, token=None):
  headers = {"Content-Type": "text/plain"}
  if token:
    headers["x-pisugar-token"] = token
  req = urllib.request.Request(
      f"{PISUGAR_API}/exec",
      data=command.encode(),
      headers=headers,
      method="POST",
      )
  with urllib.request.urlopen(req) as resp:
    raw = resp.read().decode().strip()
  _, sep, value = raw.partition(": ")
  if sep:
    raw = value
  if raw.lower() == "true":
    return True
  if raw.lower() == "false":
    return False
  try:
    return float(raw)
  except ValueError:
    return raw


def send_value(write_api, measurement, value, tags=None):
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
  influx_token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
  if not influx_token:
    logger.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
    return

  pisugar_token = None
  username, password = read_credentials()
  if username and password:
    try:
      pisugar_token = login(username, password)
      logger.info("Authenticated with pisugar-server")
    except urllib.error.HTTPError as e:
      if e.code == 404:
        logger.info("pisugar-server auth not enabled, proceeding unauthenticated")
      else:
        raise

  with InfluxDBClient(url='https://influxdb.focism.com', token=influx_token, org='focism') as influx_client:
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    device_tags = {
        "server_version": exec_command("get version", pisugar_token),
        "model": exec_command("get model", pisugar_token),
        "firmware_version": exec_command("get firmware_version", pisugar_token),
        }
    logger.info("PiSugar device: %s", device_tags)

    while True:
      try:
        charging = exec_command("get battery_charging", pisugar_token)
        current = exec_command("get battery_i", pisugar_token)
        level = exec_command("get battery", pisugar_token)
        plugged = exec_command("get battery_power_plugged", pisugar_token)
        voltage = exec_command("get battery_v", pisugar_token)
        temperature = exec_command("get temperature", pisugar_token)
        send_value(write_api, "pisugar-battery-charging", charging, device_tags)
        send_value(write_api, "pisugar-battery-current", current, device_tags)
        send_value(write_api, "pisugar-battery-level", level, device_tags)
        send_value(write_api, "pisugar-battery-power-plugged", plugged, device_tags)
        send_value(write_api, "pisugar-battery-voltage", voltage, device_tags)
        send_value(write_api, "pisugar-temperature", temperature, device_tags)
      except Exception:
        logger.exception("Error reading from PiSugar")

      sleep(0.5)


if __name__ == "__main__":
  main()
