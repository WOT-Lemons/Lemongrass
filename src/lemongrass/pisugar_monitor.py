#!/usr/bin/env python
"""Sends PiSugar measurements to InfluxDB."""

import base64
import json
import logging
import os
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from time import sleep, time

from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from lemongrass import _config, _influx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('pisugar-monitor')

_pisugar_cfg = _config.load_config().pisugar
PISUGAR_API = _pisugar_cfg.api_url
PISUGAR_CONFIG = _pisugar_cfg.config_path
TOKEN_REFRESH_MARGIN = 300  # seconds before expiry to proactively refresh
HTTP_TIMEOUT_S = 5  # a wedged pisugar-server must not hang the monitor forever
STARTUP_RETRY_DELAY_S = 5


def read_credentials():
    """Read PiSugar auth credentials from config file."""
    try:
        with open(PISUGAR_CONFIG, encoding='utf-8') as f:
            config = json.load(f)
        return config.get('auth_user'), config.get('auth_password')
    except (FileNotFoundError, json.JSONDecodeError):
        return None, None


def _resolve_host():
    """Host identity for tagging PiSugar (Pi) telemetry.

    PiSugar data describes the Pi, not the car, so it is tagged by host rather
    than VIN. pisugar.host is set to the Pi's real hostname in the deploy config
    so it matches telegraf's host tag; socket.gethostname() (the container ID
    under Docker) is only a last-resort fallback.
    """
    return _config.load_config().pisugar.host or socket.gethostname()


def login(username, password):
    """Authenticate with pisugar-server and return a session token."""
    params = urllib.parse.urlencode({"username": username, "password": password})
    req = urllib.request.Request(
        f"{PISUGAR_API}/login?{params}",
        data=b"",
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
        return resp.read().decode().strip()


def token_expiry(token):
    """Decode JWT and return the expiry timestamp, or None if unparseable."""
    try:
        payload_b64 = token.split('.')[1]
        payload_b64 += '=' * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return payload.get('exp')
    except Exception:
        return None


def exec_command(command, token=None, coerce=True):
    """Send a command to the PiSugar HTTP API and return the parsed value.

    coerce=False returns the raw string — used for device tags, where numeric
    coercion would mangle version strings like "1.10" into 1.1.
    """
    headers = {"Content-Type": "text/plain"}
    if token:
        headers["x-pisugar-token"] = token
    req = urllib.request.Request(
        f"{PISUGAR_API}/exec",
        data=command.encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
        raw = resp.read().decode().strip()
    _, sep, value = raw.partition(": ")
    if sep:
        raw = value
    if not coerce:
        return raw
    if raw.lower() == "true":
        return True
    if raw.lower() == "false":
        return False
    try:
        return float(raw)
    except ValueError:
        return raw


def _startup_connect(username, password):
    """Log in and read device tags, retrying until pisugar-server responds.

    At Pi boot this service can start before pisugar-server binds :8421; the
    monitor is useless without these reads and startup is idempotent, so retry
    forever rather than die with a traceback.
    """
    while True:
        try:
            token = None
            if username and password:
                token = login(username, password)
                logger.info("Authenticated with pisugar-server")
            tags = {
                "server_version": exec_command("get version", token, coerce=False),
                "model": exec_command("get model", token, coerce=False),
                "firmware_version": exec_command("get firmware_version", token, coerce=False),
            }
            return token, tags
        except Exception:
            logger.exception(
                "pisugar-server not reachable; retrying in %ds", STARTUP_RETRY_DELAY_S)
            sleep(STARTUP_RETRY_DELAY_S)


def build_point(measurement, value, tags=None):
    """Build a single InfluxDB measurement point."""
    point = Point(measurement)
    for k, v in (tags or {}).items():
        point = point.tag(k, v)
    point = point.field("value", value).time(datetime.now(UTC))
    logger.debug(point)
    return point


def write_points(write_api, points):
    """Write all points to InfluxDB in a single request.

    The batch is atomic: if one point is rejected (e.g. a malformed PiSugar
    response yields a non-numeric value for a normally-float field), all six
    readings for this tick are dropped. We don't re-queue on failure because
    the 0.5s loop re-reads fresh values next iteration -- a stale battery
    reading has no value worth preserving.
    """
    try:
        write_api.write(bucket=_influx.BUCKET_PISUGAR, record=points)
        logger.info("Wrote %d points to InfluxDB", len(points))
    except Exception:
        logger.exception("Failed to write %d points to InfluxDB", len(points))


def main():
    """Main loop: read PiSugar metrics and push to InfluxDB."""
    # Validate the influx token up front so we fail fast before the pisugar login
    # below; _influx.connect() reads it again at construction time.
    token_env = _config.load_config().influx.token_env
    if not os.environ.get(token_env):
        logger.error("%s environment variable not set", token_env)
        sys.exit(1)

    username, password = read_credentials()
    pisugar_token, device_tags = _startup_connect(username, password)
    device_tags["host"] = _resolve_host()
    logger.info("Tagging PiSugar telemetry with host=%s", device_tags["host"])
    logger.info("PiSugar device: %s", device_tags)

    with _influx.connect() as influx_client:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        while True:
            if pisugar_token and username and password:
                exp = token_expiry(pisugar_token)
                if exp and time() > exp - TOKEN_REFRESH_MARGIN:
                    logger.info("PiSugar token nearing expiry, refreshing")
                    try:
                        pisugar_token = login(username, password)
                    except Exception:
                        logger.exception("Proactive token refresh failed")

            try:
                charging = exec_command("get battery_charging", pisugar_token)
                current = exec_command("get battery_i", pisugar_token)
                level = exec_command("get battery", pisugar_token)
                plugged = exec_command("get battery_power_plugged", pisugar_token)
                voltage = exec_command("get battery_v", pisugar_token)
                temperature = exec_command("get temperature", pisugar_token)
                write_points(write_api, [
                    build_point("pisugar-battery-charging", charging, device_tags),
                    build_point("pisugar-battery-current", current, device_tags),
                    build_point("pisugar-battery-level", level, device_tags),
                    build_point("pisugar-battery-power-plugged", plugged, device_tags),
                    build_point("pisugar-battery-voltage", voltage, device_tags),
                    build_point("pisugar-temperature", temperature, device_tags),
                ])
            except urllib.error.HTTPError as e:
                if e.code == 401 and username and password:
                    logger.warning("PiSugar token expired, re-authenticating")
                    try:
                        pisugar_token = login(username, password)
                        logger.info("Re-authenticated with pisugar-server")
                    except Exception:
                        logger.exception("Re-authentication failed")
                else:
                    logger.exception("HTTP error reading from PiSugar")
            except Exception:
                logger.exception("Error reading from PiSugar")

            sleep(0.5)


if __name__ == "__main__":
    main()
