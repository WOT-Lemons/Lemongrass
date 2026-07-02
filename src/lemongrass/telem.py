#!/usr/bin/env python
"""Sends OBD-II measurements to InfluxDB."""

import logging
import os
import threading
from datetime import datetime, timezone
from time import sleep

import obd
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from lemongrass import _influx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('telem')

EXCLUDED_PATTERNS = ["DTC", "MIDS", "PIDS", "O2_SENSORS", "ELM", "OBD"]

FUEL_STATUS_MAP = {
    "Open loop due to insufficient engine temperature": 0,
    "Closed loop, using oxygen sensor feedback to determine fuel mix": 1,
    "Open loop due to engine load OR fuel cut due to deceleration": 2,
    "Open loop due to system failure": 3,
    "Closed loop, using at least one oxygen sensor but there is a fault in the feedback system": 4,
}

AIR_STATUS_MAP = {
    "Upstream": 0,
    "Downstream of catalytic converter": 1,
    "From the outside atmosphere or off": 2,
    "Pump commanded on for diagnostics": 3,
}

pending_points = []
pending_lock = threading.Lock()

_connection = None  # set by main(); lets new_status trigger on-demand DTC lookups
_last_dtc_count = 0  # only ever written from the Async callback thread; no lock needed


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
    try:
        measurement = str(r.command).split(":")[1]
        measurement = measurement.replace(" ", "-")
    except IndexError:
        logger.debug("Caught IndexError in new_fuel_status")
        return

    fuel_status = next((v for k, v in FUEL_STATUS_MAP.items() if k in r.value), 255)
    if fuel_status == 3:
        logger.warning("Caught open loop due to system failure")

    with pending_lock:
        pending_points.append(
            Point(measurement).field("value", fuel_status).time(ts)
        )


def new_air_status(r):
    """Queue new secondary air status for batch write to InfluxDB."""
    if not r.value:
        logger.debug("Caught falsy value in new_air_status")
        return

    ts = datetime.now(timezone.utc)
    try:
        measurement = str(r.command).split(":")[1]
        measurement = measurement.replace(" ", "-")
    except IndexError:
        logger.debug("Caught IndexError in new_air_status")
        return

    air_status = AIR_STATUS_MAP.get(r.value, 255)

    with pending_lock:
        pending_points.append(
            Point(measurement).field("value", air_status).time(ts)
        )


def _query_fuel_type_once(connection):
    """Query FUEL_TYPE once and queue a single point (it never changes mid-session)."""
    if not connection.supports(obd.commands.FUEL_TYPE):
        return

    # Async.query() only returns cached watched-command values; FUEL_TYPE is
    # deliberately never watched, so the blocking OBD.query() is called directly.
    r = obd.OBD.query(connection, obd.commands.FUEL_TYPE, force=True)
    if not r.value:
        logger.debug("Caught falsy value in _query_fuel_type_once")
        return

    ts = datetime.now(timezone.utc)
    try:
        measurement = str(r.command).split(":")[1]
        measurement = measurement.replace(" ", "-")
    except IndexError:
        logger.debug("Caught IndexError in _query_fuel_type_once")
        return

    with pending_lock:
        pending_points.append(
            Point(measurement).field("value", r.value).time(ts)
        )


def _fetch_and_store_dtcs():
    """Force a live GET_DTC read and queue the active codes as one point."""
    # Async.query() only returns cached watched-command values, and GET_DTC is
    # deliberately never watched (see _route_command), so the blocking
    # OBD.query() is called directly -- the same technique Async's own
    # background thread uses internally (asynchronous.py's run() method).
    # Safe here because this only ever runs from inside an Async callback,
    # which already executes on that same background thread.
    if _connection is None:
        return

    r = obd.OBD.query(_connection, obd.commands.GET_DTC, force=True)
    if r.value is None:
        logger.debug("Caught null value in _fetch_and_store_dtcs")
        return

    codes = ",".join(code for code, _ in r.value)
    ts = datetime.now(timezone.utc)
    with pending_lock:
        pending_points.append(Point("-Get-DTCs").field("value", codes).time(ts))


def new_status(r):
    """Queue MIL and DTC count for batch write to InfluxDB."""
    global _last_dtc_count

    if r.value is None:
        logger.debug("Caught null value in new_status")
        return

    ts = datetime.now(timezone.utc)
    try:
        measurement = str(r.command).split(":")[1]
        measurement = measurement.replace(" ", "-")
    except IndexError:
        logger.debug("Caught IndexError in new_status")
        return

    with pending_lock:
        pending_points.append(
            Point(f"{measurement}-MIL").field("value", int(r.value.MIL)).time(ts)
        )
        pending_points.append(
            Point(f"{measurement}-DTC-Count").field("value", r.value.DTC_count).time(ts)
        )

    # Only the primary STATUS command drives the on-demand DTC lookup;
    # STATUS_DRIVE_CYCLE just records its own MIL/count.
    if r.command.name != "STATUS":
        return

    dtc_count = r.value.DTC_count
    if dtc_count > _last_dtc_count:
        _fetch_and_store_dtcs()
    _last_dtc_count = dtc_count


def connect():
    """Open an OBD-II connection on the configured serial port.

    Defaults to the ``/dev/obd`` udev symlink (passed into the container via a
    device mapping). ``OBD_PORT`` overrides it for host-based testing.
    """
    return obd.Async(portstr=os.environ.get('OBD_PORT', '/dev/obd'))


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
    with _influx.connect() as influx_client:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        obd.logger.setLevel(obd.logging.DEBUG)
        connection = connect()
        status = connection.status()
        while "Car Connected" not in status:
            connection.close()
            logger.info("No car connected, sleeping...")
            sleep(1)
            connection = connect()
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
