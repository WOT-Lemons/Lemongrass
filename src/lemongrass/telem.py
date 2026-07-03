#!/usr/bin/env python
"""Sends OBD-II measurements to InfluxDB."""

import logging
import os
import sys
import threading
from datetime import datetime, timezone
from time import monotonic, sleep

import obd
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from lemongrass import _influx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('telem')

EXCLUDED_PATTERNS = ["MIDS", "PIDS", "O2_SENSORS", "ELM", "OBD"]
SKIP_NAMES = {"FREEZE_DTC", "GET_DTC", "GET_CURRENT_DTC", "CLEAR_DTC", "FUEL_TYPE"}
MAX_DTC_FETCH_FAILURES = 3

# Backlog bound for InfluxDB outages: callbacks keep queueing while writes fail;
# beyond this many points the oldest are dropped rather than OOM-ing the Pi.
MAX_PENDING_POINTS = 50_000
FLUSH_BATCH_SIZE = 5_000
STATUS_COMMANDS = {"STATUS", "STATUS_DRIVE_CYCLE"}

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
# Both only ever written from the Async callback thread; no lock needed.
_last_dtc_count = 0
_dtc_fetch_failures = 0

# Watchdog: exit (for a supervisor restart) when no data has arrived this long.
STALE_DATA_TIMEOUT_S = 60
_last_append_monotonic = 0.0


def _queue_point(point):
    """Append a point to the pending batch and mark data as flowing."""
    global _last_append_monotonic
    with pending_lock:
        pending_points.append(point)
        _last_append_monotonic = monotonic()


def _measurement_name(r):
    """Derive the InfluxDB measurement name from a response's command string."""
    try:
        return str(r.command).split(":")[1].replace(" ", "-")
    except IndexError:
        return None


def new_value(r):
    """Queue new measurement for batch write to InfluxDB."""
    ts = datetime.now(timezone.utc)
    measurement = _measurement_name(r)
    if measurement is None:
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
    _queue_point(point)


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
    measurement = _measurement_name(r)
    if measurement is None:
        logger.debug("Caught IndexError in new_fuel_status")
        return

    fuel_status = FUEL_STATUS_MAP.get(r.value[0], 255)
    if fuel_status == 3:
        logger.warning("Caught open loop due to system failure")

    _queue_point(Point(measurement).field("value", fuel_status).time(ts))


def new_air_status(r):
    """Queue new secondary air status for batch write to InfluxDB."""
    if not r.value:
        logger.debug("Caught falsy value in new_air_status")
        return

    ts = datetime.now(timezone.utc)
    measurement = _measurement_name(r)
    if measurement is None:
        logger.debug("Caught IndexError in new_air_status")
        return

    air_status = AIR_STATUS_MAP.get(r.value, 255)

    _queue_point(Point(measurement).field("value", air_status).time(ts))


def _query_fuel_type_once(connection):
    """Query FUEL_TYPE once and queue a single point (it never changes mid-session)."""
    if not connection.supports(obd.commands.FUEL_TYPE):
        return

    # Async.query() only returns cached watched-command values; FUEL_TYPE is
    # deliberately never watched, so the blocking OBD.query() is called directly.
    try:
        r = obd.OBD.query(connection, obd.commands.FUEL_TYPE, force=True)
    except Exception:
        logger.exception("Fuel-type query failed; continuing without it")
        return
    if not r.value:
        logger.debug("Caught falsy value in _query_fuel_type_once")
        return

    ts = datetime.now(timezone.utc)
    measurement = _measurement_name(r)
    if measurement is None:
        logger.debug("Caught IndexError in _query_fuel_type_once")
        return

    _queue_point(Point(measurement).field("value", r.value).time(ts))


def _fetch_and_store_dtcs():
    """Force a live GET_DTC read and queue the active codes as one point.

    Returns True if a snapshot was queued, False otherwise, so callers know
    whether it's safe to advance their own notion of the last-seen DTC count.
    """
    # Async.query() only returns cached watched-command values, and GET_DTC is
    # deliberately never watched (see _route_command), so the blocking
    # OBD.query() is called directly -- the same technique Async's own
    # background thread uses internally (asynchronous.py's run() method).
    # Safe here because this only ever runs from inside an Async callback,
    # which already executes on that same background thread.
    if _connection is None:
        return False

    try:
        r = obd.OBD.query(_connection, obd.commands.GET_DTC, force=True)
    except Exception:
        logger.exception("GET_DTC query failed; skipping this fetch")
        return False
    if r.value is None:
        logger.debug("Caught null value in _fetch_and_store_dtcs")
        return False

    # An empty list is a successful mode-03 answer ("no stored codes"), not a
    # failure -- store it so a STATUS/GET_DTC disagreement doesn't retry forever.
    codes = ",".join(code for code, _ in r.value)
    ts = datetime.now(timezone.utc)
    _queue_point(Point("-Get-DTCs").field("value", codes).time(ts))
    return True


def _route_command(command):
    """Return the callback to watch a supported command with, or None to skip it."""
    if any(pattern in command.name for pattern in EXCLUDED_PATTERNS):
        return None
    if command.name in SKIP_NAMES:
        return None
    # python-obd marks a DTC_-prefixed mode-2 freeze-frame twin as supported for
    # every supported mode-1 PID; watching them would double per-cycle load.
    if command.name.startswith("DTC_"):
        return None
    if command.name in STATUS_COMMANDS:
        return new_status
    if command.name == "AIR_STATUS":
        return new_air_status
    if "FUEL_STATUS" in command.name:
        return new_fuel_status
    return new_value


def new_status(r):
    """Queue MIL and DTC count for batch write to InfluxDB."""
    global _last_dtc_count, _dtc_fetch_failures

    if r.value is None:
        logger.debug("Caught null value in new_status")
        return

    ts = datetime.now(timezone.utc)
    measurement = _measurement_name(r)
    if measurement is None:
        logger.debug("Caught IndexError in new_status")
        return

    _queue_point(Point(f"{measurement}-MIL").field("value", int(r.value.MIL)).time(ts))
    _queue_point(
        Point(f"{measurement}-DTC-Count").field("value", r.value.DTC_count).time(ts)
    )

    # Only the primary STATUS command drives the on-demand DTC lookup;
    # STATUS_DRIVE_CYCLE just records its own MIL/count.
    if r.command.name != "STATUS":
        return

    dtc_count = r.value.DTC_count
    if dtc_count > _last_dtc_count:
        if _fetch_and_store_dtcs():
            _last_dtc_count = dtc_count
            _dtc_fetch_failures = 0
        else:
            # Each forced query costs a blocking serial round-trip on the Async
            # thread; give up after a few misses rather than retry every STATUS.
            _dtc_fetch_failures += 1
            if _dtc_fetch_failures >= MAX_DTC_FETCH_FAILURES:
                logger.warning(
                    "Giving up on DTC fetch after %d failures", _dtc_fetch_failures
                )
                _last_dtc_count = dtc_count
                _dtc_fetch_failures = 0
    else:
        _last_dtc_count = dtc_count


def connect():
    """Open an OBD-II connection on the configured serial port.

    Defaults to the ``/dev/obd`` udev symlink (passed into the container via a
    device mapping). ``OBD_PORT`` overrides it for host-based testing.
    """
    return obd.Async(portstr=os.environ.get('OBD_PORT', '/dev/obd'))


def _configure_obd_logging():
    """Enable python-obd DEBUG logging only when OBD_DEBUG is set.

    DEBUG logs every serial send/receive; with dozens of watched PIDs cycling
    continuously that is sustained journald I/O and SD-card wear on the Pi.
    """
    if os.environ.get('OBD_DEBUG'):
        obd.logger.setLevel(obd.logging.DEBUG)


def _connection_healthy(connection):
    """False when the OBD link is gone or no data has arrived recently.

    python-obd's Async loop keeps running after the adapter browns out or the
    ignition cycles, delivering only null responses — every callback then
    early-returns and telemetry silently stops. Both signals are checked: the
    reported connection status, and time since a callback last queued a point.
    """
    if not connection.is_connected():
        logger.error("OBD connection lost")
        return False
    if monotonic() - _last_append_monotonic > STALE_DATA_TIMEOUT_S:
        logger.error("No OBD data received for %ds", STALE_DATA_TIMEOUT_S)
        return False
    return True


def flush_points(write_api, batch_size=FLUSH_BATCH_SIZE):
    """Write all pending points to InfluxDB in batches of batch_size.

    On a failed write the unwritten remainder is re-queued, capped at
    MAX_PENDING_POINTS with the oldest dropped, so an extended outage cannot
    grow the backlog without bound — and recovery after the outage goes out in
    bounded requests instead of one giant write.
    """
    with pending_lock:
        if not pending_points:
            return
        batch = pending_points.copy()
        pending_points.clear()
    written = 0
    try:
        for i in range(0, len(batch), batch_size):
            write_api.write(bucket='stats_252/autogen', record=batch[i:i + batch_size])
            written += len(batch[i:i + batch_size])
        logger.info("Flushed %d points to InfluxDB", written)
    except Exception as e:
        unwritten = batch[written:]
        logger.error('Failed to write %d points to InfluxDB: %s', len(unwritten), e)
        with pending_lock:
            pending_points[:0] = unwritten
            overflow = len(pending_points) - MAX_PENDING_POINTS
            if overflow > 0:
                del pending_points[:overflow]
                logger.warning(
                    "Backlog exceeded %d points; dropped %d oldest",
                    MAX_PENDING_POINTS, overflow)


def main():
    """Main loop of OBD-II scraping"""
    global _connection, _last_append_monotonic

    with _influx.connect() as influx_client:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        _configure_obd_logging()
        connection = connect()
        status = connection.status()
        while "Car Connected" not in status:
            connection.close()
            logger.info("No car connected, sleeping...")
            sleep(1)
            connection = connect()
            status = connection.status()

        logger.debug(connection.status())

        _connection = connection
        _query_fuel_type_once(connection)

        for command in connection.supported_commands:
            callback = _route_command(command)
            if callback is not None:
                connection.watch(command, callback=callback)

        try:
            connection.watch(obd.commands.ELM_VOLTAGE, callback=new_value)
        except (AttributeError, KeyError):
            logger.warning("Could not find voltage monitoring command - skipping")

        connection.start()
        _last_append_monotonic = monotonic()

        while True:
            sleep(0.5)
            flush_points(write_api)
            if not _connection_healthy(connection):
                # Exit nonzero so the container/systemd restart policy re-runs
                # the well-tested startup connect sequence; flush what's left.
                flush_points(write_api)
                logger.error("Exiting for supervisor restart")
                sys.exit(1)


if __name__ == "__main__":
    main()
