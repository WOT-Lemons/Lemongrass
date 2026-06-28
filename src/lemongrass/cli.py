"""Entry point for the lemongrass CLI dispatcher."""
import importlib
import json
import sys

from influxdb_client.rest import ApiException
from urllib3.exceptions import HTTPError

from lemongrass import _influx

_COMMANDS = {
    "laps": "lemongrass.laps",
    "race-backfill": "lemongrass.race_backfill",
    "races": "lemongrass.races",
    "telem": "lemongrass.telem",
    "pisugar-monitor": "lemongrass.pisugar_monitor",
    "race-diagnose": "lemongrass.race_diagnose",
}


def _influx_unreachable(exc):
    """True if exc means InfluxDB could not be reached (connection failure or a
    5xx/upstream error), False if the server was reached but rejected the request
    (a 4xx such as 401/403/404)."""
    if isinstance(exc, ApiException):
        return exc.status is None or exc.status >= 500
    return True  # a urllib3 HTTPError is a connection-level failure


def _format_influx_error(exc):
    """Return a one-line human reason for an InfluxDB connection/API failure."""
    if isinstance(exc, ApiException):
        detail = None
        if exc.body:
            try:
                payload = json.loads(exc.body)
                detail = payload.get('detail') or payload.get('title')
            except (ValueError, TypeError):
                detail = None
        msg = f"HTTP {exc.status}"
        if detail:
            msg += f": {detail}"
        return msg
    return str(exc) or exc.__class__.__name__


def main():
    """Dispatch to the subcommand named by the first CLI argument."""
    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        print("Usage: lemongrass <command> [args]")
        print(f"Commands: {', '.join(_COMMANDS)}")
        sys.exit(0)

    if len(sys.argv) < 2 or sys.argv[1] not in _COMMANDS:
        print("Usage: lemongrass <command> [args]")
        print(f"Commands: {', '.join(_COMMANDS)}")
        sys.exit(1)

    cmd = sys.argv.pop(1)
    sys.argv[0] = f"lemongrass-{cmd}"
    # The only urllib3/HTTP traffic in these commands is the InfluxDB client, so
    # any ApiException/HTTPError is an InfluxDB failure. Report it cleanly (no
    # traceback), distinguishing "unreachable" from a reached-but-rejected request.
    try:
        importlib.import_module(_COMMANDS[cmd]).main()
    except (ApiException, HTTPError) as exc:
        if _influx_unreachable(exc):
            print(f"Error: cannot reach InfluxDB at {_influx.INFLUX_URL}", file=sys.stderr)
        else:
            print(f"Error: InfluxDB request failed at {_influx.INFLUX_URL}", file=sys.stderr)
        print(f"  {_format_influx_error(exc)}", file=sys.stderr)
        sys.exit(1)
