"""Entry point for the lemongrass CLI dispatcher."""
import importlib
import json
import sys

from influxdb_client.rest import ApiException
from race_monitor import RaceMonitorError, RaceMonitorHTTPError
from urllib3.exceptions import HTTPError

# _influx is imported lazily inside main(): importing it loads the config file,
# and a malformed LEMONGRASS_CONFIG must not crash the dispatcher (or --help)
# with a traceback before it can report the error cleanly.
from lemongrass._config import ConfigError, warn_dropped_env_vars

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
        # status 0: the rest layer wraps a urllib3 SSLError as ApiException(status=0),
        # which is a connectivity failure, not a reached-but-rejected request.
        return exc.status in (None, 0) or exc.status >= 500
    return True  # a urllib3 HTTPError is a connection-level failure


def _format_influx_error(exc):
    """Return a one-line human reason for an InfluxDB connection/API failure."""
    if isinstance(exc, ApiException):
        detail = None
        if exc.body:
            try:
                payload = json.loads(exc.body)
                # InfluxDB 2.x API errors use "message"; the Cloudflare 530 tunnel
                # page uses "detail"/"title". Prefer whichever is present.
                detail = (payload.get('message') or payload.get('detail')
                          or payload.get('title'))
            except (ValueError, TypeError):
                detail = None
        # Bodyless ApiExceptions (e.g. status-0 SSL/connection failures) carry
        # their text in .reason, not the body; fall back to it so we don't print
        # a bare "HTTP 0". Collapse whitespace to keep the message one line.
        if not detail and exc.reason:
            detail = ' '.join(str(exc.reason).split())
        msg = f"HTTP {exc.status}"
        if detail:
            msg += f": {detail}"
        return msg
    return str(exc) or exc.__class__.__name__


def _report_race_monitor_error(exc):
    """Print a one-line reason for a RaceMonitor API failure (no traceback).

    race-monitor 0.7.0 bounds 429 retries by max_retries and raises
    RaceMonitorHTTPError once they're exhausted instead of retrying forever, so
    the CLI must surface rate-limit exhaustion cleanly rather than crashing."""
    if isinstance(exc, RaceMonitorHTTPError) and exc.status_code == 429:
        print("Error: RaceMonitor rate limit exceeded (HTTP 429) — retries "
              "exhausted. Try again in a minute.", file=sys.stderr)
    elif isinstance(exc, RaceMonitorHTTPError):
        print(f"Error: RaceMonitor request failed (HTTP {exc.status_code})",
              file=sys.stderr)
        if exc.body:
            print(f"  {' '.join(str(exc.body).split())}", file=sys.stderr)
    else:
        print(f"Error: RaceMonitor client error: {exc}", file=sys.stderr)


def main():
    """Dispatch to the subcommand named by the first CLI argument."""
    warn_dropped_env_vars()

    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        print("Usage: lemongrass <command> [args]")
        print(f"Commands: {', '.join(_COMMANDS)}")
        sys.exit(0)

    if len(sys.argv) == 1 and sys.stdin.isatty() and sys.stdout.isatty():
        import logging

        from race_monitor import RaceMonitorClient

        from lemongrass import _env
        from lemongrass._env import resolve_tokens
        logging.basicConfig(level=logging.INFO)
        tokens = resolve_tokens()
        if not tokens:
            print(f"{_env.tokens_env_hint()} not set", file=sys.stderr)
            sys.exit(1)
        from lemongrass._home_tui import run_home_tui
        with RaceMonitorClient(api_token=tokens) as client:
            sys.exit(run_home_tui(client))

    if len(sys.argv) < 2 or sys.argv[1] not in _COMMANDS:
        print("Usage: lemongrass <command> [args]")
        print(f"Commands: {', '.join(_COMMANDS)}")
        sys.exit(1)

    cmd = sys.argv.pop(1)
    sys.argv[0] = f"lemongrass-{cmd}"
    # Two HTTP clients can fail here: the InfluxDB client (ApiException/HTTPError)
    # and the RaceMonitor client (RaceMonitorError). Report either cleanly (no
    # traceback); for Influx, distinguish "unreachable" from a reached-but-rejected
    # request.
    try:
        result = importlib.import_module(_COMMANDS[cmd]).main()
    except ConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except (ApiException, HTTPError) as exc:
        from lemongrass import _influx
        if _influx_unreachable(exc):
            print(f"Error: cannot reach InfluxDB at {_influx.INFLUX_URL}", file=sys.stderr)
        else:
            print(f"Error: InfluxDB request failed at {_influx.INFLUX_URL}", file=sys.stderr)
        print(f"  {_format_influx_error(exc)}", file=sys.stderr)
        sys.exit(1)
    except RaceMonitorError as exc:
        _report_race_monitor_error(exc)
        sys.exit(1)
    sys.exit(result)
