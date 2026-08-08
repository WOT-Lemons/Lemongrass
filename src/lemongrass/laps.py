#!/usr/bin/env python
"""Interact with the RaceMonitor lap timing system."""
#
# Timestamp anchoring (design decision):
#   Live mode anchors lap timestamps on Race['StartDateEpoc']; the historical view anchors on
#   SessionStartDateEpoc, so the two disagree by up to ~35 min. Aligning the live anchor was
#   ruled out: the live API never exposes SessionStartDateEpoc, and the live feed's cumulative
#   offset differs from historical, so even the same anchor would not produce matching points.
#   Instead, historical is treated as the source of truth: a post-race network-mode run
#   (old_race) deletes the tracked car's lap points and rewrites the complete historical set
#   (correct timestamps + class_position on every lap) via delete_existing_laps. Live/monitor
#   writes are the during-race approximation; the backfill makes the final record authoritative.

import argparse
import csv
import enum
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from race_monitor import RaceMonitorClient, get_streaming_command

from lemongrass import _config, _env, _influx
from lemongrass._env import resolve_tokens

UNDERLINE = "-" * 80

EPOCH_START = '1970-01-01T00:00:00Z'

# Version of the lap write/normalization schema. Stamped on every lap point
# written by the historical backfill (push_influx) and used by --skip-if-complete
# to decide whether a race's existing laps are current.
#
# When to bump: increment this whenever the way laps are written or normalized
# changes such that previously-written laps would now come out differently —
# e.g. a new/renamed field, a changed flag-status mapping, a timestamp-anchoring
# fix, or any other change to the lap point shape or values.
#
# Effect of bumping: the next backfill run with --skip-if-complete will treat all
# previously-written races as stale (their stamp no longer matches) and re-backfill
# them, rewriting historical data under the new schema. That "rewrite everything"
# behavior is itself a useful migration tool — bump the version and re-run the
# backfill to bring all historical races up to the current schema.
SCHEMA_VERSION = 5

_WRITE_BATCH_SIZE = 5000
_LIVE_CHECK_INTERVAL = 5

# Cadence for the pre-race waits (wait_for_live / wait_for_car). 10s is 6 checks
# per minute — exactly one RaceMonitor token's default rate-limit budget, so a
# multi-hour wait never starves the pool. The client's own limiter blocks rather
# than 429s if other calls are in flight, which self-throttles the wait.
_WAIT_POLL_S = 10

# Ticks between a wait's periodic actions: the "still waiting" log line and the
# in-wait is_live recheck. 30 ticks at the 10s cadence is once every 5 minutes.
_WAIT_RECHECK_TICKS = 30

# How far in the past a race's stored end time must be before the Influx-only skip
# will trust it as fully over. Sessions can share one race_id across a multi-day
# event, so a stored end that is only recently past may still have a later session
# going live; only skip once the whole event is settled well behind us. Anything
# more recent falls through to the authoritative is_live RaceMonitor check.
_SETTLED_BUFFER_S = 4 * 86400  # 4 days


@dataclass(frozen=True)
class StoredRace:
    """Race-completeness fields read back from a stored race point.

    Any field is None when absent from the point (schema_version and
    expected_lap_count predate the fields they name; end_time_epoc is missing
    only from a malformed point) — callers must guard against None before
    comparing them.
    """
    schema_version: int | None
    expected_lap_count: int | None
    end_time_epoc: int | None


def _describe_bad_value(value: object, field: str) -> str:
    """Return a log string distinguishing known streaming tokens from random garbage."""
    cmd = get_streaming_command(value)
    if cmd is not None:
        return (
            f"streaming command token {value!r} ({cmd.name}) in {field} field"
            " — known API quirk, not data corruption"
        )
    return f"unparseable {field} value {value!r}"


class MonitorStatus(enum.Enum):
    """Return values from monitor_routine indicating how polling ended."""

    RACE_ENDED = "race_ended"
    INTERRUPTED = "interrupted"
    NO_LIVE_DATA = "no_live_data"
    WRITE_FAILED = "write_failed"


@dataclass
class RaceMetadata:
    """Race-level metadata resolved once at startup."""
    race_name: str
    track_name: str
    series_name: str | None
    end_time_epoc: int


@dataclass
class RaceContext:
    """Fixed context for a run: race identity, API client, and optional InfluxDB handle."""
    race_id: str
    car_number: str | None
    client: object
    write_api: object
    start_epoc: int
    metadata: RaceMetadata | None = None
    delete_api: object = None
    query_api: object = None


@dataclass
class RaceOptions:
    """User-configured behaviour flags, built directly from CLI args."""
    network_mode: bool = False
    monitor_mode: bool = False
    save_file: bool = False
    selected_class: str | None = None
    interval: int = 30
    skip_if_complete: bool = False
    dry_run: bool = False
    wait_for_live: bool = False


class RaceObserver:
    """Sink for live-race display events.

    The live path (live_race / monitor_routine) calls these instead of printing
    directly, so a front-end can render however it likes. The base class is an
    inert default (every method a no-op); _StdoutObserver reproduces the
    terminal output, and the TUI supplies its own widget-driving observer.
    """

    def on_rankings(self, sorted_competitors, race_live, selected_class, categories):
        """The one-time rankings header shown at live-race start."""

    def on_live_detail(self, competitor_details, class_name, class_position):
        """The tracked competitor's detail block."""

    def on_laps(self, laps):
        """Display a full lap list (initial seed / reseed)."""

    def on_lap(self, lap):
        """A single newly-arrived lap for the tracked car."""

    def on_session_change(self, session_name):
        """The live session changed."""

    def on_standings(self, session_response):
        """A refreshed live-session response for the whole field."""

    def on_status(self, text):
        """A human-readable status line/block."""

    def on_race_ended(self):
        """The race ended naturally."""


class _StdoutObserver(RaceObserver):
    """Default observer: reproduces the historical terminal output."""

    def on_rankings(self, sorted_competitors, race_live, selected_class, categories):
        print_rankings(sorted_competitors, race_live, selected_class, categories)

    def on_live_detail(self, competitor_details, class_name, class_position):
        print(UNDERLINE)
        print(
            f"Team: {competitor_details['Name']:<6} "
            f"Car Number: {competitor_details['Number']:<4} "
            f"Class: {class_name} "
            f"Transponder: {competitor_details['Transponder']}"
        )
        print(
            f"Best Position:\t{competitor_details['BestPosition']:>}\n"
            f"Final Position:\t{competitor_details['Position']:>}\n"
            f"Final Class Position:\t"
            f"{class_position if class_position is not None else 'N/A':>}\n"
            f"Total Laps:\t{competitor_details['Laps']:>}\n"
            f"Best Lap:\t{competitor_details['BestLap']:>}\n"
            f"Best Lap Time:\t{competitor_details['BestLapTime']:>}\n"
            f"Total Time:\t{competitor_details['TotalTime']:>}"
        )
        print(UNDERLINE)

    def on_laps(self, laps):
        # Table only — each call site emits its own UNDERLINE (live_race prints
        # it after the table, monitor_routine before), so one shared method can
        # match both call sites exactly.
        print(pandas.json_normalize(laps).to_string(index=False))

    def on_lap(self, lap):
        print(pandas.json_normalize(lap).to_string(index=False, header=False))

    def on_session_change(self, session_name):
        print(f"\nNew session: {session_name}")

    def on_status(self, text):
        print(text)

    def on_race_ended(self):
        print("Race has ended.")


def _build_parser():
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description='Interact with lap data',
        epilog='Run with no arguments in a terminal to open the interactive TUI: '
               'search for a race, then watch it live or import a completed race. '
               'Passing a race_id (or any non-terminal invocation) uses the '
               'scripted behavior below.')
    parser.add_argument('race_id', metavar='race_id', nargs=1, type=int, action='store')
    parser.add_argument('car_number', metavar='car_number', nargs='?', type=int, default=None)
    parser.add_argument('-c', '--class', metavar='A/B/C', dest='selected_class', nargs='?',
                        type=ascii, action='store', help='Group or filter by class (A/B/C)')
    parser.add_argument('-m', '--monitor', dest='monitor_mode', default=False,
                        action='store_true', help='Update when new data received')
    parser.add_argument('-n', '--network', dest='network_mode', default=False,
                        action='store_true', help='Forward lap data via influx')
    parser.add_argument(
        '-o',
        '--out',
        dest='save_file',
        default=False,
        action='store_true',
        help='Write lap times to CSV')
    parser.add_argument('--skip-if-complete', dest='skip_if_complete', default=False,
                        action='store_true',
                        help='Skip the backfill if this race already has all fieldwide laps '
                             'written under the current schema version (historical -n mode only)')
    parser.add_argument('--dry-run', dest='dry_run', default=False,
                        action='store_true',
                        help='Implies -n; show what would be written without touching InfluxDB '
                             '(historical mode only)')
    parser.add_argument('-v', '--verbose', help="Set debug logging", action='store_true')
    parser.add_argument(
        '--interval',
        dest='interval',
        default=30,
        type=int,
        metavar='SECONDS',
        help='Polling interval in seconds for monitor mode (default: 30)')
    parser.add_argument('--wait-for-live', dest='wait_for_live', default=False,
                        action='store_true',
                        help='Implies -m; wait for the race to go live (checked every '
                             '10s, no timeout), then monitor car_number. If the car is '
                             'not in the live feed yet, keep waiting for it — unless the '
                             'race ends first, which stops the wait and exits nonzero.')
    parser.set_defaults(monitor_mode=False, network_mode=False)
    return parser


def _parse_args(argv=None):
    """Parse CLI args and enforce the cross-flag rules argparse can't express.

    --wait-for-live is a live-only prelude to monitoring: it turns on monitor
    mode the way --dry-run implies -n, and it cannot coexist with either
    historical-only flag. parser.error() exits 2 with usage, as argparse does
    for any other bad invocation.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.wait_for_live:
        if args.dry_run:
            parser.error('--wait-for-live cannot be combined with --dry-run '
                         '(--dry-run is historical-only)')
        if args.skip_if_complete:
            parser.error('--wait-for-live cannot be combined with --skip-if-complete '
                         '(--skip-if-complete is historical-only)')
        if args.car_number is None:
            parser.error('--wait-for-live requires car_number')
        args.monitor_mode = True
    return args


def main():
    """Parse arguments and orchestrate race data retrieval."""
    # Bare `laps` on a terminal launches the interactive TUI. Any positional
    # args (race_id …) or a non-TTY invocation fall through unaffected to the
    # existing argument-parsed behavior below, so scripts, cron, and
    # race-backfill's in-process calls are unaffected. logging.basicConfig is
    # scoped to this branch only — it must not be called unconditionally here,
    # since the later basicConfig(format=...) calls below are no-ops once the
    # root logger already has a handler, which would silently change the
    # scripted path's log format. The textual import stays lazy for
    # non-interactive runs.
    if len(sys.argv) == 1 and sys.stdin.isatty() and sys.stdout.isatty():
        from lemongrass._laps_tui import run_laps_tui
        from lemongrass._tui import launch_tui
        launch_tui(run_laps_tui)

    args = _parse_args()

    if args.verbose:
        print(args)
        logging.basicConfig(
            level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(
            level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Pandas default max rows truncating lap times. I don't expect a team to do more than 1024 laps.
    pandas.set_option("display.max_rows", 1024)

    race_id = str(args.race_id[0])
    car_number = str(args.car_number) if args.car_number is not None else None

    # Validate user-supplied identifiers before touching RaceMonitor/Influx —
    # race_id and car_number are interpolated into Flux delete predicates and
    # queries below, so an unsafe value must not reach token resolution.
    ids_to_check = [race_id] if car_number is None else [race_id, car_number]
    bad_ids = _influx.invalid_flux_ids(ids_to_check)
    if bad_ids:
        logging.error("invalid identifier(s): %s", ", ".join(f'"{b}"' for b in bad_ids))
        sys.exit(1)

    tokens = resolve_tokens()
    if not tokens:
        logging.error("%s environment variable not set", _env.tokens_env_hint())
        sys.exit(1)

    # Validate the influx token up front so we fail fast before the RaceMonitor
    # setup work below; _influx.connect() reads it again at construction time.
    influx_token_env = _config.load_config().influx.token_env
    if args.network_mode and not args.dry_run and not os.environ.get(influx_token_env):
        logging.error("%s environment variable not set", influx_token_env)
        sys.exit(1)

    opts = RaceOptions(
        network_mode=args.network_mode or args.dry_run,
        monitor_mode=args.monitor_mode,
        save_file=args.save_file,
        selected_class=args.selected_class,
        interval=args.interval,
        skip_if_complete=args.skip_if_complete,
        dry_run=args.dry_run,
        wait_for_live=args.wait_for_live,
    )

    # Fast path: for the historical backfill (--skip-if-complete), decide whether
    # to skip entirely from Influx before making any RaceMonitor call. Gated on
    # skip_if_complete (set only by race-backfill), so interactive and monitor
    # runs never enter here; stored_end_settled keeps any possibly-live race on
    # the normal is_live path. To force a re-backfill past this skip (e.g. a race
    # whose lap count was revised after it was stored), use
    # `race-backfill --upgrade-stored [--force]`, which re-runs laps without
    # --skip-if-complete and so never enters this branch.
    if opts.network_mode and opts.skip_if_complete and not opts.dry_run \
            and _influx_only_skip(race_id):
        logging.info(
            "SKIP: race %s already complete and current "
            "(from Influx, no RaceMonitor fetch)", race_id)
        return 0

    try:
        with RaceMonitorClient(api_token=tokens) as client:
            return backfill_race(race_id, car_number, client, opts)
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)


def _live_session(session_response):
    """Return the session dict from a ``client.live.get_session`` response, or None.

    A truthy ``Successful`` does not guarantee a session: RaceMonitor answers
    ``{'Successful': True, 'Session': None}`` while a live race sits between
    sessions — including the gap before a scheduled race's first session opens —
    and the client raises on a falsy ``Successful``, so that null is the only
    shape a "no session right now" poll can take. It is not a signal that the
    race is over; only ``race.is_live`` (see ``_race_ended``) decides that. Every
    consumer must treat it like an unsuccessful response rather than indexing
    ``['Session']``.
    """
    if not session_response.get('Successful'):
        return None
    return session_response.get('Session') or None


def _race_ended(client, race_id):
    """Has the race definitively ended? True, False, or None for unknown.

    Every wait loop in the CLI and the TUI needs this same decision, and the
    unknown case is the subtle one: a raised exception or a response without
    'Successful' means the *check* failed, not that the race is over, and a
    transient blip must never kill a multi-hour unattended capture. Only an
    explicit successful-and-not-live response ends a wait.
    """
    try:
        response = client.race.is_live(race_id)
    except Exception as exc:
        logging.debug("is_live check failed; treating as unknown: %s", exc)
        return None
    if not response.get('Successful'):
        return None
    return not response.get('IsLive')


def wait_for_live(client, race_id, stop_event=None, on_tick=None, poll_s=_WAIT_POLL_S):
    """Poll race.is_live until the race goes live.

    Returns True once IsLive is set, or False if stop_event was set first. The
    wait has no timeout: unattended capture must still be running whenever the
    green flag actually drops, which can be hours after setup.

    Every failure of the is_live call — RaceMonitorError (including exhausted
    429 retries), transport errors, anything unexpected — is swallowed and
    retried on the next tick, because a single bad poll must not end the wait.
    on_tick(count, error) fires after each check that did not find the race
    live, where error is None on a clean not-yet-live result and a short reason
    otherwise; callers use it to render status.
    """
    stop = stop_event if stop_event is not None else threading.Event()
    count = 0
    while not stop.is_set():
        error = None
        try:
            response = client.race.is_live(race_id)
            if response.get('Successful') and response.get('IsLive'):
                return True
        except Exception as exc:
            logging.debug("is_live check failed while waiting: %s", exc)
            error = str(exc) or exc.__class__.__name__
        count += 1
        if on_tick is not None:
            on_tick(count, error)
        if stop.wait(timeout=poll_s):
            return False
    return False


def wait_for_car(client, race_id, car_number, stop_event=None, on_tick=None,
                 poll_s=_WAIT_POLL_S):
    """Poll live.get_session until car_number appears in the live field.

    A race going live does not mean its timing feed is populated — cars trickle
    in — so this is a second, separate wait after wait_for_live. Returns True
    once the number matches a competitor's Number, or False if stop_event was
    set first. Like wait_for_live it never gives up: an unsuccessful response, a
    null session (the normal shape before the first session of the day opens),
    an empty field, a populated field without the car, and an exception are all
    just "not yet".

    on_tick(count, state, error) fires after each miss. state is 'no_feed' when
    there is nothing to match against and 'absent' when the field is populated
    but the car is not in it — the two look identical to the API caller but mean
    very different things to someone watching the screen.
    """
    stop = stop_event if stop_event is not None else threading.Event()
    wanted = str(car_number)
    count = 0
    while not stop.is_set():
        error = None
        state = 'no_feed'
        try:
            response = client.live.get_session(race_id)
            session = _live_session(response)
            competitors = (session or {}).get('Competitors') or {}
            if competitors:
                state = 'absent'
                if any(str(comp.get('Number', '')) == wanted
                       for comp in competitors.values()):
                    return True
        except Exception as exc:
            logging.debug("get_session failed while waiting for car: %s", exc)
            error = str(exc) or exc.__class__.__name__
        count += 1
        if on_tick is not None:
            on_tick(count, state, error)
        if stop.wait(timeout=poll_s):
            return False
    return False


def _wait_for_live_log_tick(race_id, poll_s):
    """Build an on_tick(count, error) callback that reports wait_for_live's
    progress at INFO, so an unattended CLI run under systemd/docker isn't
    silent between launch and the green flag.

    Logs a start line immediately, then roughly one "still waiting" line every
    5 minutes at the 10s cadence (first tick, then every 30th). A failing
    check is always logged the first time it's seen, but a repeated identical
    error is not re-logged on every tick — only a change in the error is.
    """
    logging.info("Waiting for race %s to go live (checking every %ss)…", race_id, poll_s)
    last_error = None

    def _tick(count, error):
        nonlocal last_error
        if count == 1 or count % _WAIT_RECHECK_TICKS == 0:
            logging.info(
                "Still waiting for race %s to go live (%d checks so far)", race_id, count)
        if error is not None and error != last_error:
            logging.info(
                "is_live check failing while waiting for race %s: %s", race_id, error)
        last_error = error

    return _tick


def _wait_for_car_log_tick(race_id, car_number, poll_s):
    """Build an on_tick(count, state, error) callback that reports
    wait_for_car's progress at INFO — see _wait_for_live_log_tick for the
    cadence/error-dedup rules, which are the same here.

    state distinguishes 'no_feed' (nothing to match against yet) from 'absent'
    (the field is populated but car_number isn't in it), the same distinction
    the TUI's wait screen surfaces.
    """
    logging.info(
        "Waiting for car %s to appear in race %s's timing feed (checking every %ss)…",
        car_number, race_id, poll_s)
    last_error = None

    def _tick(count, state, error):
        nonlocal last_error
        if count == 1 or count % _WAIT_RECHECK_TICKS == 0:
            if state == 'no_feed':
                logging.info(
                    "Still waiting for the timing feed for race %s (%d checks so far)",
                    race_id, count)
            else:
                logging.info(
                    "Car %s still not in race %s's field (%d checks so far)",
                    car_number, race_id, count)
        if error is not None and error != last_error:
            logging.info(
                "get_session check failing while waiting for car %s: %s", car_number, error)
        last_error = error

    return _tick


def backfill_race(race_id, car_number, client, opts, observer=None):
    """Fetch and process one race using an already-open RaceMonitorClient.

    Extracted from main() so a batch caller (race-backfill --upgrade-stored) can
    reuse a single client — and therefore a single process-wide rate-limiter
    window — across many races, instead of paying a fresh rate-limit window per
    subprocess and overrunning the server's per-token budget at each boundary.

    Returns an int exit status (0 ok, non-zero failure). KeyboardInterrupt (and
    any RaceMonitorError, e.g. 429 exhaustion) propagates to the caller so a
    batch loop can decide whether to stop or record-and-continue.
    """
    # Wait before fetching details: a scheduled-but-not-started race reports a
    # placeholder start epoch and no metadata, so everything downstream should
    # read post-green-flag values. Returning 0 on a cancelled wait keeps a
    # user-initiated stop from looking like a failure.
    if opts.wait_for_live and not wait_for_live(
            client, race_id, on_tick=_wait_for_live_log_tick(race_id, _WAIT_POLL_S)):
        return 0

    race_details = client.race.details(race_id)

    start_epoc = 0
    if race_details['Successful']:
        race_name = race_details['Race']['Name']
        start_epoc = race_details['Race']['StartDateEpoc']
        logging.debug("StartDateEpoc: %s", start_epoc)
        start_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_epoc))
        end_epoc = race_details['Race']['EndDateEpoc']
        end_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_epoc))
        race_track = race_details['Race']['Track']
        print(UNDERLINE)
        print(f"Race {race_id}")
        print(
            f"{race_name}\tStarted: {start_date:>}\n"
            f"{race_track}\t\t\tEnds: {end_date:>}"
        )
        print(UNDERLINE)

    metadata = _resolve_race_metadata(race_details, client) if opts.network_mode else None

    if opts.selected_class:
        logging.info("Sorting results for class %s.", opts.selected_class.upper())

    response = client.race.is_live(race_id)

    is_live = response.get('Successful') and response.get('IsLive')
    if car_number is None and (opts.monitor_mode or is_live):
        logging.error("car_number is required for live/monitor mode")
        return 1
    if car_number is not None and not is_live and not opts.monitor_mode:
        logging.warning("car_number provided but ignored in historical fieldwide mode")

    if not response['Successful']:
        return 1

    if not opts.network_mode:
        return _run_race(
            RaceContext(race_id, car_number, client, None, start_epoc, metadata=metadata),
            opts, response, observer=observer)

    if opts.dry_run:
        return _run_race(
            RaceContext(race_id, car_number, client, None, start_epoc, metadata=metadata),
            opts, response, observer=observer)

    with _influx.connect() as influx_client:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        delete_api = influx_client.delete_api()
        query_api = influx_client.query_api()
        return _run_race(
            RaceContext(race_id, car_number, client, write_api, start_epoc,
                        metadata=metadata, delete_api=delete_api,
                        query_api=query_api), opts, response, observer=observer)


def _run_race(ctx, opts, response, observer=None):
    """Dispatch to live_race or old_race based on race status."""
    try:
        if response['IsLive'] is not True:
            logging.info("Race %s is not live. Monitor mode disabled.", ctx.race_id)
            if opts.monitor_mode:
                return 0
            return old_race(ctx, opts) or 0
        else:
            logging.info("Race %s is currently live.", ctx.race_id)
            if opts.dry_run:
                logging.error(
                    "--dry-run is historical-only; race %s is live", ctx.race_id)
                return 1
            result = live_race(ctx, opts, observer=observer)
            attempts = 0
            # wait_for_car has no timeout of its own, so a car that never
            # appears at all (a typo'd number, or the race red-flagged before
            # it ever got out) would otherwise leave the loop blocked inside
            # it forever — the per-attempt recheck below only runs at the top
            # of each retry, before wait_for_car is entered. ended and its
            # on_tick are built once, right before the retry loop actually
            # starts, so the "waiting for car" start line isn't re-logged and
            # last_error dedup holds across every attempt.
            ended = threading.Event()
            car_tick = None
            if result is MonitorStatus.NO_LIVE_DATA and opts.wait_for_live:
                log_tick = _wait_for_car_log_tick(ctx.race_id, ctx.car_number, _WAIT_POLL_S)

                def car_tick(count, state, error):
                    log_tick(count, state, error)
                    # Re-check is_live from inside the wait itself, periodically:
                    # the car may never appear at all, so this is the only thing
                    # that ends a wait that outlives the race it's waiting on.
                    if (count % _WAIT_RECHECK_TICKS == 0
                            and _race_ended(ctx.client, ctx.race_id)):
                        logging.error(
                            "Race %s ended before car %s appeared in the live feed",
                            ctx.race_id, ctx.car_number)
                        ended.set()

            while result is MonitorStatus.NO_LIVE_DATA and opts.wait_for_live:
                # Wait for the *car* to show up in the timing feed, then retry
                # the whole live setup. get_session (wait_for_car) and
                # get_racer (live_race) can disagree, so a car listed in the
                # field but with no racer detail yet must not spin: pace every
                # retry after the first.
                if attempts:
                    time.sleep(_WAIT_POLL_S)
                attempts += 1
                # The race can end (or be red-flagged to a finish) before the
                # car ever appears in the feed — without this check the loop
                # would poll a rate-limited API forever with no recovery short
                # of SIGINT.
                if _race_ended(ctx.client, ctx.race_id):
                    logging.error(
                        "Race %s ended before car %s appeared in the live feed",
                        ctx.race_id, ctx.car_number)
                    return 1
                logging.info(
                    "Car %s is not in the live feed yet — waiting for it to appear.",
                    ctx.car_number)
                if not wait_for_car(ctx.client, ctx.race_id, ctx.car_number, ended,
                                    on_tick=car_tick):
                    break
                result = live_race(ctx, opts, observer=observer)
            if result is MonitorStatus.INTERRUPTED:
                sys.exit(130)
            if result is MonitorStatus.NO_LIVE_DATA:
                return 1
            if result is MonitorStatus.WRITE_FAILED:
                return 1
        return 0
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)


def live_race(ctx, opts, observer=None, _stop_event=None):
    """Handle a live race: fetch the current session, print rankings and racer detail,
    optionally write lap points to InfluxDB, and optionally launch monitor_routine
    to poll for new laps until the race ends or the user interrupts."""
    if observer is None:
        observer = _StdoutObserver()

    # A failed session fetch costs the session tag and the class name for this
    # launch, not the launch itself — monitor_routine already treats the same
    # failure as one skipped poll and resolves both on a later one. Falling back
    # to the unsuccessful-response sentinel keeps every downstream reader on the
    # one path that already handles "no session data".
    try:
        session_response = ctx.client.live.get_session(ctx.race_id)
    except Exception as exc:
        logging.warning(
            "Session fetch failed for race %s (%s); starting without session "
            "detail — the monitor loop will resolve it", ctx.race_id, exc)
        session_response = {'Successful': False}

    live_session_id = None
    live_session_name = None
    session = _live_session(session_response)
    if session is not None:
        live_session_id = session.get('ID')
        live_session_name = session.get('Name')

    observer.on_rankings([], True, opts.selected_class, {})

    # Get lap times from live racer. A car that is not in the feed comes back as
    # a raised RaceMonitorError, not as a falsy 'Successful': the client raises on
    # any unsuccessful response, so that key is always True by the time we see it.
    # Every wait-for-car retry keys on NO_LIVE_DATA, so the exception has to be
    # turned into that status here or an unattended capture dies at the green flag
    # — the exact moment cars are still trickling into the timing feed. Transport
    # errors are swallowed the same way for the same reason, matching the waits and
    # the monitor loop, which already treat any single failed call as "not yet".
    logging.debug("Getting lap times for %s from race %s.", ctx.car_number, ctx.race_id)
    try:
        response = ctx.client.live.get_racer(ctx.race_id, ctx.car_number)
    except Exception as exc:
        logging.error(
            "No live data for car %s in race %s (%s) — check that the car number "
            "is registered in the live feed", ctx.car_number, ctx.race_id, exc)
        return MonitorStatus.NO_LIVE_DATA

    details = response.get('Details') or {}
    competitor_details = details.get('Competitor') or {}
    if not competitor_details:
        logging.error(
            "No live data for car %s in race %s — check that the car number is "
            "registered in the live feed", ctx.car_number, ctx.race_id)
        return MonitorStatus.NO_LIVE_DATA

    # An empty lap list is not an absent car: a competitor registered before the
    # green flag has completed zero laps. Monitoring must start anyway, or the
    # retry loop would wait for a car that is already sitting on the grid.
    laps = details.get('Laps') or []

    first = competitor_details.get('FirstName', '')
    last = competitor_details.get('LastName', '')
    competitor_details['Name'] = f"{first} {last}".strip()
    competitor_name = competitor_details['Name'] or None
    car_info = competitor_details.get('AdditionalData') or None
    class_name, class_position = _resolve_class_live(session_response, ctx.car_number)

    observer.on_live_detail(competitor_details, class_name, class_position)

    observer.on_laps(laps)
    observer.on_status(UNDERLINE)

    race_meta_written = True
    if opts.network_mode:
        race_ts_ms = ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
        race_meta_written = push_influx_race(ctx, race_ts_ms)
        if live_session_id is not None:
            push_influx_session(ctx, live_session_id, live_session_name, None)
        if laps:
            # class_position intentionally discarded: historical laps were completed before
            # launch so any position we compute now is stale. monitor_routine owns
            # class_position writes. class_name was resolved above from session_response.
            logging.info("Car %s: class %r", ctx.car_number, class_name)
            push_influx(ctx, laps, False, competitor_name=competitor_name, car_info=car_info,
                        class_name=class_name, class_positions=None, session_id=live_session_id)
        push_influx_standings_live(ctx, session_response, live_session_id)

    # Seed the leaderboard immediately regardless of network mode; otherwise a
    # non-network run shows a blank leaderboard until monitor_routine's first poll.
    observer.on_standings(session_response)

    if opts.save_file:
        # Create filename and call function to write to CSV
        filename = f"{competitor_details['Name']}-{ctx.race_id}"
        write_csv(filename, laps)

    if opts.monitor_mode:
        return monitor_routine(ctx, laps, opts, competitor_name=competitor_name, car_info=car_info,
                               session_id=live_session_id, race_meta_written=race_meta_written,
                               observer=observer, _stop_event=_stop_event)

    if opts.network_mode and not race_meta_written:
        # No monitor loop to retry in; signal a failed run so a rerun restores
        # the (possibly deleted) race metadata point.
        return MonitorStatus.WRITE_FAILED


def old_race(ctx, opts):
    """Handle a completed race: fetch all sessions and accumulate lap points for every
    competitor across the full field (fieldwide backfill). Each lap point is tagged with
    session_id. After gathering the complete expected lap count, decides whether to skip
    (already complete and current schema), or delete existing laps and rewrite them all
    in one pass. Also the write path for lemongrass race-backfill --upgrade-stored."""
    logging.debug("Getting sessions for race for %s", ctx.race_id)
    race_details = ctx.client.results.sessions_for_race(ctx.race_id)

    session_ids_for_race = [s['ID'] for s in race_details.get('Sessions', [])]

    if not session_ids_for_race:
        logging.warning(
            "No sessions found for race %s — nothing to display or write", ctx.race_id)
        return

    logging.debug(
        "Race %s has %s sessions, %s",
        ctx.race_id, len(session_ids_for_race), session_ids_for_race)

    if not opts.network_mode:
        # Display mode prints only the final session's rankings; fetching the
        # earlier sessions would spend RaceMonitor rate limit on discarded data.
        session_ids_for_race = session_ids_for_race[-1:]

    pending_writes = []

    # First pass: gather every session's laps for all competitors. We accumulate
    # the write payloads instead of writing inline so the skip check below can see
    # the complete expected lap count before any delete/write happens.
    for sid in session_ids_for_race:
        logging.debug("Getting session details for %s including lap times.", sid)
        session_details = ctx.client.results.session_details(sid, include_lap_times=True)
        sorted_competitors = [dict(c) for c in session_details['Session']['SortedCompetitors']]

        flag_map = {0: "Green", 1: "Yellow", -1: "Finish"}

        if opts.network_mode:
            session_id = session_details['Session']['ID']
            session_name = session_details['Session'].get('Name', '')
            start_epoc = session_details['Session'].get('SessionStartDateEpoc')
            session_entry = {
                'session_id': session_id,
                'session_name': session_name,
                'start_epoc': start_epoc,
                'competitors': [],
            }
            class_index = _build_class_index(session_details)
            for competitor in sorted_competitors:
                comp_laps = competitor.get('LapTimes', [])
                if not comp_laps:
                    continue
                # Trim before use: RaceMonitor pads some numbers (e.g. ' 2') and
                # Python's int() accepts the padding, so the guard below would let
                # it through and the space would survive into the car_number tag.
                # Flux's int() rejects surrounding whitespace, which kills the
                # dashboard's $carno variable outright and blanks the whole race.
                comp_number = str(competitor['Number']).strip()
                try:
                    int(comp_number)
                except (ValueError, TypeError):
                    logging.debug("Skipping non-integer competitor number %r", comp_number)
                    continue
                comp_name = (
                    f"{competitor.get('FirstName', '')} {competitor.get('LastName', '')}".strip()
                    or None
                )
                comp_car_info = competitor.get('AdditionalData') or None
                influx_laps = []
                for lap in comp_laps:
                    # Filter here — not only in _build_lap_points — so the expected
                    # count used by --skip-if-complete matches what actually gets
                    # written; otherwise one garbage lap re-triggers the delete+
                    # rewrite on every backfill run.
                    try:
                        int(lap['Lap'])
                    except (ValueError, TypeError):
                        logging.warning(
                            "%s for car %s; excluding lap from backfill",
                            _describe_bad_value(lap['Lap'], 'Lap'), comp_number)
                        continue
                    if _time_to_ms(lap['TotalTime']) is None:
                        logging.warning(
                            "%s for car %s; excluding lap from backfill",
                            _describe_bad_value(lap['TotalTime'], 'TotalTime'), comp_number)
                        continue
                    influx_laps.append({
                        **lap,
                        'FlagStatus': flag_map.get(lap['FlagStatus'], str(lap['FlagStatus'])),
                    })
                if not influx_laps:
                    # Every lap was filtered out; appending an empty competitor would
                    # leave expected==0 and let the write path delete existing laps
                    # without writing replacements.
                    logging.warning(
                        "No parseable laps for car %s; excluding competitor from backfill",
                        comp_number)
                    continue
                class_name, class_positions = _resolve_class_historical(
                    comp_number, session_details, class_index)
                session_entry['competitors'].append({
                    'influx_laps': influx_laps,
                    'competitor_name': comp_name,
                    'car_info': comp_car_info,
                    'class_name': class_name,
                    'class_positions': class_positions,
                    'car_number': comp_number,
                    'final_position': competitor.get('Position', ''),
                    'final_laps': competitor.get('Laps', ''),
                    'best_lap_time': competitor.get('BestLapTime', ''),
                    'last_lap_time': competitor.get('LastLapTime', ''),
                })
            pending_writes.append(session_entry)

    # Collapse sessions RaceMonitor returned more than once BEFORE anything reads
    # pending_writes: expected must count what actually gets written, and
    # _apply_total_time_offsets must bank elapsed time across real sessions rather
    # than treating N copies of one session as N sequential ones.
    pending_writes = _merge_duplicate_sessions(pending_writes)

    no_data = not pending_writes or not any(s['competitors'] for s in pending_writes)
    if opts.network_mode and no_data:
        logging.warning(
            "No competitors with laps found for race %s — skipping write", ctx.race_id)
        return

    if opts.network_mode:
        expected = sum(
            len(comp['influx_laps'])
            for session in pending_writes
            for comp in session['competitors']
        )

        if opts.dry_run:
            print(UNDERLINE)
            total_laps = sum(
                len(comp['influx_laps'])
                for session in pending_writes
                for comp in session['competitors']
            )
            total_competitors = sum(len(s['competitors']) for s in pending_writes)
            for session in pending_writes:
                for comp in session['competitors']:
                    nlaps = len(comp['influx_laps'])
                    print(f"  would write {nlaps} laps for car {comp['car_number']}")
            print(f"  {total_competitors} competitor(s), {total_laps} laps total")
            print(UNDERLINE)
            return

        if opts.skip_if_complete and expected > 0:
            total, current = existing_lap_counts_fieldwide(ctx)
            if total == expected and current == expected:
                # Laps are complete and current, but only skip if standings are too —
                # otherwise a prior run whose standings phase failed would be skipped
                # forever with stale/missing standings. Treat standings as fresh when
                # some exist and none predate the current schema (approximate: standings
                # are written one atomic batch per session, so a partial-but-all-current
                # state is rare).
                std_total, std_current = existing_standings_counts_fieldwide(ctx)
                if std_total > 0 and std_current == std_total:
                    race_ts_ms = (
                        ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
                    )
                    if not push_influx_race(ctx, race_ts_ms, expected, len(pending_writes)):
                        logging.error(
                            "Race metadata write failed for race %s — failing the "
                            "run so the next backfill retries", ctx.race_id)
                        return 1
                    logging.info(
                        "SKIP: race %s already complete and current (%d laps, schema v%d)",
                        ctx.race_id, total, SCHEMA_VERSION)
                    return
                logging.info(
                    "Race %s laps complete but standings stale/missing (%d of %d "
                    "current) — rewriting", ctx.race_id, std_current, std_total)

        if not delete_existing_laps(ctx):
            logging.error(
                "Deleting existing laps failed for race %s — failing the run so "
                "the next backfill retries", ctx.race_id)
            return 1

        total_competitors = sum(len(s['competitors']) for s in pending_writes)
        logging.info(
            "Writing %d session(s), %d competitor(s)...",
            len(pending_writes), total_competitors)

        race_ts_ms = ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
        _apply_total_time_offsets(pending_writes)
        try:
            for session in pending_writes:
                session_points = []
                for comp in session['competitors']:
                    session_points.extend(_build_lap_points(
                        ctx, comp['influx_laps'], comp['competitor_name'], comp['car_info'],
                        comp['class_name'], comp['class_positions'], session['start_epoc'],
                        comp['car_number'], session['session_id'],
                        comp['total_time_offset_ms']))
                _write_points_chunked(ctx.write_api, session_points)
            logging.info("All lap data written successfully")
        except Exception as e:
            logging.error("Writing laps failed for race %s: %s", ctx.race_id, e)
            logging.warning("Skipping race stamp so next run will re-backfill")
            return 1

        sessions_ok = delete_existing_sessions(ctx)
        for session in pending_writes:
            if not push_influx_session(
                    ctx, session['session_id'], session['session_name'],
                    session['start_epoc']):
                sessions_ok = False
        if not sessions_ok:
            # A failed delete or a failed session write can leave the sessions
            # bucket holding stale, partial, or mixed records. Bail out here,
            # before the race is stamped complete, so the next backfill redoes
            # the whole rewrite instead of leaving an unrepairable session gap
            # that --skip-if-complete can't detect (it never checks sessions).
            logging.error(
                "Session write incomplete for race %s — failing the run so the "
                "next backfill retries", ctx.race_id)
            return 1

        if not push_influx_race(ctx, race_ts_ms, expected, len(pending_writes)):
            logging.error(
                "Race metadata write failed for race %s — failing the run so the "
                "next backfill retries", ctx.race_id)
            return 1

        standings_ok = delete_existing_standings(ctx)
        for session in pending_writes:
            if not push_influx_standings_historical(ctx, session):
                standings_ok = False
        if not standings_ok:
            # A partial write (some sessions stored, others failed) would otherwise
            # look "complete and current" to the skip checks — std_total and
            # std_current would match on just the sessions that succeeded — and
            # strand the race forever. Wipe the whole race's standings so std_total
            # drops to 0 and the next run re-backfills from scratch.
            if delete_existing_standings(ctx):
                logging.error(
                    "Standings incomplete for race %s — cleared partial standings; "
                    "failing the run so the next backfill rewrites them", ctx.race_id)
            else:
                logging.error(
                    "Standings incomplete for race %s and the cleanup delete failed; "
                    "rerun with --force to rewrite standings", ctx.race_id)
            return 1

    print_rankings(sorted_competitors, False, opts.selected_class,
                   session_details['Session']['Categories'])


def print_rankings(sorted_competitors, _race_live, selected_class, categories):
    """Take a list of sorted competitor dicts and print them in a nice table."""
    print(UNDERLINE)
    list_of_names = []

    for competitor in sorted_competitors:
        for item in competitor:
            if item in ("FirstName", "LastName") and competitor[item] != '':
                list_of_names.append(competitor[item])

    for competitor in sorted_competitors:
        if competitor['FirstName'] == '':
            competitor['Name'] = competitor['LastName']
        else:
            competitor['Name'] = competitor['FirstName']

    category_map = {k: categories.get(k, {}).get('Name', k) for k in
                    {c.get('Category') for c in sorted_competitors if c.get('Category')}}

    if selected_class:
        upper_class = selected_class[1].upper()
        logging.info("Current rankings for class %s.", upper_class)
        print(UNDERLINE)
        sorted_competitors_df = pandas.DataFrame(
            sorted_competitors,
            columns=['Position', 'Number', 'Name', 'Laps', 'Category', 'Transponder'])
        sorted_competitors_df = sorted_competitors_df.replace({'Category': category_map})
        sorted_competitors_df = sorted_competitors_df[
            sorted_competitors_df['Category'].str.contains(upper_class, case=False)]
        sorted_competitors_df = sorted_competitors_df.rename(
            columns={'Category': 'Class', 'Number': '#', 'Position': 'Overall Pos.'})
        sorted_competitors_df = sorted_competitors_df.sort_values(
            'Overall Pos.',
            key=lambda s: pandas.to_numeric(s, errors='coerce'),
            ignore_index=True)
        sorted_competitors_df['Class Pos.'] = (
            sorted_competitors_df.groupby('Class').cumcount() + 1)
        sorted_competitors_df = sorted_competitors_df[
            ['Overall Pos.', '#', 'Class', 'Class Pos.', 'Name', 'Laps', 'Transponder']]
        print(sorted_competitors_df.to_string(index=False))
    else:
        logging.info("Current overall rankings.")
        print(UNDERLINE)
        sorted_competitors_df = pandas.DataFrame(
            sorted_competitors,
            columns=['Position', 'Number', 'Name', 'Laps', 'Category', 'Transponder'])
        sorted_competitors_df = sorted_competitors_df.replace({'Category': category_map})
        sorted_competitors_df = sorted_competitors_df.rename(
            columns={'Category': 'Class', 'Number': '#', 'Position': 'Pos.'})
        sorted_competitors_df = sorted_competitors_df.sort_values(
            'Pos.',
            key=lambda s: pandas.to_numeric(s, errors='coerce'),
            ignore_index=True)
        sorted_competitors_df['Class Pos.'] = (
            sorted_competitors_df.groupby('Class').cumcount() + 1)
        sorted_competitors_df = sorted_competitors_df[
            ['Pos.', '#', 'Class', 'Class Pos.', 'Name', 'Laps', 'Transponder']]
        print(sorted_competitors_df.to_string(index=False))

    print(UNDERLINE)

    return list_of_names


def write_csv(filename, competitor_lap_times):
    """Write laptimes for a competitor to a file."""
    logging.info("Writing lap times to %s.csv", filename)
    print(UNDERLINE)
    if not competitor_lap_times:
        return
    with open(f"./{filename}.csv", 'w', encoding='utf-8', newline='') as lap_csv_fh:
        writer = csv.DictWriter(lap_csv_fh, fieldnames=competitor_lap_times[0].keys())
        writer.writeheader()
        writer.writerows(competitor_lap_times)


def monitor_routine(ctx, laps, opts, competitor_name=None, car_info=None, _stop_event=None,
                    session_id=None, race_meta_written=True,
                    observer=None) -> MonitorStatus | None:
    """Poll for new laps during a live race, displaying and optionally pushing each
    to InfluxDB via the given observer (defaults to _StdoutObserver).

    Returns MonitorStatus.RACE_ENDED when the race ends naturally, or
    MonitorStatus.INTERRUPTED on KeyboardInterrupt (caller should exit 130).
    Returns None when the loop exits because _stop_event was set before the race
    ended (the test-injection path).
    _stop_event may be injected for testing; defaults to a new threading.Event.
    session_id is tracked across polls and tags each written lap point; a new
    session push fires whenever the live session ID changes.
    race_meta_written reflects whether the caller's initial push_influx_race
    succeeded; when False (or after the epoch is corrected below) the metadata
    write is retried each poll until it lands, so a transient delete/write
    failure self-heals without aborting lap capture.
    """
    if observer is None:
        observer = _StdoutObserver()
    logging.info("Monitoring car %s...", ctx.car_number)

    observer.on_status(UNDERLINE)
    observer.on_laps(laps)

    stop = _stop_event if _stop_event is not None else threading.Event()
    poll_count = 0
    prev_standings = {}
    try:
        while not stop.wait(timeout=opts.interval):
            poll_count += 1

            if opts.network_mode and ctx.start_epoc == 0:
                try:
                    race_details = ctx.client.race.details(ctx.race_id)
                except Exception:
                    logging.debug("race details refresh failed; skipping epoch recheck")
                    race_details = {'Successful': False}
                if race_details.get('Successful'):
                    new_epoc = race_details['Race'].get('StartDateEpoc', 0)
                    if new_epoc != 0:
                        ctx.start_epoc = new_epoc
                        if ctx.metadata is not None:
                            ctx.metadata.end_time_epoc = race_details['Race'].get(
                                'EndDateEpoc', ctx.metadata.end_time_epoc)
                        # Force a rewrite with the corrected timestamp; the retry
                        # block below performs it and re-attempts on failure.
                        race_meta_written = False

            if opts.network_mode and not race_meta_written:
                # Fall back to wall-clock (like live_race's initial write) when
                # RaceMonitor still hasn't posted a start epoch — the point must
                # not stay missing just because the epoch never arrives; the
                # epoch recheck above forces a rewrite if it shows up later.
                race_ts_ms = (
                    ctx.start_epoc * 1000 if ctx.start_epoc != 0 else int(time.time() * 1000)
                )
                race_meta_written = push_influx_race(ctx, race_ts_ms)

            try:
                session_response = ctx.client.live.get_session(ctx.race_id)
            except Exception:
                logging.debug("get_session failed; skipping session check")
                session_response = {'Successful': False}
            session = _live_session(session_response)
            if session is not None:
                new_session_id = session.get('ID')
                if new_session_id and new_session_id != session_id:
                    observer.on_session_change(session.get('Name', ''))
                    session_id = new_session_id
                    prev_standings = {}
                    if opts.network_mode:
                        push_influx_session(
                            ctx, session_id, session.get('Name'), None)
            else:
                logging.debug("get_session returned no session; may be between sessions")

            if opts.network_mode:
                prev_standings = push_influx_standings_live(
                    ctx, session_response, session_id, prev_standings)
            observer.on_standings(session_response)

            if poll_count % _LIVE_CHECK_INTERVAL == 0:
                try:
                    live_response = ctx.client.race.is_live(ctx.race_id)
                    if live_response.get('Successful') and not live_response.get('IsLive'):
                        observer.on_race_ended()
                        return MonitorStatus.RACE_ENDED
                except Exception:
                    logging.debug("is_live check failed; skipping")

            try:
                current_competitor_lap_times = refresh_competitor(ctx)
            except Exception:
                logging.debug("refresh_competitor failed; skipping this poll")
                continue
            if not current_competitor_lap_times:
                continue

            # Resolve class position once per poll (on the first new lap): it is
            # invariant across this batch, so a per-lap call would only repeat the
            # same O(competitors) scan and, when the car is absent, log the same
            # warning for every lap. Computed regardless of network_mode because
            # the API-provided lap dict has no ClassPosition of its own.
            class_name = class_position = None
            resolved = False
            for lap in current_competitor_lap_times:
                if lap in laps:
                    continue
                if not resolved:
                    class_name, class_position = _resolve_class_live(
                        session_response, ctx.car_number)
                    resolved = True
                # Dedup on a pristine snapshot: the live feed never returns a
                # ClassPosition key, so stamping it below would make every re-seen
                # lap look new next poll. Snapshot before stamping.
                laps.append(dict(lap))
                if class_position is not None:
                    lap['ClassPosition'] = class_position
                observer.on_lap(lap)
                if opts.network_mode:
                    try:
                        new_lap_num = int(lap['Lap'])
                    except (ValueError, TypeError):
                        logging.warning(
                            "%s in monitor; skipping",
                            _describe_bad_value(lap['Lap'], 'Lap'))
                        continue
                    class_positions = (
                        {new_lap_num: class_position} if class_position is not None else None)
                    push_influx(
                        ctx, [lap], True,
                        competitor_name=competitor_name,
                        car_info=car_info,
                        class_name=class_name, class_positions=class_positions,
                        session_id=session_id)
    except KeyboardInterrupt:
        observer.on_status("\nMonitoring stopped.")
        return MonitorStatus.INTERRUPTED


def refresh_competitor(ctx):
    """Get latest lap times for a competitor from the live API."""
    logging.debug("Refreshing lap times for car %s.", ctx.car_number)
    response = ctx.client.live.get_racer(ctx.race_id, ctx.car_number)

    # 'Successful' is always True here — the client raises otherwise — so the
    # only empty case worth guarding is a response without lap detail. The caller
    # swallows a raised call as one lost poll.
    laps = (response.get('Details') or {}).get('Laps') or []

    if laps:
        logging.debug(
            "Current lap is %s with time %s.", laps[-1]['Lap'], laps[-1]['LapTime'])
    return laps


def _time_to_ms(value):
    """Parse a RaceMonitor time string to milliseconds.

    Accepts variable precision: 'H:MM:SS.mmm', 'MM:SS.mmm', or 'SS.mmm', with the
    fractional part optional and of any length (padded/truncated to milliseconds).
    RaceMonitor omits the hours component when a value is under an hour, so we
    right-align the colon-separated parts.

    Returns None for empty input (a competitor with no lap yet — common, so not
    logged) and for unparseable values (logged; the API occasionally returns
    garbage for invalid or pit laps), causing the field to be omitted from the
    InfluxDB point.
    """
    if not value:
        return None
    try:
        parts = value.split(':')
        sec, _, ms = parts[-1].partition('.')
        hours = int(parts[-3]) if len(parts) >= 3 else 0
        minutes = int(parts[-2]) if len(parts) >= 2 else 0
        return (hours * 3600000 + minutes * 60000 + int(sec) * 1000
                + int(ms.ljust(3, '0')[:3]))
    except (ValueError, AttributeError):
        logging.warning("%s; omitting field", _describe_bad_value(value, 'time'))
        return None


def _write_points_chunked(write_api, points, batch_size=_WRITE_BATCH_SIZE):
    """Write points to the laps bucket in chunks of batch_size, logging each
    batch individually when more than one batch is needed."""
    chunks = range(0, len(points), batch_size)
    total = len(chunks)
    for batch_num, i in enumerate(chunks, 1):
        write_api.write(bucket=_influx.BUCKET_LAPS, record=points[i:i + batch_size])
        if total > 1:
            logging.info("Batch %d of %d written successfully", batch_num, total)


def _merge_duplicate_sessions(pending_writes):
    """Collapse sessions RaceMonitor returned under more than one ID.

    RaceMonitor can hand back the same session several times with different IDs.
    session_id is an Influx tag, so writing each copy stores the same laps again as
    a fresh series — race 64202 held 88,907 points for 33,935 real laps that way.

    Sessions are grouped by (session_name, start_epoc) and the lowest ID wins as
    canonical. Copies are usually identical, but some are stubs holding a lap or
    two the full copy lacks, so each car's laps are unioned across the group rather
    than picking one copy wholesale. Selection is per car: a sibling can be the
    base for one car and not another.

    Either key component being falsy (missing session_name, or start_epoc of
    None or 0) makes the entry unmergeable — it groups only with itself, keyed
    by a fresh sentinel unique to that entry. Without this, two distinct sessions
    that happen to share a name and lack a start epoch (e.g. two heats both named "Race")
    would collapse, and the per-lap union would silently discard whichever
    heat's lap didn't win the tie for a given lap number. A session with no
    start epoch can't be time-anchored anyway (_build_lap_points warns and
    anchors to the Unix epoch), so treating it as unmergeable costs nothing.

    Returns a new list, leaving pending_writes untouched. Group order follows first
    appearance so the caller's session ordering survives — _apply_total_time_offsets
    depends on it.

    Laps are keyed by int(lap['Lap']); this is safe only because the caller
    pre-filters non-numeric Lap values before building pending_writes and drops
    any competitor whose laps are all filtered out, so every 'Lap' seen here is
    already known to parse as an int.
    """
    groups = {}
    order = []
    for entry in pending_writes:
        if entry['session_name'] and entry['start_epoc']:
            key = (entry['session_name'], entry['start_epoc'])
        else:
            # No real (name, start_epoc) identity to group on — group with
            # nothing else by keying on this entry's own identity.
            key = object()
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(entry)

    merged = []
    for key in order:
        siblings = groups[key]
        if len(siblings) == 1:
            entry = dict(siblings[0])
            entry['competitors'] = [dict(comp) for comp in entry['competitors']]
            merged.append(entry)
            continue

        by_car = {}
        car_order = []
        for sib in siblings:
            for comp in sib['competitors']:
                car = comp['car_number']
                if car not in by_car:
                    by_car[car] = []
                    car_order.append(car)
                by_car[car].append((int(sib['session_id']), comp))

        competitors = []
        for car in car_order:
            # richest first so the fullest copy supplies the scalars; session id
            # breaks ties so the merge is deterministic run to run.
            ranked = sorted(by_car[car], key=lambda pair: (-len(pair[1]['influx_laps']),
                                                           pair[0]))
            base = dict(ranked[0][1])
            laps = {int(lap['Lap']): lap for lap in base['influx_laps']}
            class_positions = dict(base['class_positions'])
            for _, other in ranked[1:]:
                for lap in other['influx_laps']:
                    laps.setdefault(int(lap['Lap']), lap)
                for lap_num, position in other['class_positions'].items():
                    class_positions.setdefault(lap_num, position)
            base['influx_laps'] = [laps[n] for n in sorted(laps)]
            base['class_positions'] = class_positions
            competitors.append(base)

        merged.append({
            'session_id': min(int(s['session_id']) for s in siblings),
            'session_name': key[0],
            'start_epoc': key[1],
            'competitors': competitors,
        })
    return merged


def _apply_total_time_offsets(pending_writes):
    """Annotate every competitor with the elapsed ms it banked in earlier sessions.

    RaceMonitor's TotalTime is a car's elapsed race clock across the *whole* race,
    but each session's laps anchor on that session's own SessionStartDateEpoc.
    Subtracting the earlier sessions' elapsed time is what keeps a multi-day race's
    later sessions from landing hours past their own green flag. The tally is per
    car because each car has its own clock and may sit a session out entirely.
    """
    banked = {}
    for session in sorted(pending_writes, key=lambda s: s['start_epoc'] or 0):
        for comp in session['competitors']:
            car_number = comp['car_number']
            comp['total_time_offset_ms'] = banked.get(car_number, 0)
            # influx_laps is pre-filtered to parseable TotalTime values, so max()
            # sees only ints; default guards the can't-happen empty case.
            last_total_ms = max(
                (_time_to_ms(lap['TotalTime']) for lap in comp['influx_laps']),
                default=None)
            if last_total_ms is not None:
                banked[car_number] = last_total_ms


def _build_lap_points(ctx, laps, competitor_name, car_info, class_name, class_positions,
                      start_epoc, car_number, session_id=None, total_time_offset_ms=0):
    """Build InfluxDB Point objects for one competitor's laps."""
    effective_epoc = start_epoc if start_epoc is not None else ctx.start_epoc
    if effective_epoc == 0:
        logging.warning("Start epoch is 0; lap timestamps will be anchored to Unix epoch")
    start_epoc_ms = effective_epoc * 1000
    points = []
    for lap in laps:
        total_time_ms = _time_to_ms(lap['TotalTime'])
        if total_time_ms is None:
            logging.warning("%s for %s; skipping lap",
                            _describe_bad_value(lap['TotalTime'], 'TotalTime'), competitor_name)
            continue
        time_lap_completed_ms = start_epoc_ms + total_time_ms - total_time_offset_ms
        lap_time_ms = _time_to_ms(lap['LapTime'])
        try:
            lap_num = int(lap['Lap'])
        except (ValueError, TypeError):
            logging.warning("%s for %s; skipping lap",
                            _describe_bad_value(lap['Lap'], 'Lap'), competitor_name)
            continue
        try:
            position = int(lap['Position'])
        except (ValueError, TypeError):
            logging.warning(
                "%s on lap %s for %s; omitting field",
                _describe_bad_value(lap['Position'], 'Position'), lap_num, competitor_name,
            )
            position = None
        point = (
            Point("lap")
            .tag("race_id", ctx.race_id)
            .tag("class", class_name)
            .tag("car_number", car_number)
            .field("lap_no", lap_num)
            .field("flag_status", lap['FlagStatus'])
            .field("schema_version", SCHEMA_VERSION)
            .field("competitor_name", competitor_name)
            .field("car_info", car_info)
            .time(time_lap_completed_ms, WritePrecision.MS)
        )
        if lap_time_ms is not None:
            point = point.field("lap_time", lap_time_ms)
        if position is not None:
            point = point.field("position", position)
        if session_id is not None:
            point = point.tag("session_id", str(session_id))
        if class_positions is not None:
            class_pos = class_positions.get(lap_num)
            if class_pos is not None:
                point = point.field("class_position", class_pos)
        logging.debug(point.to_line_protocol())
        points.append(point)
    return points


def push_influx(ctx, laps, monitor_mode, competitor_name=None, car_info=None,
                class_name=None, class_positions=None, start_epoc=None,
                car_number=None, session_id=None):
    """Build and write lap points to InfluxDB for one competitor.

    monitor_mode suppresses the "Writing laps..." log line (used for per-lap live writes).
    car_number defaults to ctx.car_number when None. session_id is passed through as a
    tag on each point.
    """
    logging.debug("Entering network mode.")
    effective_car_number = car_number if car_number is not None else ctx.car_number

    if not monitor_mode:
        logging.info("Writing laps to influx...")

    points = _build_lap_points(
        ctx, laps, competitor_name, car_info, class_name, class_positions,
        start_epoc, effective_car_number, session_id)

    if points:
        try:
            _write_points_chunked(ctx.write_api, points)
            logging.debug("Wrote %d laps to influx.", len(points))
            if not monitor_mode:
                logging.info('All lap data written successfully')
        except Exception as e:
            logging.error("Writing %d laps failed: %s", len(points), e)
            print(UNDERLINE)
            return False

    print(UNDERLINE)
    return True


def push_influx_race(ctx, timestamp_ms, expected_lap_count=None, session_count=None):
    """Write one race metadata point to the races bucket, replacing any prior point.

    Returns True on success. Returns False when metadata is missing or the
    delete/write fails — the delete may have already removed the old point, so
    a False return means the race may now be absent from the races bucket and
    the caller must treat the run as failed so a retry restores it.
    """
    if ctx.metadata is None:
        logging.warning("push_influx_race called with no metadata for race %s", ctx.race_id)
        return False
    try:
        ctx.delete_api.delete(
            start='1970-01-01T00:00:00Z',
            stop=datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="race" AND race_id="{ctx.race_id}"',
            bucket=_influx.BUCKET_RACES,
        )
        meta = ctx.metadata
        point = (
            Point("race")
            .tag("race_id", ctx.race_id)
            .tag("race_name", meta.race_name)
            .tag("track_name", meta.track_name)
            .tag("series_name", meta.series_name)
            .field("end_time_epoc", meta.end_time_epoc)
            .time(timestamp_ms, WritePrecision.MS)
        )
        if expected_lap_count is not None:
            point.field("schema_version", SCHEMA_VERSION)
            point.field("expected_lap_count", expected_lap_count)
            point.field("session_count", session_count)
        ctx.write_api.write(bucket=_influx.BUCKET_RACES, record=point)
        return True
    except Exception as e:
        logging.error("Writing race failed: %s", e)
        return False


def push_influx_session(ctx, session_id, session_name, start_epoc):
    """Write one session metadata point to the race_sessions bucket, replacing any prior point.

    Returns True on success, False if the delete or write failed (logged, not
    raised) so callers that need every session to land — such as old_race's
    rewrite loop — can tell a partial write from a complete one.
    """
    try:
        ctx.delete_api.delete(
            start=EPOCH_START,
            stop=datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="session" AND session_id="{session_id}"',
            bucket=_influx.BUCKET_SESSIONS,
        )
        start_epoc_ms = (start_epoc or 0) * 1000
        point = (
            Point("session")
            .tag("race_id", ctx.race_id)
            .tag("session_id", str(session_id))
            .field("session_name", session_name or "")
            .field("start_epoc", start_epoc or 0)
            .time(start_epoc_ms, WritePrecision.MS)
        )
        ctx.write_api.write(bucket=_influx.BUCKET_SESSIONS, record=point)
        return True
    except Exception as e:
        logging.error(
            "Writing session failed for race %s session %s: %s",
            ctx.race_id, session_id, e,
        )
        return False


def existing_lap_counts(ctx):
    """Return (total_laps, current_laps) for the tracked car's laps in this race.

    Counts laps filtered by ctx.car_number. Note: the historical backfill path
    uses existing_lap_counts_fieldwide instead (which counts across all cars
    without a car_number filter). This function is for single-car callers such
    as tests or diagnostic tooling.

    total_laps  — number of lap points written for the tracked car.
    current_laps — number of those laps stamped with the current SCHEMA_VERSION.

    A race is safe to skip only when both equal RaceMonitor's reported lap total:
    total < expected means a partial/truncated backfill, current < total means
    some laps predate the current schema (written by an older laps.py).
    """
    def _count(field_filter):
        """Count lap points for the tracked car matching ``field_filter``."""
        tables = ctx.query_api.query(
            f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{ctx.race_id}"\n'
            f'      and r.car_number == "{ctx.car_number}")\n'
            f'  |> filter(fn: (r) => {field_filter})\n'
            f'  |> count()'
        )
        return sum(r.get_value() for t in tables for r in t.records)

    total = _count('r._field == "lap_no"')
    current = _count(f'r._field == "schema_version" and r._value == {SCHEMA_VERSION}')
    return total, current


def existing_lap_counts_fieldwide(ctx):
    """Return (total_laps, current_laps) for all cars in this race.

    Like existing_lap_counts but without a car_number filter — counts across
    the full field. Used by old_race when running in fieldwide mode.
    """
    def _count(field_filter):
        """Count lap points across all cars in the race matching ``field_filter``."""
        tables = ctx.query_api.query(
            f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "lap"\n'
            f'      and r.race_id == "{ctx.race_id}")\n'
            f'  |> filter(fn: (r) => {field_filter})\n'
            f'  |> count()'
        )
        return sum(r.get_value() for t in tables for r in t.records)

    total = _count('r._field == "lap_no"')
    current = _count(f'r._field == "schema_version" and r._value == {SCHEMA_VERSION}')
    return total, current


def delete_existing_laps(ctx):
    """Delete all lap points for this race so a backfill can replace them.

    Returns True on success, False if the delete failed (logged, not raised) —
    old_race treats a failed delete the same as a failed session write: it
    aborts before stamping the race complete.
    """
    try:
        ctx.delete_api.delete(
            start=EPOCH_START,
            stop=datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="lap" AND race_id="{ctx.race_id}"',
            bucket=_influx.BUCKET_LAPS,
        )
        return True
    except Exception as e:
        logging.error("Deleting existing laps failed: %s", e)
        return False


def delete_existing_sessions(ctx):
    """Delete all session points for this race so a backfill can replace them.

    push_influx_session only deletes the session_id it is about to rewrite, so a
    session that dedupe collapsed away would otherwise linger in the bucket and
    keep showing up in the dashboard's session picker.

    The delete predicate is race_id-only, so despite the name it also removes
    session records written by the live-monitor path, which tags race_id
    identically. This self-heals: old_race always rewrites every session for
    the race afterward. But the name suggests a narrower scope than it has.

    Returns True on success, False if the delete failed (logged, not raised) —
    old_race treats a failed delete the same as a failed session write: it
    aborts before stamping the race complete.
    """
    try:
        ctx.delete_api.delete(
            start=EPOCH_START,
            stop=datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="session" AND race_id="{ctx.race_id}"',
            bucket=_influx.BUCKET_SESSIONS,
        )
        return True
    except Exception as e:
        logging.error("Deleting existing sessions failed: %s", e)
        return False


def delete_existing_standings(ctx):
    """Delete all standings points for this race so a backfill can replace them.

    Returns True on success, False if the delete failed (logged, not raised) so
    the caller can tell the operator that standings need a re-run.
    """
    try:
        ctx.delete_api.delete(
            start='1970-01-01T00:00:00Z',
            stop=datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
            predicate=f'_measurement="standings" AND race_id="{ctx.race_id}"',
            bucket=_influx.BUCKET_LAPS,
        )
        return True
    except Exception as e:
        logging.error("Deleting existing standings failed: %s", e)
        return False


def existing_standings_counts_fieldwide(ctx):
    """Return (total_standings, current_standings) for all cars in this race.

    total counts every standings point (one position field each); current counts
    those stamped with the current SCHEMA_VERSION. Used by old_race to confirm
    standings are fresh before skipping a race whose laps are already complete.
    """
    def _count(field_filter):
        """Count standings points across all cars in the race matching ``field_filter``."""
        tables = ctx.query_api.query(
            f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
            f'  |> range(start: {EPOCH_START})\n'
            f'  |> filter(fn: (r) => r._measurement == "standings"\n'
            f'      and r.race_id == "{ctx.race_id}")\n'
            f'  |> filter(fn: (r) => {field_filter})\n'
            f'  |> count()'
        )
        return sum(r.get_value() for t in tables for r in t.records)

    total = _count('r._field == "position"')
    current = _count(f'r._field == "schema_version" and r._value == {SCHEMA_VERSION}')
    return total, current


def stored_race_completeness(ctx):
    """Read the stored race metadata point for ctx.race_id from the races bucket.

    Returns a StoredRace, or None when no race point exists. schema_version and
    expected_lap_count are None when the point predates the fields they name —
    callers must guard against None before comparing them numerically.
    """
    tables = ctx.query_api.query(
        f'from(bucket: "{_influx.BUCKET_RACES}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "race"\n'
        f'      and r.race_id == "{ctx.race_id}")\n'
        f'  |> last()\n'
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )
    for table in tables:
        for record in table.records:
            vals = record.values
            return StoredRace(
                schema_version=vals.get('schema_version'),
                expected_lap_count=vals.get('expected_lap_count'),
                end_time_epoc=vals.get('end_time_epoc'),
            )
    return None


def stored_end_settled(stored):
    """True only when the race's stored end time is known and settled in the past.

    The live-race guard for the Influx-only skip. Sessions can share one race_id
    across a multi-day event, so a race whose stored end is 0 (unknown), in the
    future, or only recently past (within _SETTLED_BUFFER_S) may still have a
    later session going live. Only a race whose end is settled well behind us is
    safe to skip from Influx alone; anything more recent must fall through to the
    real is_live check rather than being skipped from Influx alone.
    """
    return bool(stored.end_time_epoc) and stored.end_time_epoc < time.time() - _SETTLED_BUFFER_S


def race_complete_in_influx(ctx, stored):
    """True when the stored race point proves this race is complete and current.

    Mirrors old_race's skip predicate but sources the expected lap total from the
    stored race point (Influx) instead of a RaceMonitor fetch. `stored` is passed
    in already-fetched so main() does not query the race point twice. The
    is-not-None guard is required: a pre-existing point has expected_lap_count
    None, and None > 0 raises TypeError in Python 3.
    """
    if stored is None or stored.schema_version != SCHEMA_VERSION:
        return False
    if stored.expected_lap_count is None or stored.expected_lap_count <= 0:
        return False
    expected = stored.expected_lap_count
    total, current = existing_lap_counts_fieldwide(ctx)
    if not (total == current == expected):
        return False
    std_total, std_current = existing_standings_counts_fieldwide(ctx)
    return std_total > 0 and std_current == std_total


def _influx_only_skip(race_id):
    """True when race_id is complete, current, and ended per Influx alone.

    Opens a short-lived read-only Influx connection and answers the backfill skip
    decision without any RaceMonitor call. Any race that is not definitively
    ended (see stored_end_settled) returns False so it falls through to the normal
    flow's is_live check.
    """
    with _influx.connect() as influx_client:
        ctx = RaceContext(race_id, None, None, None, 0,
                          query_api=influx_client.query_api())
        stored = stored_race_completeness(ctx)
        return (stored is not None
                and stored_end_settled(stored)
                and race_complete_in_influx(ctx, stored))


def _build_class_index(session_details):
    """Precompute per-session class-position data shared by every competitor.

    Returns (categories, category_by_car, laps_by_car, positions_by_category):
    laps_by_car maps car_number -> {lap_num: overall_position} and
    positions_by_category maps category -> {lap_num: [overall_positions]} across
    all cars in that category. Built once per session so resolving N competitors
    is O(total laps), not O(N * total laps).
    """
    session = session_details['Session']
    categories = session['Categories']
    category_by_car = {}
    laps_by_car = {}
    positions_by_category = defaultdict(lambda: defaultdict(list))
    for competitor in session['SortedCompetitors']:
        # Trim to match the lookup key: callers pass the already-trimmed
        # car_number (RaceMonitor pads some numbers, e.g. ' 2'), so an
        # untrimmed index key here would always miss for those cars.
        number = str(competitor['Number']).strip()
        category_by_car[number] = competitor['Category']
        lap_positions = {}
        for lap in competitor.get('LapTimes', []):
            try:
                lap_positions[int(lap['Lap'])] = int(lap['Position'])
            except (KeyError, ValueError, TypeError):
                logging.debug("%s in class resolution; skipping",
                              _describe_bad_value(lap.get('Lap'), 'Lap'))
        laps_by_car[number] = lap_positions
        for lap_num, pos in lap_positions.items():
            positions_by_category[competitor['Category']][lap_num].append(pos)
    return categories, category_by_car, laps_by_car, positions_by_category


def _resolve_class_historical(car_number, session_details, index=None):
    """Return (class_name, {lap_num: class_position}) for the given car_number.

    index is the result of _build_class_index for this session; callers resolving
    many cars should build it once and pass it in. When omitted it is built on
    the fly (single-car callers and tests).
    """
    if index is None:
        index = _build_class_index(session_details)
    categories, category_by_car, laps_by_car, positions_by_category = index

    tracked_category = category_by_car.get(car_number)
    if tracked_category is None:
        return None, {}

    class_name = (
        categories.get(tracked_category, {})
        .get('Name', tracked_category)
    )

    # The tracked car's own position is never < itself, so including it in the
    # per-category lists (unlike the old per-car rescan) does not change counts.
    class_lap_positions = positions_by_category[tracked_category]
    class_positions = {
        lap_num: 1 + sum(1 for pos in class_lap_positions.get(lap_num, []) if pos < tracked_pos)
        for lap_num, tracked_pos in laps_by_car[car_number].items()
    }

    return class_name, class_positions


def _resolve_class_live(session_response, car_number):
    """Return (class_name, class_position) for the tracked car from a live session.

    Takes the response from ``client.live.get_session`` so the caller can fetch the
    session once and reuse it.
    """
    session = _live_session(session_response)
    if session is None:
        return None, None
    classes = session['Classes']
    competitors = session['Competitors']

    tracked = None
    for competitor in competitors.values():
        if competitor['Number'] == car_number:
            tracked = competitor
            break

    if tracked is None:
        logging.warning("_resolve_class_live: car %s not found in session competitors", car_number)
        return None, None

    class_id = tracked['ClassID']
    logging.debug(
        "_resolve_class_live: car=%s overall_pos=%s class_id=%r classes=%s",
        car_number, tracked.get('Position'), class_id,
        {k: v.get('Description') for k, v in classes.items()},
    )
    class_name = classes.get(class_id, {}).get('Description', class_id)

    try:
        tracked_pos = int(tracked['Position'])
    except (ValueError, TypeError):
        return class_name, None

    class_position = 1
    for competitor in competitors.values():
        if competitor['Number'] == car_number or competitor['ClassID'] != class_id:
            continue
        try:
            comp_pos = int(competitor['Position'])
            if comp_pos < tracked_pos:
                logging.debug(
                    "_resolve_class_live: same-class car %s at pos %s counts as ahead",
                    competitor['Number'], comp_pos,
                )
                class_position += 1
        except (ValueError, TypeError):
            pass

    logging.debug(
        "_resolve_class_live: car=%s class=%r overall_pos=%s class_pos=%s",
        car_number, class_name, tracked_pos, class_position,
    )
    return class_name, class_position


def _compute_class_positions_live(session_response):
    """Return {car_number: class_position} for all live competitors in one pass.

    Keys are trimmed to match the trimmed car_number used at the call site
    (RaceMonitor pads some numbers, e.g. ' 2'), so the lookup there hits.
    """
    session = _live_session(session_response)
    if session is None:
        return {}
    competitors = session['Competitors']
    by_class = defaultdict(list)
    for comp in competitors.values():
        try:
            pos = int(comp['Position'])
        except (ValueError, TypeError):
            continue
        by_class[comp['ClassID']].append((pos, str(comp['Number']).strip()))
    result = {}
    for entries in by_class.values():
        entries.sort()
        for rank, (_, car_number) in enumerate(entries, 1):
            result[car_number] = rank
    return result


def _compute_class_positions_final(competitors):
    """Return {car_number: class_position} for a completed session, ranking
    same-class competitors by their final overall position.

    Mirrors _compute_class_positions_live. Used for the standings snapshot instead
    of the per-lap class_positions dict: that dict holds each car's class position
    at its own last lap, computed only against same-class cars that reached that
    lap, so cars finishing at different lap counts collide on the same value.
    """
    by_class = defaultdict(list)
    for comp in competitors:
        try:
            pos = int(comp['final_position'])
        except (ValueError, TypeError):
            continue
        by_class[comp['class_name']].append((pos, comp['car_number']))
    result = {}
    for entries in by_class.values():
        entries.sort()
        for rank, (_, car_number) in enumerate(entries, 1):
            result[car_number] = rank
    return result


def push_influx_standings_live(ctx, session_response, session_id, prev_standings=None):
    """Write one standings point per competitor when standings have changed.

    prev_standings maps car_number to a (position, lap_count, class_position,
    best_lap_ms, last_lap_ms) snapshot from the previous poll.  The entire
    field is compared atomically — if any competitor changed, all are written.
    Pass None to write all competitors unconditionally (e.g. at race startup).
    Returns the current standings snapshot dict for the caller to pass next poll.
    """
    session = _live_session(session_response)
    if session is None:
        logging.debug("push_influx_standings_live: no live session in response, skipping")
        return prev_standings if prev_standings is not None else {}
    competitors = session['Competitors']
    classes = session['Classes']
    class_positions = _compute_class_positions_live(session_response)
    timestamp_ms = int(time.time() * 1000)
    curr_standings = {}
    points = []
    for comp in competitors.values():
        try:
            position = int(comp['Position'])
        except (ValueError, TypeError):
            continue
        try:
            lap_count = int(comp['Laps'])
        except (ValueError, TypeError):
            continue
        class_id = comp['ClassID']
        class_name = classes.get(class_id, {}).get('Description', class_id)
        competitor_name = (
            f"{comp.get('FirstName', '')} {comp.get('LastName', '')}".strip() or None
        )
        car_info = comp.get('AdditionalData') or None
        # Trim to match the lap write path; an untrimmed ' 2' would tag standings
        # as a different car than its own laps.
        car_number = str(comp['Number']).strip()
        best_lap_ms = _time_to_ms(comp.get('BestLapTime', ''))
        last_lap_ms = _time_to_ms(comp.get('LastLapTime', ''))
        class_position = class_positions.get(car_number)
        curr_standings[car_number] = (
            position, lap_count, class_position, best_lap_ms, last_lap_ms)
        point = (
            Point("standings")
            .tag("race_id", ctx.race_id)
            .tag("car_number", car_number)
            .tag("class", class_name)
            .field("position", position)
            .field("lap_count", lap_count)
            .field("schema_version", SCHEMA_VERSION)
            .field("competitor_name", competitor_name)
            .field("car_info", car_info)
            .time(timestamp_ms, WritePrecision.MS)
        )
        if session_id is not None:
            point = point.tag("session_id", str(session_id))
        if class_position is not None:
            point = point.field("class_position", class_position)
        if best_lap_ms is not None:
            point = point.field("best_lap_time", best_lap_ms)
        if last_lap_ms is not None:
            point = point.field("last_lap_time", last_lap_ms)
        points.append(point)
    if points and curr_standings != prev_standings:
        try:
            _write_points_chunked(ctx.write_api, points)
            logging.debug(
                "Wrote %d standings points for live race %s", len(points), ctx.race_id)
        except Exception as e:
            logging.error(
                "Writing standings failed for race %s: %s", ctx.race_id, e)
            return prev_standings if prev_standings is not None else {}
    return curr_standings


def push_influx_standings_historical(ctx, session_entry):
    """Write one standings point per competitor from a completed session."""
    start_epoc = session_entry.get('start_epoc') or 0
    timestamp_ms = start_epoc * 1000
    session_id = session_entry['session_id']
    class_positions_final = _compute_class_positions_final(session_entry['competitors'])
    points = []
    for comp in session_entry['competitors']:
        try:
            position = int(comp['final_position'])
        except (ValueError, TypeError):
            continue
        try:
            lap_count = int(comp['final_laps'])
        except (ValueError, TypeError):
            continue
        class_position = class_positions_final.get(comp['car_number'])
        best_lap_ms = _time_to_ms(comp.get('best_lap_time') or '')
        last_lap_ms = _time_to_ms(comp.get('last_lap_time') or '')
        point = (
            Point("standings")
            .tag("race_id", ctx.race_id)
            .tag("car_number", comp['car_number'])
            .tag("class", comp['class_name'])
            .tag("session_id", str(session_id))
            .field("position", position)
            .field("lap_count", lap_count)
            .field("schema_version", SCHEMA_VERSION)
            .field("competitor_name", comp['competitor_name'])
            .field("car_info", comp['car_info'])
            .time(timestamp_ms, WritePrecision.MS)
        )
        if class_position is not None:
            point = point.field("class_position", class_position)
        if best_lap_ms is not None:
            point = point.field("best_lap_time", best_lap_ms)
        if last_lap_ms is not None:
            point = point.field("last_lap_time", last_lap_ms)
        points.append(point)
    if points:
        try:
            _write_points_chunked(ctx.write_api, points)
            logging.debug(
                "Wrote %d historical standings for race %s session %s",
                len(points), ctx.race_id, session_id)
        except Exception as e:
            logging.error(
                "Writing historical standings failed for race %s session %s: %s",
                ctx.race_id, session_id, e)
            return False
    return True


def _resolve_race_metadata(race_details, client):
    """Resolve race-level metadata from race details and a single series lookup."""
    if not race_details.get('Successful'):
        return RaceMetadata(race_name='', track_name='', series_name=None, end_time_epoc=0)
    race = race_details['Race']
    series_id = race.get('SeriesID')
    series_name = None
    if series_id is not None:
        try:
            resp = client.common.current_races(series_id=series_id)
            if resp.get('Races'):
                series_name = resp['Races'][0]['SeriesName']
            else:
                resp = client.common.past_races(series_id=series_id, max_results=1)
                if resp.get('Races'):
                    series_name = resp['Races'][0]['SeriesName']
        except Exception:
            logging.warning("Failed to resolve series name for series_id=%s", series_id)
    return RaceMetadata(
        race_name=race['Name'],
        track_name=race['Track'],
        series_name=series_name,
        end_time_epoc=race.get('EndDateEpoc', 0),
    )


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted, exiting.")
        sys.exit(130)
