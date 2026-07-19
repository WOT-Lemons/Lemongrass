"""Shared Textual helpers used by the backfill and laps TUIs."""

import logging
import sys
import threading
from collections import deque
from contextlib import contextmanager
from datetime import UTC, datetime

from textual.widgets import RichLog


def launch_tui(run_fn):
    """Shared bootstrap for the bare-`lemongrass` / `races` / `laps` TTY entry
    points: configure INFO logging, resolve the API token (exit with a hint if
    unset), open a RaceMonitorClient, and hand it to run_fn (a TUI runner
    returning an exit code). Always raises SystemExit — never returns.

    Callers gate this behind their own TTY check so basicConfig stays scoped to
    the interactive branch and never disturbs the argument-parsed paths."""
    from race_monitor import RaceMonitorClient

    from lemongrass import _env
    from lemongrass._env import resolve_tokens

    logging.basicConfig(level=logging.INFO)
    tokens = resolve_tokens()
    if not tokens:
        print(f"{_env.tokens_env_hint()} not set", file=sys.stderr)
        sys.exit(1)
    with RaceMonitorClient(api_token=tokens) as client:
        sys.exit(run_fn(client))

# Guards contextlib.redirect_stdout, which mutates process-global sys.stdout. The
# import / diagnose / backfill workers all redirect; serialize so concurrent workers
# can't clobber each other's restore and leak print() to the real terminal.
_STDOUT_LOCK = threading.Lock()


class _TuiLogHandler(logging.Handler):
    """Buffer formatted log records for a TUI log pane.

    emit() only appends to a deque, so it is safe from any thread — worker
    threads' httpx and rate-limiter records included. The app drains the buffer
    into a RichLog on a timer; nothing here may touch Textual.
    """

    def __init__(self):
        super().__init__(level=logging.INFO)
        self.lines = deque(maxlen=200)
        self.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record):
        """Append the formatted record to the bounded buffer."""
        self.lines.append(self.format(record))


@contextmanager
def _logging_to(handler):
    """Route root logging exclusively to handler for the duration.

    The terminal handlers would corrupt the Textual display; they are restored
    on exit even if the app crashes, so post-TUI logging prints normally.
    """
    root = logging.getLogger()
    saved = root.handlers[:]
    for existing in saved:
        root.removeHandler(existing)
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        for existing in saved:
            root.addHandler(existing)


class LogPaneScreen:
    """Screen mixin: drain the app's shared log buffer into this screen's '#log'
    RichLog, but ONLY while this screen is the active (topmost) one.

    One _TuiLogHandler deque is shared app-wide; without this guard two mounted
    screens' drain timers would each pop from it and steal half the other's lines.
    Screens using this call self.set_interval(0.25, self._drain_log) in on_mount.
    """

    def _drain_log(self):
        """Pop buffered lines into this screen's RichLog, skipping if inactive."""
        if self.app.screen is not self:
            return
        log_view = self.query_one('#log', RichLog)
        while self.app.log_handler.lines:
            log_view.write(self.app.log_handler.lines.popleft())


def _race_label(race):
    """One checklist row: 'YYYY-MM-DD  Name  (#id)'."""
    day = datetime.fromtimestamp(race['StartDateEpoc'], tz=UTC).strftime('%Y-%m-%d')
    return f"{day}  {race['Name']}  (#{race['ID']})"
