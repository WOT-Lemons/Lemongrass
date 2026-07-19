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


class _LogSink:
    """A screen-private bounded line buffer. Its worker's print() (line-buffered via
    write) and log records (whole lines via write_line) append here; the owning
    screen drains on a timer. deque.append is atomic → safe to feed from a worker
    thread."""

    def __init__(self):
        self.lines = deque(maxlen=200)
        self._buf = ''

    def write_line(self, line):
        """Append a whole formatted line (logging path / explicit progress)."""
        self.lines.append(line)

    def write(self, text):
        """Line-buffer a stdout stream, appending each completed line."""
        self._buf += text
        while '\n' in self._buf:
            line, self._buf = self._buf.split('\n', 1)
            self.lines.append(line)

    def flush(self):
        """Emit any trailing partial line."""
        if self._buf:
            self.lines.append(self._buf)
            self._buf = ''


_thread_sinks = {}                       # thread ident → _LogSink
_route_lock = threading.Lock()           # momentary: guards the registry + stdout install
_stdout_depth = 0                        # refcount of active _sink_bound scopes
_routed_stdout = None                    # the shared _RoutedStdout while depth > 0


def _current_sink():
    """The _LogSink bound to the calling thread, or None."""
    return _thread_sinks.get(threading.get_ident())


class _RoutedStdout:
    """sys.stdout proxy: route a worker-thread write to that thread's sink; fall back
    to the captured underlying stream (Textual's capture) off bound threads, so the
    UI thread's own prints keep their normal Textual handling. __getattr__ delegates
    any other attribute (isatty/fileno/encoding/writelines) to _real."""

    def __init__(self, real):
        self._real = real

    def write(self, text):
        sink = _current_sink()
        (sink.write if sink is not None else self._real.write)(text)

    def flush(self):
        if _current_sink() is None:
            self._real.flush()

    def __getattr__(self, name):
        return getattr(self._real, name)


@contextmanager
def _sink_bound(sink):
    """Bind `sink` to the current thread for logging AND print routing for the
    duration. The first concurrent binder installs the shared _RoutedStdout as
    sys.stdout (capturing whatever is current — Textual's capture — as its fallback);
    the last unbinder restores it. The lock is held only across bind/unbind, never
    during the wrapped work, so a long worker never serializes another screen's
    worker."""
    global _stdout_depth, _routed_stdout
    ident = threading.get_ident()
    with _route_lock:
        _thread_sinks[ident] = sink
        if _stdout_depth == 0:
            _routed_stdout = _RoutedStdout(sys.stdout)
            sys.stdout = _routed_stdout
        _stdout_depth += 1
    try:
        yield
    finally:
        with _route_lock:
            _stdout_depth -= 1
            if _stdout_depth == 0:
                sys.stdout = _routed_stdout._real
                _routed_stdout = None
            _thread_sinks.pop(ident, None)


class _RoutedLogHandler(logging.Handler):
    """Root handler: dispatch each record to the sink bound to the emitting thread.
    Records from the UI thread or an unbound worker are dropped (never written to the
    real terminal, which would corrupt the Textual display)."""

    def __init__(self):
        super().__init__(level=logging.INFO)
        self.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record):
        """Route the formatted record to the emitting thread's sink, if any."""
        sink = _current_sink()
        if sink is not None:
            sink.write_line(self.format(record))


@contextmanager
def _routed_output():
    """Install _RoutedLogHandler at root for one app run; restore the previous root
    handlers on exit even if the app crashes. Replaces _logging_to. Does NOT touch
    sys.stdout — print routing is established per-worker by _sink_bound.

    Also ensures the root logger's own level admits INFO records for the duration,
    restoring the previous level on exit: the root logger's default level (WARNING)
    filters records before they ever reach a handler, no matter the handler's own
    level, so without this an unconfigured process (no prior launch_tui()
    basicConfig) would silently drop every routed INFO line. Mirrors the same
    "the app-run entry point owns the root level" pattern race_backfill.main() uses
    (see tests/test_race_backfill.py::TestMainConfiguresLogging)."""
    root = logging.getLogger()
    saved = root.handlers[:]
    saved_level = root.level
    for existing in saved:
        root.removeHandler(existing)
    handler = _RoutedLogHandler()
    root.addHandler(handler)
    if root.level == logging.NOTSET or root.level > logging.INFO:
        root.setLevel(logging.INFO)
    try:
        yield
    finally:
        root.removeHandler(handler)
        for existing in saved:
            root.addHandler(existing)
        root.setLevel(saved_level)


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
