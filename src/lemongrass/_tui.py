"""Shared Textual helpers used by the backfill and laps TUIs."""

import logging
from collections import deque
from contextlib import contextmanager
from datetime import UTC, datetime


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


def _race_label(race):
    """One checklist row: 'YYYY-MM-DD  Name  (#id)'."""
    day = datetime.fromtimestamp(race['StartDateEpoc'], tz=UTC).strftime('%Y-%m-%d')
    return f"{day}  {race['Name']}  (#{race['ID']})"
