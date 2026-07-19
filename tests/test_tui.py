import logging
import re

import pytest
from textual.app import App
from textual.screen import Screen
from textual.widgets import RichLog

from lemongrass._tui import LogPaneScreen, _logging_to, _race_label, _TuiLogHandler


def _info_record(message):
    return logging.LogRecord('httpx', logging.INFO, __file__, 0, message, None, None)


class TestTuiLogHandler:
    def test_emit_buffers_formatted_line(self):
        handler = _TuiLogHandler()
        handler.emit(_info_record('HTTP Request: POST /PastRaces 200 OK'))
        assert len(handler.lines) == 1
        line = handler.lines[0]
        assert 'INFO' in line
        assert 'HTTP Request: POST /PastRaces 200 OK' in line
        assert re.match(r'^\d\d:\d\d:\d\d ', line)  # HH:MM:SS, no date

    def test_buffer_is_bounded(self):
        handler = _TuiLogHandler()
        for i in range(250):
            handler.emit(_info_record(f'line {i}'))
        assert len(handler.lines) == 200
        assert 'line 249' in handler.lines[-1]


class TestLoggingTo:
    def test_swaps_in_handler_and_restores(self):
        root = logging.getLogger()
        sentinel = logging.NullHandler()
        root.addHandler(sentinel)
        handler = _TuiLogHandler()
        try:
            with _logging_to(handler):
                assert root.handlers == [handler]
            assert sentinel in root.handlers
            assert handler not in root.handlers
        finally:
            root.removeHandler(sentinel)

    def test_restores_after_exception(self):
        root = logging.getLogger()
        sentinel = logging.NullHandler()
        root.addHandler(sentinel)
        handler = _TuiLogHandler()
        try:
            with pytest.raises(RuntimeError):
                with _logging_to(handler):
                    raise RuntimeError('app crashed')
            assert sentinel in root.handlers
            assert handler not in root.handlers
        finally:
            root.removeHandler(sentinel)


class TestRaceLabel:
    def test_formats_date_name_id(self):
        label = _race_label({'ID': 42, 'Name': 'Sears Pointless', 'StartDateEpoc': 0})
        assert '#42' in label
        assert 'Sears Pointless' in label
        assert re.match(r'^\d{4}-\d\d-\d\d ', label)


class _LogScreen(LogPaneScreen, Screen):
    def compose(self):
        yield RichLog(id='log')

    def on_mount(self):
        self.set_interval(0.05, self._drain_log)


class _HostApp(App):
    def __init__(self):
        super().__init__()
        self.log_handler = _TuiLogHandler()

    def on_mount(self):
        self.push_screen(_LogScreen())


class TestLogPaneScreen:
    @pytest.mark.asyncio
    async def test_inactive_screen_does_not_drain(self):
        app = _HostApp()
        async with app.run_test() as pilot:
            await pilot.pause()
            bottom = app.screen
            top = _LogScreen()
            app.push_screen(top)          # pushes a second screen on top
            await pilot.pause()
            app.log_handler.lines.append('line-1')
            await pilot.pause(0.2)

            bottom_log = bottom.query_one('#log', RichLog)
            top_log = top.query_one('#log', RichLog)

            # the active top screen drained the line into its RichLog...
            assert len(top_log.lines) > 0
            # ...the shared deque was fully drained by that one screen...
            assert len(app.log_handler.lines) == 0
            # ...and the inactive bottom screen never wrote anything.
            assert len(bottom_log.lines) == 0
