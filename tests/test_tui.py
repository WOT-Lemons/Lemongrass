import logging
import re
import sys
import threading

import pytest
from textual.app import App
from textual.screen import Screen
from textual.widgets import RichLog

from lemongrass._tui import (
    LogPaneScreen,
    _current_sink,
    _logging_to,
    _LogSink,
    _race_label,
    _routed_output,
    _RoutedLogHandler,
    _RoutedStdout,
    _sink_bound,
    _TuiLogHandler,
)


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
        self._init_sink()
        self.set_interval(0.25, self._drain_log)


class _HostApp(App):
    def on_mount(self):
        self.push_screen(_LogScreen())


class TestLogPaneScreen:
    @pytest.mark.asyncio
    async def test_drains_own_sink_into_pane(self):
        app = _HostApp()
        async with app.run_test() as pilot:
            app.screen.sink.write_line('mine')
            await pilot.pause(0.3)
            log = app.screen.query_one('#log', RichLog)
            assert any('mine' in str(line) for line in log.lines)


class TestLogSink:
    def test_write_line_appends_whole_line(self):
        sink = _LogSink()
        sink.write_line('hello')
        assert list(sink.lines) == ['hello']

    def test_write_buffers_until_newline(self):
        sink = _LogSink()
        sink.write('par')
        assert list(sink.lines) == []
        sink.write('tial\nnext')
        assert list(sink.lines) == ['partial']

    def test_flush_emits_trailing_partial(self):
        sink = _LogSink()
        sink.write('no newline')
        sink.flush()
        assert list(sink.lines) == ['no newline']

    def test_flush_noop_when_buffer_empty(self):
        sink = _LogSink()
        sink.write('done\n')
        sink.flush()
        assert list(sink.lines) == ['done']


class TestSinkBound:
    def test_binds_and_unbinds_current_thread(self):
        sink = _LogSink()
        assert _current_sink() is None
        with _sink_bound(sink):
            assert _current_sink() is sink
        assert _current_sink() is None

    def test_installs_and_restores_stdout_refcounted(self):
        original = sys.stdout
        sink_a, sink_b = _LogSink(), _LogSink()
        with _sink_bound(sink_a):
            assert isinstance(sys.stdout, _RoutedStdout)
            router = sys.stdout
            with _sink_bound(sink_b):  # same thread: rebinds sink, refcount to 2
                assert sys.stdout is router
            assert sys.stdout is router  # still installed at depth 1
        assert sys.stdout is original  # restored at depth 0

    def test_print_routes_to_bound_thread_sink(self):
        sink = _LogSink()
        with _sink_bound(sink):
            print('routed line')
        assert 'routed line' in list(sink.lines)


class TestRoutedStdout:
    def test_falls_back_to_real_when_unbound(self):
        captured = []
        real = type('R', (), {'write': lambda self, t: captured.append(t),
                              'flush': lambda self: None})()
        router = _RoutedStdout(real)
        router.write('unbound')  # no _sink_bound active on this thread
        assert captured == ['unbound']

    def test_getattr_delegates_to_real(self):
        real = sys.__stdout__
        router = _RoutedStdout(real)
        assert router.isatty() == real.isatty()


class TestRoutedLogHandler:
    def test_routes_record_to_bound_sink(self):
        sink = _LogSink()
        handler = _RoutedLogHandler()
        record = logging.LogRecord('x', logging.INFO, __file__, 1, 'msg', None, None)
        with _sink_bound(sink):
            handler.emit(record)
        assert any('msg' in line for line in sink.lines)

    def test_drops_record_when_unbound(self):
        handler = _RoutedLogHandler()
        record = logging.LogRecord('x', logging.INFO, __file__, 1, 'dropped', None, None)
        handler.emit(record)  # no sink bound → dropped, no exception

    def test_concurrent_sinks_do_not_cross_contaminate(self):
        sink_main = _LogSink()
        other = {}

        def worker():
            s = _LogSink()
            with _sink_bound(s):
                logging.getLogger().info('from-worker')
            other['sink'] = s

        with _routed_output():
            with _sink_bound(sink_main):
                t = threading.Thread(target=worker)
                t.start()
                t.join()
                logging.getLogger().info('from-main')
        assert any('from-main' in line for line in sink_main.lines)
        assert all('from-main' not in line for line in other['sink'].lines)
        assert any('from-worker' in line for line in other['sink'].lines)


class TestRoutedOutput:
    def test_installs_and_restores_root_handlers(self):
        root = logging.getLogger()
        before = root.handlers[:]
        with _routed_output():
            assert any(isinstance(h, _RoutedLogHandler) for h in root.handlers)
        assert root.handlers == before
