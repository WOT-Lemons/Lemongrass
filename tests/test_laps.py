# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import importlib.util
import pathlib
import threading
from unittest.mock import MagicMock, mock_open, patch

_spec = importlib.util.spec_from_file_location(
    "laps",
    pathlib.Path(__file__).parent.parent / "laps.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class TestIntervalArg:
    def test_default_interval_is_30(self):
        args = _mod._build_parser().parse_args(['12345', '42'])
        assert args.interval == 30

    def test_custom_interval(self):
        args = _mod._build_parser().parse_args(['12345', '42', '--interval', '60'])
        assert args.interval == 60


class TestMonitorRoutine:
    def test_uses_interval_as_wait_timeout(self):
        stop = threading.Event()
        mock_event = MagicMock()
        mock_event.wait.return_value = True  # stop after first check
        with patch.object(_mod.threading, 'Event', return_value=mock_event):
            _mod.monitor_routine('42', [], '123', '42', None, 0, MagicMock(), False, interval=45)
        mock_event.wait.assert_called_with(timeout=45)

    def test_stop_event_exits_loop(self):
        stop = threading.Event()
        stop.set()
        with patch.object(_mod, 'refresh_competitor', return_value=[{'Lap': 1}]):
            _mod.monitor_routine('42', [{'Lap': 1}], '123', '42', None, 0, MagicMock(), False, interval=30, _stop_event=stop)


class TestWriteCSV:
    def test_opens_file_with_correct_name(self):
        laps = [{"Lap": 1, "LapTime": "0:01:30.000"}]
        with patch("builtins.open", mock_open()) as m:
            _mod.write_csv("my-race", laps)
        m.assert_called_once_with("./my-race.csv", 'w', encoding='utf-8')

    def test_writes_header_and_rows(self):
        laps = [{"Lap": 1, "LapTime": "0:01:30.000"}, {"Lap": 2, "LapTime": "0:01:31.000"}]
        written = []
        with patch("builtins.open", mock_open()) as m:
            m.return_value.__enter__.return_value.write = written.append
            _mod.write_csv("race", laps)
        combined = "".join(written)
        assert "Lap" in combined
        assert "LapTime" in combined
