# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import importlib.util
import pathlib
from unittest.mock import mock_open, patch

_spec = importlib.util.spec_from_file_location(
    "laps",
    pathlib.Path(__file__).parent.parent / "laps.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


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
