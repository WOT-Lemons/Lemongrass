import importlib.util
import json
import pathlib
from unittest.mock import MagicMock, mock_open, patch

_spec = importlib.util.spec_from_file_location(
    "laps",
    pathlib.Path(__file__).parent.parent / "laps.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class TestCallRaceMonitor:
    def setup_method(self):
        _mod.requests.post.reset_mock()

    def _mock_response(self, status_code, body):
        r = MagicMock()
        r.status_code = status_code
        r.text = json.dumps(body)
        return r

    def test_returns_parsed_json_on_success(self):
        body = {"Successful": True, "Race": {"Name": "24hrs"}}
        _mod.requests.post.return_value = self._mock_response(200, body)
        result = _mod.call_race_monitor('/v2/Race/RaceDetails', {'apiToken': 'x'})
        assert result == body

    def test_returns_none_on_error_status(self):
        _mod.requests.post.return_value = self._mock_response(500, {})
        result = _mod.call_race_monitor('/v2/Race/RaceDetails', {'apiToken': 'x'})
        assert result is None

    def test_retries_on_rate_limit(self):
        ok = self._mock_response(200, {"Successful": True})
        throttled = self._mock_response(429, {})
        _mod.requests.post.side_effect = [throttled, ok]
        with patch.object(_mod.time, 'sleep'):
            result = _mod.call_race_monitor('/v2/Race/RaceDetails', {'apiToken': 'x'})
        assert result == {"Successful": True}
        assert _mod.requests.post.call_count == 2


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
            m.return_value.__enter__.return_value.write = lambda s: written.append(s)
            _mod.write_csv("race", laps)
        combined = "".join(written)
        assert "Lap" in combined
        assert "LapTime" in combined
