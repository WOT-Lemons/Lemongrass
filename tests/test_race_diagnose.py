import os
import sys
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

import lemongrass.race_diagnose as _mod


class TestMainTokenResolution:
    def _run_main(self, env):
        mock_client = MagicMock()
        mock_client.race.details.return_value = {'Successful': False}
        mock_client.results.sessions_for_race.return_value = {'Sessions': []}
        mock_influx = MagicMock()
        mock_influx.query_api.return_value = MagicMock()

        with patch.dict(os.environ, env, clear=True):
            with patch.object(sys, 'argv', ['race-diagnose', '12345', '42']):
                with patch('lemongrass.race_diagnose.RaceMonitorClient') as mock_rm_cls:
                    mock_rm_cls.return_value.__enter__.return_value = mock_client
                    with patch('lemongrass._influx.connect') as mock_connect:
                        mock_connect.return_value.__enter__.return_value = mock_influx
                        _mod.main()
        return mock_rm_cls

    @pytest.mark.parametrize('token_env, expected', [
        ({'RACEMONITOR_TOKENS': 'TOKEN1'}, 'TOKEN1'),
        ({'RACEMONITOR_TOKENS': 'TOKEN1,TOKEN2'}, ['TOKEN1', 'TOKEN2']),
        ({'RACEMONITOR_TOKEN': 'FALLBACK'}, 'FALLBACK'),
    ])
    def test_resolves_tokens_for_client(self, token_env, expected):
        mock_rm_cls = self._run_main({**token_env, 'INFLUX_TELEMETRY_TOKEN': 'ITOKEN'})
        mock_rm_cls.assert_called_once_with(api_token=expected)

    def test_exits_when_no_token_set(self):
        with patch.dict(os.environ, {'INFLUX_TELEMETRY_TOKEN': 'ITOKEN'}, clear=True):
            with patch.object(sys, 'argv', ['race-diagnose', '12345', '42']):
                with pytest.raises(SystemExit) as exc_info:
                    _mod.main()
        assert exc_info.value.code == 1

    def test_error_message_mentions_both_token_vars(self, capsys):
        with patch.dict(os.environ, {'INFLUX_TELEMETRY_TOKEN': 'ITOKEN'}, clear=True):
            with patch.object(sys, 'argv', ['race-diagnose', '12345', '42']):
                with pytest.raises(SystemExit):
                    _mod.main()
        out = capsys.readouterr().out
        assert 'RACEMONITOR_TOKENS' in out
        assert 'RACEMONITOR_TOKEN' in out

    def test_no_token_message_honors_configured_pool_var(self, capsys, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
        env = {
            'INFLUX_TELEMETRY_TOKEN': 'ITOKEN',
            'RACEMONITOR_TOKEN': 'stale-legacy',
            'LEMONGRASS_CONFIG': str(cfg),
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(sys, 'argv', ['race-diagnose', '12345', '42']):
                with pytest.raises(SystemExit) as exc_info:
                    _mod.main()
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'MY_POOL not set' in out

    def test_honors_configured_token_env_var(self, capsys, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text('[influx]\ntoken_env = "MY_INFLUX_TOKEN"\n')
        env = {
            'RACEMONITOR_TOKENS': 'TOKEN1',
            'LEMONGRASS_CONFIG': str(cfg),
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(sys, 'argv', ['race-diagnose', '12345', '42']):
                with pytest.raises(SystemExit) as exc_info:
                    _mod.main()
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'MY_INFLUX_TOKEN not set' in out


class TestInputValidation:
    def test_race_id_with_quote_exits_before_any_query(self):
        import lemongrass.race_diagnose as rd
        with patch.object(sys, 'argv', ['lemongrass-race-diagnose', 'x"y', '42']):
            with pytest.raises(SystemExit) as exc:
                rd.main()
        assert exc.value.code == 1


class TestWindowPadding:
    def test_range_padded_one_day_each_side(self):
        """Flux stop is exclusive and lap timestamps are session-anchored, so a
        session running past the nominal EndDateEpoc would be invisible to
        diagnose, mis-reporting a write bug."""
        import lemongrass.race_diagnose as rd
        query_api = MagicMock()
        query_api.query.return_value = []
        start = 864000   # 1970-01-11
        end = 950400     # 1970-01-12
        rd.diagnose_influx(query_api, '999', '42', start_epoc=start, end_epoc=end)
        flux = query_api.query.call_args.args[0]
        assert 'start: 1970-01-10T00:00:00Z' in flux
        assert 'stop: 1970-01-13T00:00:00Z' in flux


class TestEpocToStr:
    def test_zero_returns_sentinel(self):
        assert _mod.epoc_to_str(0) == '0 (zero/null)'

    def test_none_returns_sentinel(self):
        assert _mod.epoc_to_str(None) == 'None (zero/null)'

    def test_normal_epoch_formats_utc(self):
        epoc = 1600000000
        expected = datetime.fromtimestamp(epoc, tz=UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
        assert _mod.epoc_to_str(epoc) == expected
        assert _mod.epoc_to_str(epoc).endswith('UTC')


class TestDiagnoseApi:
    def test_happy_path_counts_matching_car_laps(self, capsys):
        client = MagicMock()
        client.race.details.return_value = {
            'Successful': True,
            'Race': {'Name': 'Lemons 2026', 'StartDateEpoc': 1600000000,
                     'EndDateEpoc': 1600100000},
        }
        client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 'S1'}]}
        client.results.session_details.return_value = {
            'Session': {
                'SessionStartDateEpoc': 1600000500,
                'SortedCompetitors': [
                    {'Number': '99', 'LapTimes': [1, 2]},
                    {'Number': '42', 'LapTimes': [10, 20, 30]},
                ],
            }
        }
        start_epoc, end_epoc = _mod.diagnose_api(client, '144185', '42')
        out = capsys.readouterr().out
        assert (start_epoc, end_epoc) == (1600000000, 1600100000)
        assert 'Lemons 2026' in out
        assert 'Session S1' in out
        assert 'car 42 laps=3' in out
        assert 'Total laps for car 42 across all sessions: 3' in out


class TestDiagnoseInflux:
    def _query_api(self, records):
        api = MagicMock()
        table = MagicMock()
        table.records = records
        api.query.return_value = [table]
        return api

    def _record(self, lap_no, time):
        rec = MagicMock()
        rec.get_value.return_value = lap_no
        rec.get_time.return_value = time
        return rec

    def test_prints_stored_laps_when_records_present(self, capsys):
        t1 = datetime(2020, 1, 1, tzinfo=UTC)
        t2 = datetime(2020, 1, 1, 1, tzinfo=UTC)
        api = self._query_api([self._record(1, t1), self._record(2, t2)])
        _mod.diagnose_influx(api, '999', '42', start_epoc=1600000000, end_epoc=1600100000)
        out = capsys.readouterr().out
        assert 'Stored laps: 2' in out
        assert 'Lap numbers stored: [1, 2]' in out

    def test_prints_no_laps_when_empty(self, capsys):
        api = self._query_api([])
        _mod.diagnose_influx(api, '999', '42', start_epoc=1600000000, end_epoc=1600100000)
        assert 'No laps found in InfluxDB.' in capsys.readouterr().out

    def test_zero_start_epoc_uses_epoch_start(self):
        api = self._query_api([])
        _mod.diagnose_influx(api, '999', '42', start_epoc=0, end_epoc=1600100000)
        flux = api.query.call_args.args[0]
        assert f'start: {_mod.EPOCH_START}' in flux

    def test_zero_end_epoc_warns_and_uses_now(self, capsys):
        api = self._query_api([])
        now = datetime.now(UTC)
        _mod.diagnose_influx(api, '999', '42', start_epoc=1600000000, end_epoc=0)
        out = capsys.readouterr().out
        assert 'end_time_epoc not set' in out
        flux = api.query.call_args.args[0]
        # stop falls back to now(), not a fixed epoch derived from end_epoc
        assert f'stop: {now.strftime("%Y")}' in flux
