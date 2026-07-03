import os
import sys
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

    def test_uses_racemonitor_tokens_when_set(self):
        mock_rm_cls = self._run_main({
            'RACEMONITOR_TOKENS': 'TOKEN1',
            'INFLUX_TELEMETRY_TOKEN': 'ITOKEN',
        })
        mock_rm_cls.assert_called_once_with(api_token='TOKEN1')

    def test_uses_multi_token_list_when_multiple_tokens_set(self):
        mock_rm_cls = self._run_main({
            'RACEMONITOR_TOKENS': 'TOKEN1,TOKEN2',
            'INFLUX_TELEMETRY_TOKEN': 'ITOKEN',
        })
        mock_rm_cls.assert_called_once_with(api_token=['TOKEN1', 'TOKEN2'])

    def test_falls_back_to_racemonitor_token(self):
        mock_rm_cls = self._run_main({
            'RACEMONITOR_TOKEN': 'FALLBACK',
            'INFLUX_TELEMETRY_TOKEN': 'ITOKEN',
        })
        mock_rm_cls.assert_called_once_with(api_token='FALLBACK')

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
