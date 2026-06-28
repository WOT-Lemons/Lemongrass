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
