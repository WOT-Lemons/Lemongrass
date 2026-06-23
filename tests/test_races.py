import sys
from unittest.mock import MagicMock, patch
import pytest

import lemongrass.races as _mod


class TestDispatch:
    def test_unknown_subcommand_exits_nonzero(self):
        with patch.object(sys, 'argv', ['lemongrass-races', 'notasubcommand']):
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        assert exc.value.code != 0

    def test_no_args_exits_nonzero(self):
        with patch.object(sys, 'argv', ['lemongrass-races']):
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        assert exc.value.code != 0

    def test_routes_to_list(self):
        mock_list = MagicMock()
        with patch.object(sys, 'argv', ['lemongrass-races', 'list']):
            with patch.object(_mod, '_handle_list', mock_list):
                _mod.main()
        mock_list.assert_called_once()

    def test_routes_to_prune(self):
        mock_prune = MagicMock()
        with patch.object(sys, 'argv', ['lemongrass-races', 'prune', '12345']):
            with patch.object(_mod, '_handle_prune', mock_prune):
                _mod.main()
        mock_prune.assert_called_once()

    def test_pops_subcommand_from_argv(self):
        captured = {}

        def capture():
            captured['argv'] = sys.argv[:]

        with patch.object(sys, 'argv', ['lemongrass-races', 'list']):
            with patch.object(_mod, '_handle_list', capture):
                _mod.main()
        assert 'list' not in captured['argv']
        assert captured['argv'][0] == 'lemongrass-races-list'


class TestHandlePrune:
    def _make_influx_client(self, races=None):
        if races is None:
            races = {'12345': 'Test Race'}
        client = MagicMock()
        query_api = MagicMock()

        def fake_query(flux):
            tables = []
            for race_id, race_name in races.items():
                table = MagicMock()
                rec = MagicMock()
                rec.values = {'race_id': race_id, 'race_name': race_name}
                table.records = [rec]
                tables.append(table)
            return tables

        query_api.query.side_effect = fake_query
        client.query_api.return_value = query_api
        client.delete_api.return_value = MagicMock()
        client.__enter__ = lambda s: client
        client.__exit__ = MagicMock(return_value=False)
        return client

    def test_prune_with_yes_skips_prompt(self, capsys):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345', '--yes']):
            with patch('lemongrass.races.InfluxDBClient',
                       return_value=self._make_influx_client()):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        out = capsys.readouterr().out
        assert 'Deleted laps' in out
        assert 'Deleted race metadata' in out
        assert 'Deleted sessions' in out

    def test_prune_aborts_on_no_confirmation(self, capsys):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345']):
            with patch('lemongrass.races.InfluxDBClient',
                       return_value=self._make_influx_client()):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    with patch('builtins.input', return_value='n'):
                        with pytest.raises(SystemExit) as exc:
                            _mod._handle_prune()
        assert exc.value.code == 0
        assert 'Aborted' in capsys.readouterr().out

    def test_prune_deletes_from_all_three_buckets(self):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345', '--yes']):
            fake_client = self._make_influx_client()
            with patch('lemongrass.races.InfluxDBClient', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        delete_api = fake_client.delete_api.return_value
        buckets_deleted = [c.kwargs.get('bucket') or c.args[2]
                          for c in delete_api.delete.call_args_list]
        assert 'laps' in buckets_deleted
        assert 'races' in buckets_deleted
        assert 'race_sessions' in buckets_deleted

    def test_prune_exits_when_no_influx_token(self):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345', '--yes']):
            with patch.dict('os.environ', {}, clear=True):
                with pytest.raises(SystemExit) as exc:
                    _mod._handle_prune()
        assert exc.value.code != 0

    def test_prune_rejects_invalid_race_id(self, capsys):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', 'bad id!']):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                with pytest.raises(SystemExit) as exc:
                    _mod._handle_prune()
        assert exc.value.code != 0

    def test_prune_rejects_multiple_invalid_race_ids(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', 'bad id!', 'also bad!']):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                with pytest.raises(SystemExit) as exc:
                    _mod._handle_prune()
        assert exc.value.code != 0

    def test_prune_reports_all_invalid_ids(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', 'bad id!', 'also bad!']):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                with pytest.raises(SystemExit):
                    _mod._handle_prune()
        # both bad IDs should appear in stderr
        err = capsys.readouterr().err
        assert 'bad id!' in err
        assert 'also bad!' in err

    def test_prune_aborts_when_race_not_found_in_influx(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '99999', '--yes']):
            with patch('lemongrass.races.InfluxDBClient',
                       return_value=self._make_influx_client(races={})):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    with pytest.raises(SystemExit) as exc:
                        _mod._handle_prune()
        assert exc.value.code != 0
        assert '99999' in capsys.readouterr().err

    def test_prune_reports_all_not_found_ids(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '11111', '22222', '--yes']):
            with patch('lemongrass.races.InfluxDBClient',
                       return_value=self._make_influx_client(races={})):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    with pytest.raises(SystemExit) as exc:
                        _mod._handle_prune()
        assert exc.value.code != 0
        err = capsys.readouterr().err
        assert '11111' in err
        assert '22222' in err

    def test_prune_multi_shows_summary_before_confirm(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '12345', '67890']):
            races = {'12345': 'Le Mans 2026', '67890': 'Sebring 2025'}
            with patch('lemongrass.races.InfluxDBClient',
                       return_value=self._make_influx_client(races=races)):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    with patch('builtins.input', return_value='n'):
                        with pytest.raises(SystemExit):
                            _mod._handle_prune()
        out = capsys.readouterr().out
        assert '12345' in out
        assert 'Le Mans 2026' in out
        assert '67890' in out
        assert 'Sebring 2025' in out

    def test_prune_multi_deletes_all_races_with_yes(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '12345', '67890', '--yes']):
            races = {'12345': 'Le Mans 2026', '67890': 'Sebring 2025'}
            fake_client = self._make_influx_client(races=races)
            with patch('lemongrass.races.InfluxDBClient', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        out = capsys.readouterr().out
        assert out.count('Deleted laps') == 2
        assert out.count('Deleted race metadata') == 2
        assert out.count('Deleted sessions') == 2

    def test_prune_multi_deletes_all_three_buckets_per_race(self):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '12345', '67890', '--yes']):
            races = {'12345': 'Le Mans 2026', '67890': 'Sebring 2025'}
            fake_client = self._make_influx_client(races=races)
            with patch('lemongrass.races.InfluxDBClient', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        delete_api = fake_client.delete_api.return_value
        assert delete_api.delete.call_count == 6  # 3 buckets × 2 races
        buckets = [c.kwargs.get('bucket') for c in delete_api.delete.call_args_list]
        assert buckets.count('laps') == 2
        assert buckets.count('races') == 2
        assert buckets.count('race_sessions') == 2


class TestHandleBackfill:
    def test_delegates_to_race_backfill_main(self):
        mock_main = MagicMock()
        with patch.object(sys, 'argv', ['lemongrass-races-backfill', '--dry-run']):
            with patch('lemongrass.race_backfill.main', mock_main):
                _mod._handle_backfill()
        mock_main.assert_called_once()

    def test_argv_passed_through_to_race_backfill(self):
        captured = {}

        def capture():
            captured['argv'] = sys.argv[:]

        with patch.object(sys, 'argv', ['lemongrass-races-backfill', '--force']):
            with patch('lemongrass.race_backfill.main', capture):
                _mod._handle_backfill()
        assert '--force' in captured['argv']

    def test_routes_backfill_through_main_dispatch(self):
        mock_main = MagicMock()
        with patch.object(sys, 'argv', ['lemongrass-races', 'backfill', '--dry-run']):
            with patch('lemongrass.race_backfill.main', mock_main):
                _mod.main()
        mock_main.assert_called_once()

    def test_upgrade_stored_flag_reaches_race_backfill(self):
        captured = {}

        def capture():
            captured['argv'] = sys.argv[:]

        with patch.object(sys, 'argv',
                          ['lemongrass-races', 'backfill', '--upgrade-stored']):
            with patch('lemongrass.race_backfill.main', capture):
                _mod.main()
        assert '--upgrade-stored' in captured['argv']


class TestHandleList:
    def _make_client(self, races, totals, currents):
        """Build mock InfluxDB client for _handle_list.

        races:    list of (race_id, race_name, date_str) e.g. ('R1', 'My Race', '2026-01-15')
        totals:   dict {race_id: total_lap_count}
        currents: dict {race_id: current_schema_lap_count}
        """
        from datetime import datetime, timezone

        def fake_query(flux):
            if 'bucket: "races"' in flux:
                tables = []
                for race_id, race_name, date_str in races:
                    table = MagicMock()
                    rec = MagicMock()
                    rec.values = {'race_id': race_id, 'race_name': race_name}
                    rec.get_time.return_value = datetime.strptime(
                        date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    table.records = [rec]
                    tables.append(table)
                return tables
            elif '"lap_no"' in flux:
                tables = []
                for race_id, count in totals.items():
                    table = MagicMock()
                    rec = MagicMock()
                    rec.values = {'race_id': race_id}
                    rec.get_value.return_value = count
                    table.records = [rec]
                    tables.append(table)
                return tables
            else:
                tables = []
                for race_id, count in currents.items():
                    table = MagicMock()
                    rec = MagicMock()
                    rec.values = {'race_id': race_id}
                    rec.get_value.return_value = count
                    table.records = [rec]
                    tables.append(table)
                return tables

        client = MagicMock()
        query_api = MagicMock()
        query_api.query.side_effect = fake_query
        client.query_api.return_value = query_api
        client.__enter__ = lambda s: client
        client.__exit__ = MagicMock(return_value=False)
        return client

    def test_no_laps_schema_state(self, capsys):
        client = self._make_client(
            races=[('R1', 'Empty Race', '2026-01-01')],
            totals={},
            currents={},
        )
        with patch('lemongrass.races.InfluxDBClient', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        assert 'no laps' in capsys.readouterr().out

    def test_current_schema_state(self, capsys):
        from lemongrass.laps import SCHEMA_VERSION
        client = self._make_client(
            races=[('R1', 'Full Race', '2026-01-01')],
            totals={'R1': 50},
            currents={'R1': 50},
        )
        with patch('lemongrass.races.InfluxDBClient', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        assert f'current (v{SCHEMA_VERSION})' in capsys.readouterr().out

    def test_stale_schema_state(self, capsys):
        client = self._make_client(
            races=[('R1', 'Old Race', '2026-01-01')],
            totals={'R1': 50},
            currents={'R1': 20},
        )
        with patch('lemongrass.races.InfluxDBClient', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        out = capsys.readouterr().out
        assert 'stale' in out
        assert '20/50' in out

    def test_sorted_newest_first(self, capsys):
        client = self._make_client(
            races=[
                ('R1', 'Old Race', '2024-06-01'),
                ('R2', 'New Race', '2026-06-01'),
            ],
            totals={'R1': 10, 'R2': 10},
            currents={'R1': 10, 'R2': 10},
        )
        with patch('lemongrass.races.InfluxDBClient', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        out = capsys.readouterr().out
        assert out.index('New Race') < out.index('Old Race')

    def test_exits_when_no_influx_token(self):
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(SystemExit) as exc:
                _mod._handle_list()
        assert exc.value.code != 0


class TestHandleDiagnose:
    def test_delegates_to_race_diagnose_main(self):
        mock_main = MagicMock()
        with patch.object(sys, 'argv',
                          ['lemongrass-races-diagnose', '12345', '42']):
            with patch('lemongrass.race_diagnose.main', mock_main):
                _mod._handle_diagnose()
        mock_main.assert_called_once()

    def test_argv_passed_through_to_race_diagnose(self):
        captured = {}

        def capture():
            captured['argv'] = sys.argv[:]

        with patch.object(sys, 'argv',
                          ['lemongrass-races-diagnose', '12345', '42']):
            with patch('lemongrass.race_diagnose.main', capture):
                _mod._handle_diagnose()
        assert '12345' in captured['argv']
        assert '42' in captured['argv']

    def test_routes_diagnose_through_main_dispatch(self):
        mock_main = MagicMock()
        with patch.object(sys, 'argv',
                          ['lemongrass-races', 'diagnose', '12345', '42']):
            with patch('lemongrass.race_diagnose.main', mock_main):
                _mod.main()
        mock_main.assert_called_once()
