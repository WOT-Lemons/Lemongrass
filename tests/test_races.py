import sys
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

import lemongrass.races as _mod
from lemongrass.races import fetch_race_rows, prune_races


class TestFetchRaceRows:
    def _query_api(self):
        """Fake query_api: three queries in order — races meta, total laps, current-schema laps."""
        def _rec(values, value=None, time=None):
            rec = MagicMock()
            rec.values = values
            rec.get_value.return_value = value
            rec.get_time.return_value = time
            return rec

        def _table(records):
            t = MagicMock()
            t.records = records
            return t

        meta = _table([_rec(
            {'race_id': '144185', 'race_name': 'Sears Pointless'},
            time=datetime(2026, 6, 1, tzinfo=UTC))])
        totals = _table([_rec({'race_id': '144185'}, value=100)])
        current = _table([_rec({'race_id': '144185'}, value=100)])

        api = MagicMock()
        api.query.side_effect = [[meta], [totals], [current]]
        return api

    def test_returns_row_with_counts(self):
        rows = fetch_race_rows(self._query_api())
        assert len(rows) == 1
        assert rows[0]['race_id'] == '144185'
        assert rows[0]['name'] == 'Sears Pointless'
        assert rows[0]['date'] == '2026-06-01'
        assert rows[0]['total'] == 100
        assert rows[0]['current'] == 100


class TestDispatch:
    def test_unknown_subcommand_exits_nonzero(self):
        with patch.object(sys, 'argv', ['lemongrass-races', 'notasubcommand']):
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        assert exc.value.code != 0

    def test_no_args_exits_nonzero(self):
        with patch.object(sys, 'argv', ['lemongrass-races']):
            with patch.object(sys.stdin, 'isatty', return_value=False):
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


class TestPruneRaces:
    def test_deletes_metadata_last_and_reports_progress(self):
        delete_api = MagicMock()
        progress = []
        failed = prune_races(delete_api, ['144185'], on_progress=progress.append)
        assert failed == []
        # metadata (race measurement) delete is the last call
        preds = [c.kwargs['predicate'] for c in delete_api.delete.call_args_list]
        assert preds[-1].startswith('_measurement="race"')
        assert len(progress) == 4

    def test_failure_is_collected_not_raised(self):
        delete_api = MagicMock()
        delete_api.delete.side_effect = RuntimeError('boom')
        failed = prune_races(delete_api, ['144185'])
        assert failed == ['144185']

    def test_failure_reported_via_on_error_when_provided(self):
        delete_api = MagicMock()
        delete_api.delete.side_effect = RuntimeError('boom')
        progress = []
        errors = []
        failed = prune_races(delete_api, ['144185'],
                             on_progress=progress.append, on_error=errors.append)
        assert failed == ['144185']
        assert len(errors) == 1
        assert 'error pruning race 144185' in errors[0]
        assert 'boom' in errors[0]
        # the error line must not also be duplicated onto on_progress
        assert not any('error pruning race' in m for m in progress)

    def test_failure_falls_back_to_on_progress_when_no_on_error(self):
        delete_api = MagicMock()
        delete_api.delete.side_effect = RuntimeError('boom')
        progress = []
        failed = prune_races(delete_api, ['144185'], on_progress=progress.append)
        assert failed == ['144185']
        assert any('error pruning race 144185' in m for m in progress)


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
            with patch('lemongrass._influx.connect',
                       return_value=self._make_influx_client()):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        out = capsys.readouterr().out
        assert 'Deleted laps' in out
        assert 'Deleted race metadata' in out
        assert 'Deleted sessions' in out

    def test_prune_aborts_on_no_confirmation(self, capsys):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345']):
            with patch('lemongrass._influx.connect',
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
            with patch('lemongrass._influx.connect', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        delete_api = fake_client.delete_api.return_value
        buckets_deleted = [c.kwargs.get('bucket') or c.args[2]
                          for c in delete_api.delete.call_args_list]
        assert 'laps' in buckets_deleted
        assert 'races' in buckets_deleted
        assert 'race_sessions' in buckets_deleted

    def test_prune_deletes_standings_measurement(self):
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345', '--yes']):
            fake_client = self._make_influx_client()
            with patch('lemongrass._influx.connect', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        delete_api = fake_client.delete_api.return_value
        predicates = [c.kwargs.get('predicate') for c in delete_api.delete.call_args_list]
        assert any('_measurement="standings"' in p and 'race_id="12345"' in p
                   for p in predicates)

    def test_prune_deletes_race_metadata_last(self):
        # The not-found guard keys off the race measurement, so a retry after a
        # partial failure only works if race metadata is the last thing deleted.
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345', '--yes']):
            fake_client = self._make_influx_client()
            with patch('lemongrass._influx.connect', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        delete_api = fake_client.delete_api.return_value
        predicates = [c.kwargs.get('predicate') for c in delete_api.delete.call_args_list]
        assert '_measurement="race"' in predicates[-1]

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

    def test_prune_rejects_multiple_invalid_race_ids(self):
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
        err = capsys.readouterr().err
        assert '"bad id!"' in err
        assert '"also bad!"' in err

    def test_prune_rejects_mix_of_valid_and_invalid_ids(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', 'valid-id', 'bad id!']):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                with pytest.raises(SystemExit) as exc:
                    _mod._handle_prune()
        assert exc.value.code != 0
        err = capsys.readouterr().err
        assert '"bad id!"' in err
        assert 'valid-id' not in err

    def test_prune_aborts_when_race_not_found_in_influx(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '99999', '--yes']):
            with patch('lemongrass._influx.connect',
                       return_value=self._make_influx_client(races={})):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    with pytest.raises(SystemExit) as exc:
                        _mod._handle_prune()
        assert exc.value.code != 0
        assert '99999' in capsys.readouterr().err

    def test_prune_reports_all_not_found_ids(self, capsys):
        with patch.object(sys, 'argv',
                          ['lemongrass-races-prune', '11111', '22222', '--yes']):
            with patch('lemongrass._influx.connect',
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
            with patch('lemongrass._influx.connect',
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
            with patch('lemongrass._influx.connect', return_value=fake_client):
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
            with patch('lemongrass._influx.connect', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    _mod._handle_prune()
        delete_api = fake_client.delete_api.return_value
        # 4 deletes (race, session, lap, standings) x 2 races
        assert delete_api.delete.call_count == 8
        buckets = [c.kwargs.get('bucket') for c in delete_api.delete.call_args_list]
        assert buckets.count('laps') == 4  # 2 for laps + 2 for standings
        assert buckets.count('races') == 2
        assert buckets.count('race_sessions') == 2

    def test_prune_partial_failure_exits_1(self, capsys):
        # A delete that raises for a race is recorded in the failed list, and the
        # command exits 1 so a partial prune is not mistaken for success.
        with patch.object(sys, 'argv', ['lemongrass-races-prune', '12345', '--yes']):
            fake_client = self._make_influx_client()
            fake_client.delete_api.return_value.delete.side_effect = RuntimeError('boom')
            with patch('lemongrass._influx.connect', return_value=fake_client):
                with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                    with pytest.raises(SystemExit) as exc:
                        _mod._handle_prune()
        assert exc.value.code == 1
        captured = capsys.readouterr()
        assert 'failed to prune' in captured.err
        assert '12345' in captured.err
        # the per-race error line must go to stderr, not stdout
        assert 'error pruning race 12345' in captured.err
        assert 'error pruning race 12345' not in captured.out


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

        def fake_query(flux):
            if 'bucket: "races"' in flux:
                tables = []
                for race_id, race_name, date_str in races:
                    table = MagicMock()
                    rec = MagicMock()
                    rec.values = {'race_id': race_id, 'race_name': race_name}
                    rec.get_time.return_value = datetime.strptime(
                        date_str, '%Y-%m-%d').replace(tzinfo=UTC)
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
        with patch('lemongrass._influx.connect', return_value=client):
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
        with patch('lemongrass._influx.connect', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        assert f'current (v{SCHEMA_VERSION})' in capsys.readouterr().out

    def test_stale_schema_state(self, capsys):
        client = self._make_client(
            races=[('R1', 'Old Race', '2026-01-01')],
            totals={'R1': 50},
            currents={'R1': 20},
        )
        with patch('lemongrass._influx.connect', return_value=client):
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
        with patch('lemongrass._influx.connect', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        out = capsys.readouterr().out
        assert out.index('New Race') < out.index('Old Race')

    def test_exits_when_no_influx_token(self):
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(SystemExit) as exc:
                _mod._handle_list()
        assert exc.value.code != 0

    def test_missing_race_date_shows_question_mark(self, capsys):
        # A race whose metadata record has no timestamp renders '?' in the DATE
        # column rather than crashing on None.strftime.
        def fake_query(flux):
            if 'bucket: "races"' in flux:
                table = MagicMock()
                rec = MagicMock()
                rec.values = {'race_id': 'R1', 'race_name': 'Dateless Race'}
                rec.get_time.return_value = None
                table.records = [rec]
                return [table]
            return []

        client = MagicMock()
        query_api = MagicMock()
        query_api.query.side_effect = fake_query
        client.query_api.return_value = query_api
        client.__enter__ = lambda s: client
        client.__exit__ = MagicMock(return_value=False)
        with patch('lemongrass._influx.connect', return_value=client):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        out = capsys.readouterr().out
        assert 'Dateless Race' in out
        assert '?' in out


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


class TestRacesTuiEntry:
    def test_bare_tty_launches_browser(self, monkeypatch):
        monkeypatch.setattr(_mod.sys, 'argv', ['lemongrass-races'])
        monkeypatch.setattr(_mod.sys.stdin, 'isatty', lambda: True)
        monkeypatch.setattr(_mod.sys.stdout, 'isatty', lambda: True)
        with patch('lemongrass._env.resolve_tokens', return_value='tok'), \
             patch('race_monitor.RaceMonitorClient'), \
             patch('lemongrass.races.run_races_tui', return_value=0) as run:
            with pytest.raises(SystemExit):
                _mod.main()
        run.assert_called_once()

    def test_unknown_subcommand_still_usage(self, monkeypatch):
        monkeypatch.setattr(_mod.sys, 'argv', ['lemongrass-races', 'typo'])
        monkeypatch.setattr(_mod.sys.stdin, 'isatty', lambda: True)
        monkeypatch.setattr(_mod.sys.stdout, 'isatty', lambda: True)
        with pytest.raises(SystemExit):
            _mod.main()
