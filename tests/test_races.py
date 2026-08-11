import sys
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

import lemongrass.races as _mod
from lemongrass.races import prune_races

races_mod = _mod


def _rec(values, value=None):
    rec = MagicMock()
    rec.values = values
    rec.get_value.return_value = value
    return rec


def _tables(mapping):
    """Build fake Flux tables (one record per entry) keyed by race_id -> count."""
    t = MagicMock()
    t.records = [_rec({'race_id': rid}, value=count) for rid, count in mapping.items()]
    return [t]


def _list_row(race_id, name, when, venue_name=None, event_name=None):
    from lemongrass import _db
    return _db.RaceListRow(race_id=race_id, name=name, race_time=when,
                           venue_name=venue_name, event_name=event_name)


def test_fetch_race_rows_joins_sql_attributes_with_flux_counts():
    rows = [_list_row('101', 'Spring', datetime(2026, 5, 1, tzinfo=UTC)),
            _list_row('102', 'Fall', datetime(2026, 9, 1, tzinfo=UTC))]
    query_api = MagicMock()
    query_api.query.side_effect = [
        _tables({'101': 40, '102': 10}),   # total lap counts
        _tables({'101': 40}),              # current-schema lap counts
    ]
    with patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
        got = races_mod.fetch_race_rows(query_api)
    assert [r['race_id'] for r in got] == ['102', '101']   # newest first
    assert got[1]['total'] == 40 and got[1]['current'] == 40
    assert got[0]['total'] == 10 and got[0]['current'] == 0
    assert got[0]['date'] == '2026-09-01'
    assert query_api.query.call_count == 2   # the race query is gone


def test_fetch_race_rows_reports_the_current_schema_version():
    # Deliberate: races list renders "stale (N/M at vX)" where X is the
    # version laps should be at, not the version stored on the race.
    from lemongrass.laps import SCHEMA_VERSION
    rows = [_list_row('101', 'Spring', datetime(2026, 5, 1, tzinfo=UTC))]
    query_api = MagicMock()
    query_api.query.side_effect = [_tables({}), _tables({})]
    with patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
        got = races_mod.fetch_race_rows(query_api)
    assert got[0]['schema_version'] == SCHEMA_VERSION


def test_fetch_race_rows_includes_venue_and_event_names():
    rows = [_list_row('101', 'GP du Lac', datetime(2024, 5, 1, tzinfo=UTC),
                      'Thompson Speedway Motorsports Park', 'GP du Lac'),
            _list_row('102', 'Mystery', datetime(2023, 5, 1, tzinfo=UTC))]
    query_api = MagicMock()
    query_api.query.side_effect = [_tables({}), _tables({})]
    with patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
        got = {r['race_id']: r for r in races_mod.fetch_race_rows(query_api)}
    assert got['101']['venue_name'] == 'Thompson Speedway Motorsports Park'
    assert got['101']['event_name'] == 'GP du Lac'
    assert got['102']['venue_name'] == ''
    assert got['102']['event_name'] == ''


def test_races_list_prints_a_venue_column(capsys):
    rows = [{"race_id": "101", "name": "GP du Lac", "date": "2024-05-01",
             "total": 400, "current": 400, "schema_version": 5,
             "venue_name": "Thompson Speedway Motorsports Park", "event_name": "GP du Lac"}]
    with patch("lemongrass._influx.connect"), \
         patch("lemongrass.races.fetch_race_rows", return_value=rows):
        races_mod._handle_list()
    out = capsys.readouterr().out
    header, rule, row = out.splitlines()[:3]
    # The widths are chosen, not incidental: NAME 35->24 pays for an 18-wide
    # VENUE, and 'New Jersey Motorsports Park' is 27 characters, so anything
    # narrower than 18 makes the column useless.
    assert header == (f"{'RACE ID':<10} {'NAME':<24} {'VENUE':<18} "
                      f"{'DATE':<12} {'LAPS':<8} SCHEMA")
    assert rule == '-' * 91
    # Venue names are truncated to 18, not dropped.
    assert row.startswith("101        GP du Lac                Thompson Speedway  ")


def test_prune_deletes_influx_before_the_race_row():
    calls = []
    delete_api = MagicMock()
    delete_api.delete.side_effect = lambda **kw: calls.append(kw['bucket'])
    with patch('lemongrass.races._db.delete_race',
               side_effect=lambda rid: calls.append('postgres') or True):
        failed = races_mod.prune_races(delete_api, ['144185'])
    assert failed == []
    assert calls[-1] == 'postgres'


def test_prune_keeps_the_race_row_when_an_influx_delete_fails():
    # The retry guard keys off the race row: if it is gone but the laps are
    # not, nothing can find the orphans.
    delete_api = MagicMock()
    delete_api.delete.side_effect = Exception('nope')
    with patch('lemongrass.races._db.delete_race') as drop:
        failed = races_mod.prune_races(delete_api, ['144185'])
    assert failed == ['144185']
    drop.assert_not_called()


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
        with patch('lemongrass.races._db.delete_race', return_value=True):
            failed = prune_races(delete_api, ['144185'], on_progress=progress.append)
        assert failed == []
        # legacy metadata (race measurement) delete is the last Influx call
        preds = [c.kwargs['predicate'] for c in delete_api.delete.call_args_list]
        assert preds[-1].startswith('_measurement="race"')
        # the Postgres race row is deleted after all Influx deletes
        assert len(progress) == 5
        assert 'race row' in progress[-1]

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
    @pytest.fixture(autouse=True)
    def _patch_db(self, monkeypatch):
        """Race-lookup and delete now go through Postgres; back them with a fake
        keyed off self._races so the existing InfluxDB-flavored fixtures still work."""
        from lemongrass import _db
        self._races = {}

        def fake_get_race(rid, conn=None):
            name = self._races.get(rid)
            if name is None:
                return None
            return _db.RaceRow(race_id=rid, race_time=datetime(2026, 1, 1, tzinfo=UTC), name=name)

        monkeypatch.setattr('lemongrass.races._db.get_race', fake_get_race)
        monkeypatch.setattr('lemongrass.races._db.delete_race', lambda rid, conn=None: True)

    def _make_influx_client(self, races=None):
        if races is None:
            races = {'12345': 'Test Race'}
        self._races = races
        client = MagicMock()
        query_api = MagicMock()
        query_api.query.return_value = []
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
        assert 'Deleted legacy race metadata' in out
        assert 'Deleted race row' in out
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
        assert out.count('Deleted legacy race metadata') == 2
        assert out.count('Deleted race row') == 2
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
    def _race_rows(self, races):
        """races: list of (race_id, race_name, date_str) -> list[RaceListRow]."""
        from lemongrass import _db
        return [
            _db.RaceListRow(
                race_id=race_id, name=race_name,
                race_time=datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC))
            for race_id, race_name, date_str in races
        ]

    def _make_client(self, totals, currents):
        """Build mock InfluxDB client for _handle_list's two Flux lap-count queries.

        totals:   dict {race_id: total_lap_count}
        currents: dict {race_id: current_schema_lap_count}
        """

        def fake_query(flux):
            counts = totals if '"lap_no"' in flux else currents
            tables = []
            for race_id, count in counts.items():
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
        client = self._make_client(totals={}, currents={})
        rows = self._race_rows([('R1', 'Empty Race', '2026-01-01')])
        with patch('lemongrass._influx.connect', return_value=client), \
             patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        assert 'no laps' in capsys.readouterr().out

    def test_current_schema_state(self, capsys):
        from lemongrass.laps import SCHEMA_VERSION
        client = self._make_client(totals={'R1': 50}, currents={'R1': 50})
        rows = self._race_rows([('R1', 'Full Race', '2026-01-01')])
        with patch('lemongrass._influx.connect', return_value=client), \
             patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        assert f'current (v{SCHEMA_VERSION})' in capsys.readouterr().out

    def test_stale_schema_state(self, capsys):
        client = self._make_client(totals={'R1': 50}, currents={'R1': 20})
        rows = self._race_rows([('R1', 'Old Race', '2026-01-01')])
        with patch('lemongrass._influx.connect', return_value=client), \
             patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
            with patch.dict('os.environ', {'INFLUX_TELEMETRY_TOKEN': 'tok'}):
                _mod._handle_list()
        out = capsys.readouterr().out
        assert 'stale' in out
        assert '20/50' in out

    def test_sorted_newest_first(self, capsys):
        client = self._make_client(totals={'R1': 10, 'R2': 10}, currents={'R1': 10, 'R2': 10})
        rows = self._race_rows([
            ('R1', 'Old Race', '2024-06-01'),
            ('R2', 'New Race', '2026-06-01'),
        ])
        with patch('lemongrass._influx.connect', return_value=client), \
             patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
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
        # A race whose stored row has no timestamp renders '?' in the DATE
        # column rather than crashing on None.strftime.
        from lemongrass import _db
        client = self._make_client(totals={}, currents={})
        rows = [_db.RaceListRow(race_id='R1', name='Dateless Race', race_time=None)]
        with patch('lemongrass._influx.connect', return_value=client), \
             patch('lemongrass.races._db.list_races_with_venue', return_value=rows):
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


def _stored(race_id, name, track_name, series_id=None, **ids):
    from datetime import UTC, datetime

    from lemongrass import _db
    return _db.RaceRow(race_id=race_id, race_time=datetime(2024, 5, 1, tzinfo=UTC),
                       name=name, track_name=track_name, series_id=series_id, **ids)


def test_identify_resolves_a_stored_race():
    from unittest.mock import patch

    from lemongrass import races as races_mod
    rows = [_stored("101", "GP du Lac 2023", "Thompson Motor Speedway", 145)]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks"), \
         patch("lemongrass._db.set_race_identity") as write:
        changes, unresolved, _ = races_mod.identify_races()
    assert changes == [("101", (None, None, None),
                        ("thompson", None, "gp-du-lac"))]
    assert unresolved == {}
    write.assert_called_once_with("101", "thompson", None, "gp-du-lac")


def test_identify_leaves_unresolved_races_null_and_counts_them():
    from unittest.mock import patch

    from lemongrass import races as races_mod
    rows = [_stored("101", "Race A", "Mystery Park"),
            _stored("102", "Race B", "Mystery Park")]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks"), \
         patch("lemongrass._db.set_race_identity") as write:
        changes, unresolved, _ = races_mod.identify_races()
    assert changes == []
    assert unresolved == {"Mystery Park": 2}
    assert not write.called


def test_identify_is_idempotent():
    from unittest.mock import patch

    from lemongrass import races as races_mod
    rows = [_stored("101", "GP du Lac 2023", "Thompson Motor Speedway", 145,
                    venue_id="thompson", event_id="gp-du-lac")]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks"), \
         patch("lemongrass._db.set_race_identity") as write:
        changes, _, _ = races_mod.identify_races()
    assert changes == []
    assert not write.called


def test_identify_dry_run_writes_nothing():
    from unittest.mock import patch

    from lemongrass import races as races_mod
    rows = [_stored("101", "GP du Lac 2023", "Thompson Motor Speedway", 145)]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks") as sync, \
         patch("lemongrass._db.set_race_identity") as write:
        changes, _, _ = races_mod.identify_races(dry_run=True)
    assert len(changes) == 1
    assert not write.called
    assert not sync.called


def test_identify_resolves_the_event_of_a_legacy_race_with_no_series_id():
    from unittest.mock import patch

    from lemongrass import races as races_mod
    rows = [_stored("101", "GP du Lac 2019", "Thompson Motor Speedway", None)]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks"), \
         patch("lemongrass._db.set_race_identity"):
        changes, _, _ = races_mod.identify_races()
    assert changes[0][2] == ("thompson", None, "gp-du-lac")


def test_identify_can_be_limited_to_named_races():
    from unittest.mock import patch

    from lemongrass import races as races_mod
    rows = [_stored("101", "GP du Lac", "Thompson Motor Speedway", 145),
            _stored("102", "Other", "Gingerman")]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks"), \
         patch("lemongrass._db.set_race_identity"):
        changes, _, _ = races_mod.identify_races(race_ids=["102"])
    assert [c[0] for c in changes] == ["102"]


def test_identify_reports_race_ids_with_no_stored_row():
    # A typo'd id must not read the same as "already correct".
    rows = [_stored("101", "GP du Lac", "Thompson Motor Speedway", 145)]
    with patch("lemongrass._db.list_races", return_value=rows), \
         patch("lemongrass._db.sync_tracks"), \
         patch("lemongrass._db.set_race_identity"):
        _, _, missing = races_mod.identify_races(race_ids=["101", "999", "998"])
    assert missing == ["998", "999"]


class TestHandleIdentify:
    def _run(self, argv, result):
        with patch.object(sys, 'argv', ['lemongrass-races-identify', *argv]), \
             patch.object(_mod, 'identify_races', return_value=result) as ident:
            code = _mod._handle_identify()
        return code, ident

    def test_reports_changes_and_returns_zero(self, capsys):
        changes = [("101", (None, None, None), ("thompson", None, "gp-du-lac"))]
        code, ident = self._run([], (changes, {}, []))
        out = capsys.readouterr().out
        assert code == 0
        assert "101        -/-/- -> thompson/-/gp-du-lac" in out
        assert "1 race(s) changed" in out
        ident.assert_called_once_with(race_ids=None, dry_run=False)

    def test_dry_run_switches_the_verb_and_the_flag(self, capsys):
        code, ident = self._run(['--dry-run'], ([], {}, []))
        assert code == 0
        assert "0 race(s) would change" in capsys.readouterr().out
        ident.assert_called_once_with(race_ids=None, dry_run=True)

    def test_passes_positional_race_ids_through(self):
        _, ident = self._run(['101', '102'], ([], {}, []))
        ident.assert_called_once_with(race_ids=['101', '102'], dry_run=False)

    def test_unresolved_report_is_sorted_by_count_then_name(self, capsys):
        unresolved = {"Zed Park": 1, "Mystery Park": 3, "Alpha Park": 1, "": 2}
        code, _ = self._run([], ([], unresolved, []))
        lines = capsys.readouterr().out.splitlines()
        assert code == 0
        assert lines[1] == "unresolved track names (add to tracks.toml):"
        assert [line.split(None, 1)[1] for line in lines[2:]] == [
            "Mystery Park", "(blank)", "Alpha Park", "Zed Park"]

    def test_missing_race_ids_are_reported_and_exit_nonzero(self, capsys):
        code, _ = self._run(['999'], ([], {}, ["999"]))
        assert code == 1
        assert "No race row stored for race 999" in capsys.readouterr().err

    def test_the_exit_code_survives_dispatch_through_main(self):
        # Testing the handler alone would not catch races.main() discarding
        # what the handler returns, which is what cli.main exits with.
        with patch.object(sys, 'argv', ['lemongrass-races', 'identify', '999']), \
             patch.object(_mod, 'identify_races', return_value=([], {}, ["999"])):
            assert _mod.main() == 1
