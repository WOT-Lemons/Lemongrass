# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import importlib.util
import logging
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
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=45)
        mock_event = MagicMock()
        mock_event.wait.return_value = True  # stop after first check
        with patch.object(_mod.threading, 'Event', return_value=mock_event):
            _mod.monitor_routine(ctx, [], opts)
        mock_event.wait.assert_called_with(timeout=45)

    def test_stop_event_exits_loop(self):
        stop = threading.Event()
        stop.set()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=30)
        with patch.object(_mod, 'refresh_competitor', return_value=[{'Lap': 1}]):
            _mod.monitor_routine(ctx, [{'Lap': 1}], opts, _stop_event=stop)


class TestWriteCSV:
    def test_opens_file_with_correct_name(self):
        laps = [{"Lap": 1, "LapTime": "0:01:30.000"}]
        with patch("builtins.open", mock_open()) as m:
            _mod.write_csv("my-race", laps)
        m.assert_called_once_with("./my-race.csv", 'w', encoding='utf-8', newline='')

    def test_writes_header_and_rows(self):
        laps = [{"Lap": 1, "LapTime": "0:01:30.000"}, {"Lap": 2, "LapTime": "0:01:31.000"}]
        written = []
        with patch("builtins.open", mock_open()) as m:
            m.return_value.__enter__.return_value.write = written.append
            _mod.write_csv("race", laps)
        combined = "".join(written)
        assert "Lap" in combined
        assert "LapTime" in combined


class TestResolveClassHistorical:
    def _session(self, tracked_number, cat_id, others=None):
        """others: list of (car_number, [(lap_num, position), ...]) for same-class cars."""
        def _make_competitor(number, laps_data, comp_id):
            return {
                'Number': number, 'Category': cat_id,
                'ID': comp_id, 'SessionID': 1, 'RaceID': 1,
                'FirstName': '', 'LastName': '', 'Position': '', 'Laps': '',
                'LastLapTime': '', 'BestPosition': '', 'BestLap': '',
                'BestLapTime': '', 'TotalTime': '', 'Transponder': '',
                'Nationality': '', 'AdditionalData': '',
                'LapTimes': [
                    {'Lap': str(lap), 'LapTime': '0:01:30.000',
                     'Position': str(pos), 'FlagStatus': 0,
                     'TotalTime': '0:01:30.000'}
                    for lap, pos in laps_data
                ],
            }

        competitors = [_make_competitor(tracked_number, [(1, 3), (2, 2)], 1)]
        for i, (num, laps_data) in enumerate(others or []):
            competitors.append(_make_competitor(num, laps_data, i + 2))

        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 1, 'Name': 'S1', 'SessionDate': '',
                'SessionTime': '', 'SortMode': '', 'CategoryString': '',
                'ResultsProcessorVersion': 1, 'SessionStartDateEpoc': 0,
                'Categories': {cat_id: {'ID': cat_id, 'Name': 'A'}},
                'SortedCompetitors': competitors,
            },
        }

    def test_returns_class_name(self):
        sd = self._session('42', '1')
        class_name, _ = _mod._resolve_class_historical('42', sd)
        assert class_name == 'A'

    def test_class_name_spaces_preserved(self):
        sd = self._session('42', '1')
        sd['Session']['Categories']['1']['Name'] = 'Super Street'
        class_name, _ = _mod._resolve_class_historical('42', sd)
        assert class_name == 'Super Street'

    def test_class_name_special_chars_preserved(self):
        sd = self._session('42', '1')
        sd['Session']['Categories']['1']['Name'] = 'GT3,Pro=Am'
        class_name, _ = _mod._resolve_class_historical('42', sd)
        assert class_name == 'GT3,Pro=Am'

    def test_only_car_in_class_is_always_position_1(self):
        sd = self._session('42', '1')
        _, positions = _mod._resolve_class_historical('42', sd)
        assert positions[1] == 1
        assert positions[2] == 1

    def test_two_cars_tracked_car_ahead(self):
        # tracked: lap1=pos3, lap2=pos2 — other: lap1=pos5, lap2=pos4
        sd = self._session('42', '1', others=[('99', [(1, 5), (2, 4)])])
        _, positions = _mod._resolve_class_historical('42', sd)
        assert positions[1] == 1
        assert positions[2] == 1

    def test_two_cars_tracked_car_behind(self):
        # tracked: lap1=pos3, lap2=pos2 — other: lap1=pos1, lap2=pos1
        sd = self._session('42', '1', others=[('99', [(1, 1), (2, 1)])])
        _, positions = _mod._resolve_class_historical('42', sd)
        assert positions[1] == 2
        assert positions[2] == 2

    def test_car_not_in_session_returns_none_and_empty(self):
        sd = self._session('42', '1')
        class_name, positions = _mod._resolve_class_historical('99', sd)
        assert class_name is None
        assert positions == {}

    def test_unknown_category_falls_back_to_raw_id(self):
        sd = self._session('42', '9')
        sd['Session']['Categories'] = {}
        class_name, _ = _mod._resolve_class_historical('42', sd)
        assert class_name == '9'

    def test_three_cars_tracked_car_in_middle(self):
        # tracked laps: pos3, pos2 — one ahead at pos1 — one behind at pos8
        sd = self._session('42', '1', others=[('10', [(1, 1), (2, 1)]), ('99', [(1, 8), (2, 8)])])
        _, positions = _mod._resolve_class_historical('42', sd)
        assert positions[1] == 2
        assert positions[2] == 2

    def test_classmate_missing_lap_not_counted_as_ahead(self):
        # tracked car completes lap 2, class-mate only has lap 1 data
        # class-mate's absence from lap 2 should not bump tracked car's class_pos
        sd = self._session('42', '1', others=[('99', [(1, 1)])])  # 99 only has lap 1
        _, positions = _mod._resolve_class_historical('42', sd)
        # at lap 1: 99 is at pos1 < tracked pos3 → tracked is class_pos 2
        assert positions[1] == 2
        # at lap 2: 99 has no lap 2 record → not counted → tracked is class_pos 1
        assert positions[2] == 1


class TestResolveClassLive:
    def _make_client(self, car_number, class_id, car_position, others=None):
        """others: list of (car_number, class_id, position)."""
        competitors = {
            'r1': {
                'RacerID': 'r1', 'Number': car_number, 'ClassID': class_id,
                'Position': str(car_position), 'Transponder': '', 'FirstName': '',
                'LastName': '', 'Nationality': '', 'AdditionalData': '',
                'Laps': '', 'TotalTime': '', 'BestPosition': '',
                'BestLap': '', 'BestLapTime': '', 'LastLapTime': '',
            }
        }
        for i, (num, cid, pos) in enumerate(others or []):
            competitors[f'r{i + 2}'] = {
                'RacerID': f'r{i + 2}', 'Number': num, 'ClassID': cid,
                'Position': str(pos), 'Transponder': '', 'FirstName': '',
                'LastName': '', 'Nationality': '', 'AdditionalData': '',
                'Laps': '', 'TotalTime': '', 'BestPosition': '',
                'BestLap': '', 'BestLapTime': '', 'LastLapTime': '',
            }
        client = MagicMock()
        client.live.get_session.return_value = {
            'Successful': True,
            'Session': {
                'RunNumber': '', 'SessionName': '', 'TrackName': '',
                'TrackLength': '', 'CurrentTime': '', 'SessionTime': '',
                'TimeToGo': '', 'LapsToGo': '', 'FlagStatus': '', 'SortMode': '',
                'Classes': {class_id: {'ClassID': class_id, 'Description': 'A'}},
                'Competitors': competitors,
            },
        }
        return client

    def test_returns_class_name(self):
        client = self._make_client('42', 'classA', 3)
        client.live.get_session.return_value['Session']['Classes']['classA']['Description'] = 'A'
        class_name, _ = _mod._resolve_class_live(client, '999', '42')
        assert class_name == 'A'

    def test_only_car_in_class_is_position_1(self):
        client = self._make_client('42', 'classA', 3)
        _, class_pos = _mod._resolve_class_live(client, '999', '42')
        assert class_pos == 1

    def test_tracked_car_ahead_in_class(self):
        client = self._make_client('42', 'classA', 3, others=[('99', 'classA', 5)])
        _, class_pos = _mod._resolve_class_live(client, '999', '42')
        assert class_pos == 1

    def test_tracked_car_behind_in_class(self):
        client = self._make_client('42', 'classA', 5, others=[('99', 'classA', 1)])
        _, class_pos = _mod._resolve_class_live(client, '999', '42')
        assert class_pos == 2

    def test_different_class_not_counted(self):
        client = self._make_client('42', 'classA', 5, others=[('99', 'classB', 1)])
        _, class_pos = _mod._resolve_class_live(client, '999', '42')
        assert class_pos == 1

    def test_car_not_found_returns_none_none(self):
        client = self._make_client('42', 'classA', 3)
        class_name, class_pos = _mod._resolve_class_live(client, '999', '99')
        assert class_name is None
        assert class_pos is None

    def test_api_failure_returns_none_none(self):
        client = MagicMock()
        client.live.get_session.return_value = {'Successful': False}
        class_name, class_pos = _mod._resolve_class_live(client, '999', '42')
        assert class_name is None
        assert class_pos is None

    def test_class_name_special_chars_preserved(self):
        client = self._make_client('42', 'classA', 1)
        client.live.get_session.return_value['Session']['Classes']['classA']['Description'] = (
            'GT3,Pro=Am')
        class_name, _ = _mod._resolve_class_live(client, '999', '42')
        assert class_name == 'GT3,Pro=Am'


class TestPushInfluxClassInfo:
    def _laps(self):
        return [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
                 'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}]

    def _ctx(self):
        write_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 0)
        return ctx, write_api

    def _record(self, write_api):
        point = write_api.write.call_args[1]['record']
        return point.to_line_protocol()

    def test_includes_class_tag_when_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, class_name='A', class_positions={1: 2})
        assert 'class=A' in self._record(write_api)

    def test_class_tag_before_driver_tag(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, class_name='A', class_positions={1: 1})
        record = self._record(write_api)
        assert record.index('class=') < record.index('driver=')

    def test_includes_class_position_field_when_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, class_name='A', class_positions={1: 2})
        assert 'class_position=2i' in self._record(write_api)

    def test_omits_class_tag_when_not_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'class=' not in self._record(write_api)

    def test_omits_class_position_when_not_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'class_position' not in self._record(write_api)

    def test_ctx_start_epoc_used_when_no_override(self):
        ctx, write_api = self._ctx()  # ctx.start_epoc = 0
        _mod.push_influx(ctx, self._laps(), False)
        record = self._record(write_api)
        # start_epoc=0 → timestamp = 0*1000 + 90000 = 90000
        # TotalTime '0:01:30.000' = 90000 ms
        assert record.endswith('90000')

    def test_start_epoc_override_changes_timestamp(self):
        ctx, write_api = self._ctx()  # ctx.start_epoc = 0, would give 90000
        _mod.push_influx(ctx, self._laps(), False, start_epoc=1000)
        record = self._record(write_api)
        # start_epoc=1000 → timestamp = 1000*1000 + 90000 = 1090000
        assert record.endswith('1090000')

    def test_warns_when_effective_epoc_is_zero(self, caplog):
        ctx, write_api = self._ctx()  # ctx.start_epoc = 0
        with caplog.at_level(logging.WARNING):
            _mod.push_influx(ctx, self._laps(), False)
        assert any('epoch' in r.message.lower() and r.levelno == logging.WARNING
                   for r in caplog.records)


class TestOldRaceClassWiring:
    def _session_details(self, car_number='42', cat_id='1', cat_name='A'):
        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionDate': '',
                'SessionTime': '', 'SortMode': '', 'CategoryString': '',
                'ResultsProcessorVersion': 1, 'SessionStartDateEpoc': 0,
                'Categories': {cat_id: {'ID': cat_id, 'Name': cat_name}},
                'SortedCompetitors': [{
                    'Number': car_number, 'Category': cat_id,
                    'ID': 1, 'SessionID': 1, 'RaceID': 999,
                    'FirstName': 'Jane', 'LastName': 'Doe',
                    'Position': '1', 'Laps': '1', 'LastLapTime': '',
                    'BestPosition': '1', 'BestLap': '1',
                    'BestLapTime': '0:01:30.000', 'TotalTime': '0:01:30.000',
                    'Transponder': '', 'Nationality': '', 'AdditionalData': '',
                    'LapTimes': [
                        {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                         'FlagStatus': 0, 'TotalTime': '0:01:30.000'},
                    ],
                }],
            },
        }

    def test_calls_resolve_class_historical_per_session(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(
            _mod, '_resolve_class_historical', return_value=('A', {1: 1})
        ) as mock_resolve:
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        mock_resolve.assert_called_once_with('42', self._session_details())

    def test_passes_class_name_to_push_influx(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('class_name') == 'A'

    def test_passes_session_start_epoc_to_push_influx(self):
        # ctx.start_epoc=9999 is intentionally different from SessionStartDateEpoc=5555
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 9999)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SessionStartDateEpoc'] = 5555
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('start_epoc') == 5555

    def test_handles_missing_session_start_epoc_key(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        del session['Session']['SessionStartDateEpoc']
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('start_epoc') is None

    def test_no_network_mode_skips_resolve(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical') as mock_resolve:
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        mock_resolve.assert_not_called()


class TestLiveClassWiring:
    def _make_ctx(self):
        write_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 0)
        ctx.client.live.get_racer.return_value = {
            'Successful': True,
            'Details': {
                'Competitor': {
                    'RacerID': 'r1', 'Number': '42', 'ClassID': 'A',
                    'Position': '3', 'Laps': '1', 'TotalTime': '0:01:30.000',
                    'BestPosition': '3', 'BestLap': '1', 'BestLapTime': '0:01:30.000',
                    'LastLapTime': '0:01:30.000', 'Transponder': '',
                    'FirstName': 'Jane', 'LastName': 'Doe',
                    'Nationality': '', 'AdditionalData': '',
                },
                'Laps': [
                    {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
                     'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
                ],
            },
        }
        return ctx

    def test_live_race_calls_resolve_class_live(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)) as mock_resolve:
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'print_rankings'):
                    _mod.live_race(ctx, opts)
        mock_resolve.assert_called_once_with(ctx.client, ctx.race_id, ctx.car_number)

    def test_live_race_passes_class_name_to_push_influx(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 2)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'print_rankings'):
                    _mod.live_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('class_name') == 'A'

    def test_live_race_passes_none_class_name_when_car_not_in_session(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=(None, None)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'print_rankings'):
                    _mod.live_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('class_name') is None
        assert kwargs.get('class_positions') is None

    def test_live_race_does_not_write_class_positions(self):
        ctx = self._make_ctx()
        two_laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '2',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        ctx.client.live.get_racer.return_value['Details']['Laps'] = two_laps
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 2)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'print_rankings'):
                    _mod.live_race(ctx, opts)
        args, kwargs = mock_push.call_args
        assert args[1] == two_laps
        assert kwargs.get('class_positions') is None

    def test_live_race_skips_influx_when_no_laps(self):
        ctx = self._make_ctx()
        ctx.client.live.get_racer.return_value['Details']['Laps'] = []
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx') as mock_push:
            with patch.object(_mod, 'print_rankings'):
                _mod.live_race(ctx, opts)
        mock_push.assert_not_called()

    def test_monitor_routine_push_influx_receives_only_new_lap(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        existing_laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
        ]
        new_laps = existing_laps + [
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        mock_stop = MagicMock()
        mock_stop.wait.side_effect = [False, True]
        with patch.object(_mod, 'refresh_competitor', return_value=new_laps):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
                with patch.object(_mod, 'push_influx') as mock_push:
                    _mod.monitor_routine(ctx, existing_laps, opts, _stop_event=mock_stop)
        laps_arg = mock_push.call_args[0][1]
        assert laps_arg == [new_laps[-1]]

    def test_monitor_routine_calls_resolve_class_live_on_new_lap(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        existing_laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
        ]
        new_laps = existing_laps + [
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        # wait() returns False first (enter loop body), then True (stop)
        mock_stop = MagicMock()
        mock_stop.wait.side_effect = [False, True]
        with patch.object(_mod, 'refresh_competitor', return_value=new_laps):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)) as mock_resolve:
                with patch.object(_mod, 'push_influx'):
                    _mod.monitor_routine(ctx, existing_laps, opts, _stop_event=mock_stop)
        mock_resolve.assert_called_once_with(ctx.client, ctx.race_id, ctx.car_number)


class TestResolveRaceMetadata:
    def _race_details(self, series_id=145):
        return {
            'Successful': True,
            'Race': {
                'ID': 166153,
                'Name': 'The Sausage Fest 2026',
                'SeriesID': series_id,
                'Track': 'Road America',
            }
        }

    def _client_with_series(self, series_name='24 Hours of Lemons'):
        client = MagicMock()
        client.common.current_races.return_value = {
            'Successful': True,
            'Races': [{'SeriesName': series_name}],
        }
        return client

    def test_race_name_extracted(self):
        meta = _mod._resolve_race_metadata(self._race_details(), self._client_with_series())
        assert meta.race_name == 'The Sausage Fest 2026'

    def test_track_name_extracted(self):
        meta = _mod._resolve_race_metadata(self._race_details(), self._client_with_series())
        assert meta.track_name == 'Road America'

    def test_series_name_from_current_races(self):
        client = self._client_with_series('24 Hours of Lemons')
        meta = _mod._resolve_race_metadata(self._race_details(series_id=145), client)
        assert meta.series_name == '24 Hours of Lemons'
        client.common.current_races.assert_called_once_with(series_id=145)

    def test_series_name_falls_back_to_past_races(self):
        client = MagicMock()
        client.common.current_races.return_value = {'Successful': True, 'Races': []}
        client.common.past_races.return_value = {
            'Successful': True,
            'Races': [{'SeriesName': '24 Hours of Lemons'}],
        }
        meta = _mod._resolve_race_metadata(self._race_details(series_id=145), client)
        assert meta.series_name == '24 Hours of Lemons'
        client.common.past_races.assert_called_once_with(series_id=145, max_results=1)

    def test_series_name_none_when_series_id_is_none(self):
        client = MagicMock()
        meta = _mod._resolve_race_metadata(self._race_details(series_id=None), client)
        assert meta.series_name is None
        client.common.current_races.assert_not_called()

    def test_series_name_none_when_both_lookups_empty(self):
        client = MagicMock()
        client.common.current_races.return_value = {'Successful': True, 'Races': []}
        client.common.past_races.return_value = {'Successful': True, 'Races': []}
        meta = _mod._resolve_race_metadata(self._race_details(series_id=145), client)
        assert meta.series_name is None

    def test_series_name_none_when_lookup_raises(self):
        client = MagicMock()
        client.common.current_races.side_effect = Exception('API error')
        meta = _mod._resolve_race_metadata(self._race_details(series_id=145), client)
        assert meta.series_name is None

    def test_returns_empty_metadata_when_unsuccessful(self):
        client = MagicMock()
        meta = _mod._resolve_race_metadata({'Successful': False}, client)
        assert meta.race_name == ''
        assert meta.track_name == ''
        assert meta.series_name is None
        client.common.current_races.assert_not_called()
