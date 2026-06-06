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

    def test_class_name_spaces_replaced_with_underscores(self):
        sd = self._session('42', '1')
        sd['Session']['Categories']['1']['Name'] = 'Super Street'
        class_name, _ = _mod._resolve_class_historical('42', sd)
        assert class_name == 'Super_Street'

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


class TestPushInfluxClassInfo:
    def _laps(self):
        return [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
                 'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}]

    def _ctx(self):
        write_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 0)
        return ctx, write_api

    def _record(self, write_api):
        return write_api.write.call_args[1]['record'][0]

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
        assert 'class_position=2' in self._record(write_api)

    def test_omits_class_tag_when_not_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'class=' not in self._record(write_api)

    def test_omits_class_position_when_not_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'class_position' not in self._record(write_api)


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
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})) as mock_resolve:
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

    def test_no_network_mode_skips_resolve(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical') as mock_resolve:
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        mock_resolve.assert_not_called()
