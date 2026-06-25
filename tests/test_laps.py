import logging
import threading
from typing import ClassVar
from unittest.mock import MagicMock, call, mock_open, patch

import pytest

import lemongrass.laps as _mod


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

    def test_monitor_status_values_exist(self):
        assert _mod.MonitorStatus.RACE_ENDED is not None
        assert _mod.MonitorStatus.INTERRUPTED is not None

    def test_refresh_competitor_empty_response_returns_empty_list(self):
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        ctx.client.live.get_racer.return_value = {'Successful': False}
        result = _mod.refresh_competitor(ctx)
        assert result == []

    def test_collects_all_new_laps_not_just_last(self):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)

        lap1 = {'Lap': '1', 'LapTime': '1:00.000'}
        lap2 = {'Lap': '2', 'LapTime': '1:01.000'}
        lap3 = {'Lap': '3', 'LapTime': '1:02.000'}

        call_count = 0
        def fake_refresh(c):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return [lap1, lap2, lap3]

        existing_laps = [lap1]
        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            with patch.object(ctx.client.live, 'get_session',
                              return_value={'Successful': False}):
                _mod.monitor_routine(ctx, existing_laps, opts, _stop_event=stop)

        assert lap2 in existing_laps
        assert lap3 in existing_laps

    def test_empty_refresh_does_not_crash(self):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)

        call_count = 0
        def fake_refresh(c):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return []

        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            with patch.object(ctx.client.live, 'get_session',
                              return_value={'Successful': False}):
                _mod.monitor_routine(ctx, [], opts, _stop_event=stop)
        # no exception raised is the assertion

    def test_detects_new_session_and_updates_session_id(self):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        lap1 = {'Lap': '1', 'LapTime': '1:00.000'}
        lap2 = {'Lap': '2', 'LapTime': '1:01.000'}

        session_calls = 0
        def fake_get_session(race_id):
            nonlocal session_calls
            session_calls += 1
            if session_calls == 1:
                return {'Successful': True, 'Session': {'ID': 'sess-A', 'Name': 'Session A',
                                                        'Competitors': {}, 'Classes': {}}}
            stop.set()
            return {'Successful': True, 'Session': {'ID': 'sess-B', 'Name': 'Session B',
                                                    'Competitors': {}, 'Classes': {}}}

        ctx.client.live.get_session.side_effect = fake_get_session

        with patch.object(_mod, 'refresh_competitor', return_value=[lap1, lap2]):
            with patch.object(_mod, 'push_influx_session') as mock_push_session:
                with patch.object(_mod, 'push_influx'):
                    _mod.monitor_routine(ctx, [lap1], opts, _stop_event=stop,
                                         session_id='sess-A')

        mock_push_session.assert_called_once_with(ctx, 'sess-B', 'Session B', None)

    def test_new_session_prints_message(self, capsys):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)

        session_calls = 0
        def fake_get_session(race_id):
            nonlocal session_calls
            session_calls += 1
            if session_calls == 1:
                return {'Successful': True, 'Session': {'ID': 'sess-A', 'Name': 'Session A',
                                                        'Competitors': {}, 'Classes': {}}}
            stop.set()
            return {'Successful': True, 'Session': {'ID': 'sess-B', 'Name': 'Session B',
                                                    'Competitors': {}, 'Classes': {}}}

        ctx.client.live.get_session.side_effect = fake_get_session

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            _mod.monitor_routine(ctx, [], opts, _stop_event=stop, session_id='sess-A')

        captured = capsys.readouterr()
        assert 'Session B' in captured.out

    def test_returns_race_ended_when_not_live(self):
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=30)

        poll_count = 0
        def fake_wait(timeout):
            nonlocal poll_count
            poll_count += 1
            return poll_count > _mod._LIVE_CHECK_INTERVAL  # stop after interval+1 calls

        stop = MagicMock()
        stop.wait.side_effect = fake_wait

        ctx.client.live.get_session.return_value = {'Successful': False}
        ctx.client.race.is_live.return_value = {'Successful': True, 'IsLive': False}

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod.threading, 'Event', return_value=stop):
                result = _mod.monitor_routine(ctx, [], opts)

        assert result == _mod.MonitorStatus.RACE_ENDED

    def test_is_live_called_every_n_polls(self):
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=30)

        n = _mod._LIVE_CHECK_INTERVAL
        poll_count = 0
        def fake_wait(timeout):
            nonlocal poll_count
            poll_count += 1
            return poll_count > n * 2  # run for 2 full intervals

        stop = MagicMock()
        stop.wait.side_effect = fake_wait

        ctx.client.live.get_session.return_value = {'Successful': False}
        ctx.client.race.is_live.return_value = {'Successful': True, 'IsLive': True}

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod.threading, 'Event', return_value=stop):
                _mod.monitor_routine(ctx, [], opts)

        assert ctx.client.race.is_live.call_count == 2

    def test_returns_interrupted_on_keyboard_interrupt(self, capsys):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)

        ctx.client.live.get_session.return_value = {'Successful': False}

        def raise_kbi(c):
            raise KeyboardInterrupt

        with patch.object(_mod, 'refresh_competitor', side_effect=raise_kbi):
            result = _mod.monitor_routine(ctx, [], opts, _stop_event=stop)

        assert result == _mod.MonitorStatus.INTERRUPTED
        captured = capsys.readouterr()
        assert 'Monitoring stopped' in captured.out

    def test_standings_written_each_poll_in_network_mode(self):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        call_count = 0
        def fake_get_session(race_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return {'Successful': True, 'Session': {
                'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}

        ctx.client.live.get_session.side_effect = fake_get_session

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod, 'push_influx_standings_live') as mock_standings:
                with patch.object(_mod, 'push_influx'):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop)

        mock_standings.assert_called_once_with(ctx, mock_standings.call_args[0][1], 'sess-1', {})

    def test_standings_not_written_when_not_network_mode(self):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)

        call_count = 0
        def fake_get_session(race_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return {'Successful': True, 'Session': {
                'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}

        ctx.client.live.get_session.side_effect = fake_get_session

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod, 'push_influx_standings_live') as mock_standings:
                _mod.monitor_routine(ctx, [], opts, _stop_event=stop)

        mock_standings.assert_not_called()

    def test_prev_standings_threaded_back_each_poll(self):
        """Verify monitor_routine passes the return value of each standings call
        back as prev_standings on the subsequent poll."""
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True, interval=0)
        sentinel = {'42': (1, 5, None, None, None)}

        call_count = 0
        def fake_get_session(race_id):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                stop.set()
            return {'Successful': True, 'Session': {
                'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}

        ctx.client.live.get_session.side_effect = fake_get_session

        # First call returns sentinel; second call should receive it as prev_standings.
        mock_standings = MagicMock(return_value=sentinel)
        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod, 'push_influx_standings_live', mock_standings):
                with patch.object(_mod, 'push_influx'):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop)

        assert mock_standings.call_count == 2
        _, _, _, second_prev = mock_standings.call_args_list[1][0]
        assert second_prev == sentinel


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
    def _make_session(self, car_number, class_id, car_position, others=None):
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
        return {
            'Successful': True,
            'Session': {
                'RunNumber': '', 'SessionName': '', 'TrackName': '',
                'TrackLength': '', 'CurrentTime': '', 'SessionTime': '',
                'TimeToGo': '', 'LapsToGo': '', 'FlagStatus': '', 'SortMode': '',
                'Classes': {class_id: {'ClassID': class_id, 'Description': 'A'}},
                'Competitors': competitors,
            },
        }

    def test_returns_class_name(self):
        session = self._make_session('42', 'classA', 3)
        session['Session']['Classes']['classA']['Description'] = 'A'
        class_name, _ = _mod._resolve_class_live(session, '42')
        assert class_name == 'A'

    def test_only_car_in_class_is_position_1(self):
        session = self._make_session('42', 'classA', 3)
        _, class_pos = _mod._resolve_class_live(session, '42')
        assert class_pos == 1

    def test_tracked_car_ahead_in_class(self):
        session = self._make_session('42', 'classA', 3, others=[('99', 'classA', 5)])
        _, class_pos = _mod._resolve_class_live(session, '42')
        assert class_pos == 1

    def test_tracked_car_behind_in_class(self):
        session = self._make_session('42', 'classA', 5, others=[('99', 'classA', 1)])
        _, class_pos = _mod._resolve_class_live(session, '42')
        assert class_pos == 2

    def test_different_class_not_counted(self):
        session = self._make_session('42', 'classA', 5, others=[('99', 'classB', 1)])
        _, class_pos = _mod._resolve_class_live(session, '42')
        assert class_pos == 1

    def test_car_not_found_returns_none_none(self):
        session = self._make_session('42', 'classA', 3)
        class_name, class_pos = _mod._resolve_class_live(session, '99')
        assert class_name is None
        assert class_pos is None

    def test_api_failure_returns_none_none(self):
        class_name, class_pos = _mod._resolve_class_live({'Successful': False}, '42')
        assert class_name is None
        assert class_pos is None

    def test_class_name_special_chars_preserved(self):
        session = self._make_session('42', 'classA', 1)
        session['Session']['Classes']['classA']['Description'] = 'GT3,Pro=Am'
        class_name, _ = _mod._resolve_class_live(session, '42')
        assert class_name == 'GT3,Pro=Am'


class TestTimeToMs:
    def test_three_part_with_hours(self):
        assert _mod._time_to_ms('1:23:45.678') == 1 * 3600000 + 23 * 60000 + 45 * 1000 + 678

    def test_three_part_zero_padded_hours(self):
        assert _mod._time_to_ms('0:01:30.000') == 90000

    def test_two_part_under_an_hour(self):
        assert _mod._time_to_ms('45:30.000') == 45 * 60000 + 30 * 1000

    def test_two_part_single_digit_minute(self):
        assert _mod._time_to_ms('1:30.000') == 90000

    def test_missing_milliseconds(self):
        assert _mod._time_to_ms('45:30') == 45 * 60000 + 30 * 1000

    def test_unparseable_value_returns_none(self):
        assert _mod._time_to_ms('3$H') is None

    def test_unparseable_value_logs_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            _mod._time_to_ms('3$H')
        assert '3$H' in caplog.text


class TestPushInfluxClassInfo:
    def _laps(self):
        return [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
                 'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}]

    def _ctx(self):
        write_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 0)
        return ctx, write_api

    def _record(self, write_api):
        record = write_api.write.call_args[1]['record']
        points = record if isinstance(record, list) else [record]
        return points[0].to_line_protocol()

    def test_multiple_laps_written_in_single_batch(self):
        ctx, write_api = self._ctx()
        laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'},
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '2',
             'FlagStatus': 'Green', 'TotalTime': '0:03:01.000'},
        ]
        _mod.push_influx(ctx, laps, False)
        write_api.write.assert_called_once()
        record = write_api.write.call_args[1]['record']
        assert isinstance(record, list)
        assert len(record) == 2

    def test_includes_class_tag_when_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, class_name='A', class_positions={1: 2})
        assert 'class=A' in self._record(write_api)

    def test_car_number_tag_before_class_tag(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, class_name='A', class_positions={1: 1})
        record = self._record(write_api)
        assert record.index('car_number=') < record.index('class=')

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
        ctx, _write_api = self._ctx()  # ctx.start_epoc = 0
        with caplog.at_level(logging.WARNING):
            _mod.push_influx(ctx, self._laps(), False)
        assert any('epoch' in r.message.lower() and r.levelno == logging.WARNING
                   for r in caplog.records)

    def test_includes_competitor_name_tag_when_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, competitor_name='Jane Doe')
        assert 'competitor_name=Jane\\ Doe' in self._record(write_api)

    def test_omits_competitor_name_tag_when_none(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'competitor_name' not in self._record(write_api)

    def test_includes_car_info_tag_when_provided(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, car_info='2005/Toy/Celica')
        assert 'car_info=2005/Toy/Celica' in self._record(write_api)

    def test_omits_car_info_tag_when_none(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'car_info' not in self._record(write_api)

    def test_measurement_is_lap(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert self._record(write_api).startswith('lap,')

    def test_race_id_tag_present(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert 'race_id=999' in self._record(write_api)

    def test_car_number_tag_replaces_driver(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        record = self._record(write_api)
        assert 'car_number=42' in record
        assert 'driver=' not in record

    def test_race_metadata_tags_absent_from_lap(self):
        ctx, write_api = self._ctx()
        ctx.metadata = _mod.RaceMetadata(
            race_name='Test Race', track_name='Road America',
            series_name='Lemons', end_time_epoc=9999)
        _mod.push_influx(ctx, self._laps(), False)
        record = self._record(write_api)
        assert 'race_name=' not in record
        assert 'track_name=' not in record
        assert 'series_name=' not in record

    def test_bucket_is_laps(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert write_api.write.call_args.kwargs['bucket'] == 'laps'

    def test_schema_version_field_present(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False)
        assert f'schema_version={_mod.SCHEMA_VERSION}i' in self._record(write_api)

    def test_handles_times_without_hours_component(self):
        ctx, write_api = self._ctx()  # start_epoc=0
        laps = [{'Lap': '1', 'LapTime': '1:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '45:30.000'}]
        _mod.push_influx(ctx, laps, False)
        record = self._record(write_api)
        assert 'lap_time=90000i' in record
        # TotalTime 45:30.000 = 2730000 ms; start_epoc=0 → timestamp 2730000
        assert record.endswith('2730000')

    def test_omits_lap_time_field_when_unparseable(self):
        ctx, write_api = self._ctx()
        laps = [{'Lap': '1', 'LapTime': '3$H', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}]
        _mod.push_influx(ctx, laps, False)
        assert 'lap_time' not in self._record(write_api)

    def test_unparseable_total_time_anchors_to_start_epoc(self):
        ctx, write_api = self._ctx()  # start_epoc=0
        laps = [{'Lap': '1', 'LapTime': '1:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '3$H'}]
        _mod.push_influx(ctx, laps, False)
        # TotalTime unparseable → falls back to 0; timestamp = start_epoc_ms + 0 = 0
        assert self._record(write_api).endswith(' 0')

    def test_explicit_car_number_overrides_ctx(self):
        ctx, write_api = self._ctx()  # ctx.car_number = '42'
        _mod.push_influx(ctx, self._laps(), False, car_number='99')
        record = self._record(write_api)
        assert 'car_number=99' in record
        assert 'car_number=42' not in record

    def test_passes_session_id_to_build_lap_points(self):
        ctx, _write_api = self._ctx()
        with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
            _mod.push_influx(ctx, self._laps(), False, session_id=77)
        assert mock_build.call_args.args[8] == 77


class TestSkipIfCompleteArg:
    def test_default_is_false(self):
        args = _mod._build_parser().parse_args(['12345', '42'])
        assert args.skip_if_complete is False

    def test_flag_sets_true(self):
        args = _mod._build_parser().parse_args(['12345', '42', '--skip-if-complete'])
        assert args.skip_if_complete is True


class TestOldRaceSkip:
    def _session_details(self):
        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [{
                    'Number': '42', 'Category': '1', 'ID': 1, 'SessionID': 1,
                    'RaceID': 999, 'FirstName': 'Jane', 'LastName': 'Doe',
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

    def _ctx(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.query_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        return ctx

    def test_skips_writes_when_complete_and_current(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 1)):
            with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                with patch.object(_mod, 'push_influx') as mock_push:
                    with patch.object(_mod, 'push_influx_race') as mock_race:
                        with patch.object(_mod, 'delete_existing_laps') as mock_del:
                            with patch.object(_mod, 'print_rankings'):
                                _mod.old_race(ctx, opts)
        mock_push.assert_not_called()
        mock_race.assert_called_once()
        mock_del.assert_not_called()

    def test_logs_skip_message_when_complete(self, caplog):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 1)):
            with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                with patch.object(_mod, 'push_influx'):
                    with patch.object(_mod, 'push_influx_race'):
                        with patch.object(_mod, 'print_rankings'):
                            with caplog.at_level(logging.INFO):
                                _mod.old_race(ctx, opts)
        assert any('SKIP' in r.message and '999' in r.message for r in caplog.records)

    def test_writes_when_laps_incomplete(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(0, 0)):
            with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert ctx.write_api.write.called

    def test_writes_when_laps_stale_schema(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 0)):
            with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert ctx.write_api.write.called

    def test_does_not_query_counts_when_skip_disabled(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=False)
        with patch.object(_mod, 'existing_lap_counts_fieldwide') as mock_counts:
            with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        mock_counts.assert_not_called()
        assert ctx.write_api.write.called


class TestExistingLapCounts:
    def _query_api(self, lap_no_count=0, current_count=0):
        """Mock query_api returning lap_no count for the first query and
        current-schema_version count for the second."""
        responses = iter([lap_no_count, current_count])

        def fake_query(flux):
            count = next(responses)
            table = MagicMock()
            if count is None:
                table.records = []
            else:
                rec = MagicMock()
                rec.get_value.return_value = count
                table.records = [rec]
            return [table]

        api = MagicMock()
        api.query.side_effect = fake_query
        return api

    def _ctx(self, query_api):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.query_api = query_api
        return ctx

    def test_returns_total_and_current_counts(self):
        ctx = self._ctx(self._query_api(lap_no_count=10, current_count=10))
        assert _mod.existing_lap_counts(ctx) == (10, 10)

    def test_current_less_than_total_when_some_laps_unstamped(self):
        ctx = self._ctx(self._query_api(lap_no_count=10, current_count=4))
        assert _mod.existing_lap_counts(ctx) == (10, 4)

    def test_returns_zero_when_no_laps(self):
        ctx = self._ctx(self._query_api(lap_no_count=None, current_count=None))
        assert _mod.existing_lap_counts(ctx) == (0, 0)

    def test_total_query_filters_lap_no_field(self):
        ctx = self._ctx(self._query_api(lap_no_count=1, current_count=1))
        _mod.existing_lap_counts(ctx)
        total_flux = ctx.query_api.query.call_args_list[0].args[0]
        assert '_field == "lap_no"' in total_flux
        assert 'race_id == "999"' in total_flux
        assert 'car_number == "42"' in total_flux

    def test_current_query_filters_on_schema_version_value(self):
        ctx = self._ctx(self._query_api(lap_no_count=1, current_count=1))
        _mod.existing_lap_counts(ctx)
        current_flux = ctx.query_api.query.call_args_list[1].args[0]
        assert '_field == "schema_version"' in current_flux
        assert f'_value == {_mod.SCHEMA_VERSION}' in current_flux


class TestArgParserCarNumber:
    def test_car_number_optional(self):
        args = _mod._build_parser().parse_args(['12345'])
        assert args.car_number is None

    def test_car_number_accepted_when_provided(self):
        args = _mod._build_parser().parse_args(['12345', '42'])
        assert args.car_number == 42


class TestExistingLapCountsFieldwide:
    def _query_api(self, lap_no_count=0, current_count=0):
        responses = iter([lap_no_count, current_count])

        def fake_query(flux):
            count = next(responses)
            table = MagicMock()
            rec = MagicMock()
            rec.get_value.return_value = count
            table.records = [rec]
            return [table]

        api = MagicMock()
        api.query.side_effect = fake_query
        return api

    def test_returns_total_and_current_schema_counts(self):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = self._query_api(lap_no_count=10, current_count=7)
        total, current = _mod.existing_lap_counts_fieldwide(ctx)
        assert total == 10
        assert current == 7

    def test_query_does_not_filter_by_car_number(self):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = self._query_api()
        _mod.existing_lap_counts_fieldwide(ctx)
        flux_calls = [c.args[0] for c in ctx.query_api.query.call_args_list]
        assert all('car_number' not in q for q in flux_calls)
        assert all('race_id == "999"' in q for q in flux_calls)


class TestOldRaceFieldwide:
    def _session_details(self, car_number='42'):
        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [{
                    'Number': car_number, 'Category': '1', 'ID': 1, 'SessionID': 1,
                    'RaceID': 999, 'FirstName': 'Jane', 'LastName': 'Doe',
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

    def test_proceeds_even_when_tracked_car_absent(self):
        """With no competitor_missing gate, old_race writes even when ctx.car_number absent."""
        ctx = _mod.RaceContext('999', '77', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        ctx.query_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        # session has car 42 only; tracked car 77 is absent
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details('42')
        with patch.object(_mod, 'delete_existing_laps') as mock_del:
            with patch.object(_mod, '_write_points_chunked'):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'push_influx_session'):
                        with patch.object(_mod, 'print_rankings'):
                            with patch.object(_mod, '_resolve_class_historical',
                                              return_value=('A', {1: 1})):
                                _mod.old_race(ctx, opts)
        mock_del.assert_called_once()

    def test_returns_early_when_no_competitors_have_laps(self, caplog):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        # competitor has empty LapTimes
        no_laps_details = self._session_details()
        no_laps_details['Session']['SortedCompetitors'][0]['LapTimes'] = []
        ctx.client.results.session_details.return_value = no_laps_details
        with patch.object(_mod, 'delete_existing_laps') as mock_del:
            with caplog.at_level(logging.WARNING):
                _mod.old_race(ctx, opts)
        mock_del.assert_not_called()
        assert any('No competitors' in r.message for r in caplog.records)


class TestOldRacePrintsClass:
    def _session_details(self):
        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'B-Class'}},
                'SortedCompetitors': [{
                    'Number': '42', 'Category': '1', 'ID': 1, 'SessionID': 1,
                    'RaceID': 999, 'FirstName': 'Jane', 'LastName': 'Doe',
                    'Position': '1', 'Laps': '1', 'LastLapTime': '',
                    'BestPosition': '1', 'BestLap': '1',
                    'BestLapTime': '0:01:30.000', 'TotalTime': '0:01:30.000',
                    'Transponder': 'T123', 'Nationality': '', 'AdditionalData': '',
                    'LapTimes': [
                        {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                         'FlagStatus': 0, 'TotalTime': '0:01:30.000'},
                    ],
                }],
            },
        }

class TestLiveRace:
    def _session_response(self, class_desc='A'):
        return {
            'Successful': True,
            'Session': {
                'RunNumber': '', 'SessionName': '', 'TrackName': '', 'TrackLength': '',
                'CurrentTime': '', 'SessionTime': '', 'TimeToGo': '', 'LapsToGo': '',
                'FlagStatus': '', 'SortMode': '',
                'Classes': {'classA': {'ClassID': 'classA', 'Description': class_desc}},
                'Competitors': {
                    'r1': {
                        'RacerID': 'r1', 'Number': '42', 'ClassID': 'classA',
                        'Position': '1', 'Transponder': 'T123', 'FirstName': 'Jane',
                        'LastName': 'Doe', 'Nationality': '', 'AdditionalData': '',
                        'Laps': '1', 'TotalTime': '', 'BestPosition': '1',
                        'BestLap': '1', 'BestLapTime': '', 'LastLapTime': '',
                    },
                },
            },
        }

    def _racer_response(self):
        return {
            'Successful': True,
            'Details': {
                'Laps': [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                          'FlagStatus': 0, 'TotalTime': '0:01:30.000'}],
                'Competitor': {
                    'Number': '42', 'FirstName': 'Jane', 'LastName': 'Doe',
                    'Transponder': 'T123', 'ClassID': 'classA', 'AdditionalData': '',
                    'Position': '1', 'Laps': '1', 'BestPosition': '1', 'BestLap': '1',
                    'BestLapTime': '0:01:30.000', 'TotalTime': '0:01:30.000',
                },
            },
        }

    def _ctx(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.client.live.get_session.return_value = self._session_response()
        ctx.client.live.get_racer.return_value = self._racer_response()
        return ctx

    def test_prints_class_name(self, capsys):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=False)
        with patch.object(_mod, 'print_rankings'):
            _mod.live_race(ctx, opts)
        assert 'Class: A' in capsys.readouterr().out

    def test_network_mode_calls_get_session_once(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'print_rankings'):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race'):
                    _mod.live_race(ctx, opts)
        assert ctx.client.live.get_session.call_count == 1


class TestLiveRaceStandingsWrite:
    def test_standings_written_at_startup_in_network_mode(self):
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 1000)
        ctx.metadata = _mod.RaceMetadata('Race', 'Track', None, 9999)
        opts = _mod.RaceOptions(network_mode=True, monitor_mode=False, interval=30)

        session_resp = {'Successful': True, 'Session': {
            'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}
        ctx.client.live.get_session.return_value = session_resp
        ctx.client.live.get_racer.return_value = {
            'Successful': True,
            'Details': {
                'Competitor': {
                    'FirstName': 'Ben', 'LastName': 'K', 'Number': '42',
                    'Transponder': 'T', 'BestPosition': '1', 'Position': '1',
                    'Laps': '5', 'BestLap': '3', 'BestLapTime': '1:30.000',
                    'TotalTime': '10:00.000', 'AdditionalData': None,
                },
                'Laps': [],
            },
        }

        with patch.object(_mod, 'push_influx_race'):
            with patch.object(_mod, 'push_influx_session'):
                with patch.object(_mod, 'push_influx'):
                    with patch.object(_mod, 'push_influx_standings_live') as mock_standings:
                        _mod.live_race(ctx, opts)

        mock_standings.assert_called_once_with(ctx, session_resp, 'sess-1')

    def test_standings_not_written_at_startup_when_not_network_mode(self):
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 1000)
        opts = _mod.RaceOptions(network_mode=False, monitor_mode=False, interval=30)

        ctx.client.live.get_session.return_value = {'Successful': True, 'Session': {
            'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}
        ctx.client.live.get_racer.return_value = {
            'Successful': True,
            'Details': {
                'Competitor': {
                    'FirstName': 'Ben', 'LastName': 'K', 'Number': '42',
                    'Transponder': 'T', 'BestPosition': '1', 'Position': '1',
                    'Laps': '5', 'BestLap': '3', 'BestLapTime': '1:30.000',
                    'TotalTime': '10:00.000', 'AdditionalData': None,
                },
                'Laps': [],
            },
        }

        with patch.object(_mod, 'push_influx_standings_live') as mock_standings:
            _mod.live_race(ctx, opts)

        mock_standings.assert_not_called()


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
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        mock_resolve.assert_any_call('42', self._session_details())

    def test_passes_class_name_to_build_lap_points(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_build.call_args.args[4] == 'A'

    def test_passes_session_start_epoc_to_build_lap_points(self):
        # ctx.start_epoc=9999 is intentionally different from SessionStartDateEpoc=5555
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 9999)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SessionStartDateEpoc'] = 5555
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_build.call_args.args[6] == 5555

    def test_handles_missing_session_start_epoc_key(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        del session['Session']['SessionStartDateEpoc']
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_build.call_args.args[6] is None

    def test_no_network_mode_skips_resolve(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical') as mock_resolve:
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        mock_resolve.assert_not_called()

    def test_passes_competitor_name_to_build_lap_points(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_build.call_args.args[2] == 'Jane Doe'

    def test_passes_car_info_to_build_lap_points(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SortedCompetitors'][0]['AdditionalData'] = '2005/Toy/Celica'
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_build.call_args.args[3] == '2005/Toy/Celica'

    def test_competitor_name_none_when_both_name_fields_empty(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SortedCompetitors'][0]['FirstName'] = ''
        session['Session']['SortedCompetitors'][0]['LastName'] = ''
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_build.call_args.args[2] is None

    def test_old_race_calls_push_influx_race_once_across_multiple_sessions(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 1000)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}, {'ID': 2}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race') as mock_race:
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_race.call_count == 1

    def test_old_race_push_influx_race_timestamp_uses_start_epoc(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 5000)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race') as mock_race:
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_race.call_args.args[1] == 5000 * 1000

    def test_old_race_push_influx_race_uses_wall_clock_when_start_epoc_zero(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race') as mock_race:
                    with patch.object(_mod, 'print_rankings'):
                        with patch.object(_mod.time, 'time', return_value=12345.0):
                            _mod.old_race(ctx, opts)
        assert mock_race.call_args.args[1] == 12345000

    def test_old_race_push_influx_race_not_called_when_not_network_mode(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, 'push_influx_race') as mock_race:
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        mock_race.assert_not_called()

    def test_deletes_existing_laps_before_push(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        order = []
        with patch.object(_mod, 'delete_existing_laps',
                          side_effect=lambda c: order.append('delete')) as mock_del:
            with patch.object(_mod, '_write_points_chunked',
                              side_effect=lambda *a, **k: order.append('write')):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        with patch.object(_mod, '_resolve_class_historical',
                                          return_value=('A', {1: 1})):
                            _mod.old_race(ctx, opts)
        mock_del.assert_called_once_with(ctx)
        assert order[0] == 'delete'
        assert 'write' in order

    def test_delete_fires_once_across_multiple_sessions(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {
            'Sessions': [{'ID': 1}, {'ID': 2}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, 'delete_existing_laps') as mock_del:
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        with patch.object(_mod, '_resolve_class_historical',
                                          return_value=('A', {1: 1})):
                            _mod.old_race(ctx, opts)
        mock_del.assert_called_once_with(ctx)

    def test_delete_fires_once_regardless_of_session_order(self):
        # Car 42 absent from session 1 (car 99 only), present in session 2.
        # Full-field writes: both sessions' competitors collected and written together.
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {
            'Sessions': [{'ID': 1}, {'ID': 2}]}
        ctx.client.results.session_details.side_effect = [
            self._session_details(car_number='99'),
            self._session_details(car_number='42'),
        ]
        with patch.object(_mod, 'delete_existing_laps') as mock_del:
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        with patch.object(_mod, '_resolve_class_historical',
                                          return_value=('A', {1: 1})):
                            _mod.old_race(ctx, opts)
        mock_del.assert_called_once_with(ctx)
        assert mock_build.call_count == 2

    def test_no_delete_when_not_network_mode(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=False)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, 'delete_existing_laps') as mock_del:
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        mock_del.assert_not_called()


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
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        mock_resolve.assert_called_once_with(
            ctx.client.live.get_session.return_value, ctx.car_number)

    def test_live_race_passes_class_name_to_push_influx(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 2)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('class_name') == 'A'

    def test_live_race_passes_none_class_name_when_car_not_in_session(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=(None, None)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'push_influx_race'):
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
                with patch.object(_mod, 'push_influx_race'):
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
            with patch.object(_mod, 'push_influx_race'):
                with patch.object(_mod, 'print_rankings'):
                    _mod.live_race(ctx, opts)
        mock_push.assert_not_called()

    def test_live_race_calls_push_influx_race_in_network_mode(self):
        ctx = self._make_ctx()
        ctx.start_epoc = 1000
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race') as mock_race:
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        mock_race.assert_called_once()

    def test_live_race_push_influx_race_timestamp_uses_start_epoc(self):
        ctx = self._make_ctx()
        ctx.start_epoc = 1000
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race') as mock_race:
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        assert mock_race.call_args.args[1] == 1000 * 1000

    def test_live_race_push_influx_race_timestamp_uses_wall_clock_when_start_epoc_zero(self):
        ctx = self._make_ctx()
        ctx.start_epoc = 0
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race') as mock_race:
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        assert mock_race.called
        assert mock_race.call_args.args[1] > 0

    def test_live_race_push_influx_race_called_even_when_no_laps(self):
        ctx = self._make_ctx()
        ctx.start_epoc = 1000
        ctx.client.live.get_racer.return_value['Details']['Laps'] = []
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx') as mock_push:
            with patch.object(_mod, 'push_influx_race') as mock_race:
                with patch.object(_mod, 'print_rankings'):
                    _mod.live_race(ctx, opts)
        mock_push.assert_not_called()
        mock_race.assert_called_once()

    def test_live_race_push_influx_race_not_called_when_not_network_mode(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=False)
        with patch.object(_mod, 'push_influx_race') as mock_race:
            with patch.object(_mod, 'print_rankings'):
                _mod.live_race(ctx, opts)
        mock_race.assert_not_called()

    def test_monitor_routine_push_influx_receives_only_new_lap(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        existing_laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
        ]
        new_laps = [
            *existing_laps,
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        mock_stop = MagicMock()
        mock_stop.wait.side_effect = [False, True]
        with patch.object(_mod, 'refresh_competitor', return_value=new_laps):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
                with patch.object(_mod, 'push_influx') as mock_push:
                    with patch.object(_mod, 'push_influx_race'):
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
        new_laps = [
            *existing_laps,
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        # wait() returns False first (enter loop body), then True (stop)
        mock_stop = MagicMock()
        mock_stop.wait.side_effect = [False, True]
        with patch.object(_mod, 'refresh_competitor', return_value=new_laps):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)) as mock_resolve:
                with patch.object(_mod, 'push_influx'):
                    with patch.object(_mod, 'push_influx_race'):
                        _mod.monitor_routine(ctx, existing_laps, opts, _stop_event=mock_stop)
        mock_resolve.assert_called_once_with(
            ctx.client.live.get_session.return_value, ctx.car_number)

    def test_live_race_passes_competitor_name_to_push_influx(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('competitor_name') == 'Jane Doe'

    def test_live_race_passes_car_info_to_push_influx(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True)
        competitor = ctx.client.live.get_racer.return_value['Details']['Competitor']
        competitor['AdditionalData'] = '2005/Toy/Celica'
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.live_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('car_info') == '2005/Toy/Celica'

    def _session_response_with_id(self, session_id=7, session_name='Day 1', class_desc='A'):
        return {
            'Successful': True,
            'Session': {
                'ID': session_id, 'Name': session_name,
                'RunNumber': '', 'SessionName': '', 'TrackName': '', 'TrackLength': '',
                'CurrentTime': '', 'SessionTime': '', 'TimeToGo': '', 'LapsToGo': '',
                'FlagStatus': '', 'SortMode': '',
                'Classes': {'classA': {'ClassID': 'classA', 'Description': class_desc}},
                'Competitors': {
                    'r1': {
                        'RacerID': 'r1', 'Number': '42', 'ClassID': 'classA',
                        'Position': '1', 'Transponder': '', 'FirstName': 'Jane',
                        'LastName': 'Doe', 'Nationality': '', 'AdditionalData': '',
                        'Laps': '1', 'TotalTime': '', 'BestPosition': '1',
                        'BestLap': '1', 'BestLapTime': '', 'LastLapTime': '',
                    },
                },
            },
        }

    def test_live_race_calls_push_influx_session_in_network_mode(self):
        ctx = self._make_ctx()
        ctx.delete_api = MagicMock()
        ctx.client.live.get_session.return_value = self._session_response_with_id(7, 'Day 1')
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx'):
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'push_influx_session') as mock_session:
                        with patch.object(_mod, 'print_rankings'):
                            _mod.live_race(ctx, opts)
        mock_session.assert_called_once()
        assert mock_session.call_args.args[1] == 7
        assert mock_session.call_args.args[2] == 'Day 1'

    def test_live_race_passes_session_id_to_push_influx(self):
        ctx = self._make_ctx()
        ctx.delete_api = MagicMock()
        ctx.client.live.get_session.return_value = self._session_response_with_id(7)
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'push_influx_session'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.live_race(ctx, opts)
        _, kwargs = mock_push.call_args
        assert kwargs.get('session_id') == 7

    def test_live_race_push_influx_session_not_called_when_not_network_mode(self):
        ctx = self._make_ctx()
        ctx.delete_api = MagicMock()
        ctx.client.live.get_session.return_value = self._session_response_with_id(7)
        opts = _mod.RaceOptions(network_mode=False)
        with patch.object(_mod, 'push_influx_session') as mock_session:
            with patch.object(_mod, 'print_rankings'):
                _mod.live_race(ctx, opts)
        mock_session.assert_not_called()

    def test_monitor_routine_passes_session_id_to_push_influx(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        existing_laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
        ]
        new_laps = [
            *existing_laps,
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        mock_stop = MagicMock()
        mock_stop.wait.side_effect = [False, True]
        ctx.client.live.get_session.return_value = {
            'Successful': True,
            'Session': {'ID': 55, 'Name': 'Session 1', 'Competitors': {}, 'Classes': {}},
        }
        with patch.object(_mod, 'refresh_competitor', return_value=new_laps):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
                with patch.object(_mod, 'push_influx') as mock_push:
                    with patch.object(_mod, 'push_influx_race'):
                        _mod.monitor_routine(ctx, existing_laps, opts,
                                             session_id=55, _stop_event=mock_stop)
        _, kwargs = mock_push.call_args
        assert kwargs.get('session_id') == 55

    def test_monitor_routine_forwards_competitor_name_and_car_info_to_push_influx(self):
        ctx = self._make_ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        existing_laps = [
            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:01:30.000'},
        ]
        new_laps = [
            *existing_laps,
            {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '3',
             'FlagStatus': '0', 'TotalTime': '0:03:01.000'},
        ]
        mock_stop = MagicMock()
        mock_stop.wait.side_effect = [False, True]
        with patch.object(_mod, 'refresh_competitor', return_value=new_laps):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
                with patch.object(_mod, 'push_influx') as mock_push:
                    with patch.object(_mod, 'push_influx_race'):
                        _mod.monitor_routine(ctx, existing_laps, opts,
                                             competitor_name='Jane Doe', car_info='2005/Toy/Celica',
                                             _stop_event=mock_stop)
        _, kwargs = mock_push.call_args
        assert kwargs.get('competitor_name') == 'Jane Doe'
        assert kwargs.get('car_info') == '2005/Toy/Celica'


class TestMonitorRoutineEpocRecheck:
    # A lap that is already in the laps list — refresh_competitor returns it so the
    # "new lap" branch never executes, keeping tests focused on the recheck logic.
    _existing_lap: ClassVar[dict] = {
        'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
        'FlagStatus': '0', 'TotalTime': '0:01:30.000',
    }

    def _make_ctx(self, start_epoc=0):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), start_epoc)
        ctx.delete_api = MagicMock()
        ctx.metadata = _mod.RaceMetadata(
            race_name='Test', track_name='Track', series_name=None, end_time_epoc=0)
        ctx.client.race.details.return_value = {'Successful': False}
        return ctx

    def _stop_after(self, n):
        stop = MagicMock()
        stop.wait.side_effect = [False] * n + [True]
        return stop

    def test_rechecks_race_details_each_iteration_when_start_epoc_zero(self):
        ctx = self._make_ctx(start_epoc=0)
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            with patch.object(_mod, 'push_influx_race'):
                _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                     _stop_event=self._stop_after(1))
        ctx.client.race.details.assert_called_once_with(ctx.race_id)

    def test_does_not_recheck_when_start_epoc_nonzero(self):
        ctx = self._make_ctx(start_epoc=1000)
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                 _stop_event=self._stop_after(1))
        ctx.client.race.details.assert_not_called()

    def test_does_not_recheck_when_not_network_mode(self):
        ctx = self._make_ctx(start_epoc=0)
        opts = _mod.RaceOptions(network_mode=False, interval=30)
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                 _stop_event=self._stop_after(1))
        ctx.client.race.details.assert_not_called()

    def test_updates_ctx_start_epoc_when_api_returns_nonzero(self):
        ctx = self._make_ctx(start_epoc=0)
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        ctx.client.race.details.return_value = {
            'Successful': True,
            'Race': {'StartDateEpoc': 5000, 'EndDateEpoc': 9000, 'Name': '', 'Track': ''},
        }
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            with patch.object(_mod, 'push_influx_race'):
                _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                     _stop_event=self._stop_after(1))
        assert ctx.start_epoc == 5000

    def test_calls_push_influx_race_with_updated_timestamp(self):
        ctx = self._make_ctx(start_epoc=0)
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        ctx.client.race.details.return_value = {
            'Successful': True,
            'Race': {'StartDateEpoc': 5000, 'EndDateEpoc': 9000, 'Name': '', 'Track': ''},
        }
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            with patch.object(_mod, 'push_influx_race') as mock_race:
                _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                     _stop_event=self._stop_after(1))
        mock_race.assert_called_once_with(ctx, 5000 * 1000)

    def test_updates_metadata_end_time_epoc_when_api_returns_epoc(self):
        ctx = self._make_ctx(start_epoc=0)
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        ctx.client.race.details.return_value = {
            'Successful': True,
            'Race': {'StartDateEpoc': 5000, 'EndDateEpoc': 9000, 'Name': '', 'Track': ''},
        }
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            with patch.object(_mod, 'push_influx_race'):
                _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                     _stop_event=self._stop_after(1))
        assert ctx.metadata.end_time_epoc == 9000

    def test_stops_rechecking_once_start_epoc_set(self):
        ctx = self._make_ctx(start_epoc=0)
        opts = _mod.RaceOptions(network_mode=True, interval=30)
        ctx.client.race.details.return_value = {
            'Successful': True,
            'Race': {'StartDateEpoc': 5000, 'EndDateEpoc': 9000, 'Name': '', 'Track': ''},
        }
        with patch.object(_mod, 'refresh_competitor', return_value=[self._existing_lap]):
            with patch.object(_mod, 'push_influx_race'):
                _mod.monitor_routine(ctx, [self._existing_lap], opts,
                                     _stop_event=self._stop_after(2))
        # First iteration sets epoc; second iteration skips the re-check
        assert ctx.client.race.details.call_count == 1


class TestResolveRaceMetadata:
    def _race_details(self, series_id=145):
        return {
            'Successful': True,
            'Race': {
                'ID': 166153,
                'Name': 'The Sausage Fest 2026',
                'SeriesID': series_id,
                'Track': 'Road America',
                'EndDateEpoc': 1749132000,
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

    def test_end_time_epoc_extracted(self):
        meta = _mod._resolve_race_metadata(self._race_details(), self._client_with_series())
        assert meta.end_time_epoc == 1749132000

    def test_end_time_epoc_zero_when_unsuccessful(self):
        meta = _mod._resolve_race_metadata({'Successful': False}, MagicMock())
        assert meta.end_time_epoc == 0


class TestPushInfluxRace:
    def _ctx(self):
        write_api = MagicMock()
        delete_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 1000000)
        ctx.delete_api = delete_api
        ctx.metadata = _mod.RaceMetadata(
            race_name='The Sausage Fest 2026',
            track_name='Road America',
            series_name='24 Hours of Lemons',
            end_time_epoc=1749132000,
        )
        return ctx, write_api, delete_api

    def _record(self, write_api):
        return write_api.write.call_args.kwargs['record'].to_line_protocol()

    def test_calls_delete_before_write(self):
        ctx, write_api, delete_api = self._ctx()
        call_order = []
        delete_api.delete.side_effect = lambda **kw: call_order.append('delete')
        write_api.write.side_effect = lambda **kw: call_order.append('write')
        _mod.push_influx_race(ctx, 5000000)
        assert call_order == ['delete', 'write']

    def test_delete_targets_correct_race_id(self):
        ctx, _write_api, delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        predicate = delete_api.delete.call_args.kwargs['predicate']
        assert 'race_id="999"' in predicate
        assert '_measurement="race"' in predicate

    def test_delete_targets_races_bucket(self):
        ctx, _write_api, delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert delete_api.delete.call_args.kwargs['bucket'] == 'races'

    def test_writes_to_races_bucket(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert write_api.write.call_args.kwargs['bucket'] == 'races'

    def test_measurement_is_race(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert self._record(write_api).startswith('race,')

    def test_race_id_tag(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert 'race_id=999' in self._record(write_api)

    def test_track_name_tag(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert 'track_name=Road\\ America' in self._record(write_api)

    def test_end_time_epoc_field(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert 'end_time_epoc=1749132000i' in self._record(write_api)

    def test_uses_provided_timestamp(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000)
        assert self._record(write_api).endswith('5000')

    def test_exception_during_delete_is_logged_not_raised(self):
        ctx, _write_api, delete_api = self._ctx()
        delete_api.delete.side_effect = Exception("network error")
        _mod.push_influx_race(ctx, 5000000)  # must not raise

    def test_exception_during_write_is_logged_not_raised(self):
        ctx, write_api, _delete_api = self._ctx()
        write_api.write.side_effect = Exception("network error")
        _mod.push_influx_race(ctx, 5000000)  # must not raise

    def test_omits_series_name_tag_when_none(self):
        ctx, write_api, _delete_api = self._ctx()
        ctx.metadata = _mod.RaceMetadata(
            race_name='Race', track_name='Track', series_name=None, end_time_epoc=0)
        _mod.push_influx_race(ctx, 1000)
        assert 'series_name=' not in self._record(write_api)

    def test_metadata_none_skips_delete_and_write(self):
        ctx, write_api, delete_api = self._ctx()
        ctx.metadata = None
        _mod.push_influx_race(ctx, 1000)
        delete_api.delete.assert_not_called()
        write_api.write.assert_not_called()


class TestPushInfluxSession:
    def _ctx(self):
        write_api = MagicMock()
        delete_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 1000000)
        ctx.delete_api = delete_api
        return ctx, write_api, delete_api

    def _record(self, write_api):
        return write_api.write.call_args.kwargs['record'].to_line_protocol()

    def test_calls_delete_before_write(self):
        ctx, write_api, delete_api = self._ctx()
        call_order = []
        delete_api.delete.side_effect = lambda **kw: call_order.append('delete')
        write_api.write.side_effect = lambda **kw: call_order.append('write')
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert call_order == ['delete', 'write']

    def test_delete_targets_correct_session_id(self):
        ctx, _write_api, delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        predicate = delete_api.delete.call_args.kwargs['predicate']
        assert 'session_id="42"' in predicate
        assert '_measurement="session"' in predicate

    def test_delete_targets_race_sessions_bucket(self):
        ctx, _write_api, delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert delete_api.delete.call_args.kwargs['bucket'] == 'race_sessions'

    def test_writes_to_race_sessions_bucket(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert write_api.write.call_args.kwargs['bucket'] == 'race_sessions'

    def test_measurement_is_session(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert self._record(write_api).startswith('session,')

    def test_race_id_tag(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert 'race_id=999' in self._record(write_api)

    def test_session_id_tag(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert 'session_id=42' in self._record(write_api)

    def test_session_name_field(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert 'session_name="Day 1"' in self._record(write_api)

    def test_start_epoc_field(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert 'start_epoc=1700000000i' in self._record(write_api)

    def test_timestamp_uses_start_epoc_ms(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert self._record(write_api).endswith('1700000000000')

    def test_exception_during_delete_is_logged_not_raised(self, caplog):
        ctx, _write_api, delete_api = self._ctx()
        delete_api.delete.side_effect = Exception('network error')
        with caplog.at_level(logging.ERROR):
            _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)  # must not raise
        assert "Writing session failed" in caplog.text

    def test_exception_during_write_is_logged_not_raised(self, caplog):
        ctx, write_api, _delete_api = self._ctx()
        write_api.write.side_effect = Exception('network error')
        with caplog.at_level(logging.ERROR):
            _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)  # must not raise
        assert "Writing session failed" in caplog.text

    def test_start_epoc_none_writes_zero(self):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', None)
        assert 'start_epoc=0i' in self._record(write_api)
        assert self._record(write_api).endswith('0')


class TestDeleteExistingLaps:
    def _ctx(self):
        delete_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = delete_api
        return ctx, delete_api

    def test_predicate_targets_measurement_and_race(self):
        ctx, delete_api = self._ctx()
        _mod.delete_existing_laps(ctx)
        predicate = delete_api.delete.call_args.kwargs['predicate']
        assert '_measurement="lap"' in predicate
        assert 'race_id="999"' in predicate
        assert 'car_number' not in predicate

    def test_targets_laps_bucket(self):
        ctx, delete_api = self._ctx()
        _mod.delete_existing_laps(ctx)
        assert delete_api.delete.call_args.kwargs['bucket'] == 'laps'

    def test_delete_failure_is_swallowed_and_logged(self, caplog):
        ctx, delete_api = self._ctx()
        delete_api.delete.side_effect = Exception("network error")
        _mod.delete_existing_laps(ctx)  # must not raise
        assert "Deleting existing laps failed" in caplog.text


class TestOldRaceFullField:
    def _make_competitor(self, number, comp_id, position):
        return {
            'Number': number, 'Category': '1',
            'ID': comp_id, 'SessionID': 1, 'RaceID': 999,
            'FirstName': 'Driver', 'LastName': number,
            'Position': position, 'Laps': '1', 'LastLapTime': '',
            'BestPosition': position, 'BestLap': '1',
            'BestLapTime': '0:01:30.000', 'TotalTime': '0:01:30.000',
            'Transponder': '', 'Nationality': '', 'AdditionalData': '',
            'LapTimes': [
                {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': position,
                 'FlagStatus': 0, 'TotalTime': '0:01:30.000'},
            ],
        }

    def _session_details_two_cars(self):
        """Session with tracked car 42 and competitor 99."""
        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [
                    self._make_competitor('42', 1, '1'),
                    self._make_competitor('99', 2, '2'),
                ],
            },
        }

    def _ctx(self, session_details, car_number='42'):
        ctx = _mod.RaceContext('999', car_number, MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session_details
        return ctx

    def test_writes_all_competitors_not_just_tracked(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        assert mock_build.call_count == 2
        built_car_numbers = {c.args[7] for c in mock_build.call_args_list}
        assert built_car_numbers == {'42', '99'}

    def test_resolve_class_called_per_competitor(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(
            _mod, '_resolve_class_historical', return_value=('A', {1: 1})
        ) as mock_resolve:
            with patch.object(_mod, 'push_influx_race'):
                with patch.object(_mod, 'delete_existing_laps'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert mock_resolve.call_count == 2
        assert {c.args[0] for c in mock_resolve.call_args_list} == {'42', '99'}

    def test_build_lap_points_receives_explicit_car_number(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        for c in mock_build.call_args_list:
            assert c.args[7] is not None

    def test_race_not_stamped_when_write_fails(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_write_points_chunked', side_effect=Exception('write error')):
                with patch.object(_mod, 'push_influx_race') as mock_stamp:
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        mock_stamp.assert_not_called()

    def test_non_integer_car_number_skipped(self):
        """Competitors with non-integer car numbers (e.g. 'SC') are not written."""
        sc_competitor = self._make_competitor('SC', 99, '0')
        session = {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [
                    self._make_competitor('42', 1, '1'),
                    sc_competitor,
                ],
            },
        }
        ctx = self._ctx(session)
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        built_car_numbers = {c.args[7] for c in mock_build.call_args_list}
        assert 'SC' not in built_car_numbers
        assert '42' in built_car_numbers

    def test_validation_aborts_when_no_laps_collected(self):
        """Tracked car found but has zero laps — do not touch InfluxDB."""
        competitor_no_laps = self._make_competitor('42', 1, '1')
        competitor_no_laps['LapTimes'] = []
        session = {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [competitor_no_laps],
            },
        }
        ctx = self._ctx(session)
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'delete_existing_laps') as mock_del:
            with patch.object(_mod, 'push_influx_race'):
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        assert not ctx.write_api.write.called
        mock_del.assert_not_called()

    def test_push_influx_session_called_once_per_session(self):
        ctx = self._ctx(self._session_details_two_cars())
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}, {'ID': 2}]}
        ctx.client.results.session_details.return_value = self._session_details_two_cars()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx_session') as mock_session:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        assert mock_session.call_count == 2

    def test_push_influx_session_called_with_correct_session_id(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, 'push_influx_session') as mock_session:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        session_ids = {c.args[1] for c in mock_session.call_args_list}
        assert 1 in session_ids

    def test_push_influx_session_not_called_when_not_network_mode(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), None, 0)
        ctx.delete_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details_two_cars()
        opts = _mod.RaceOptions(network_mode=False)
        with patch.object(_mod, 'push_influx_session') as mock_session:
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        mock_session.assert_not_called()

    def test_build_lap_points_receives_session_id(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with patch.object(_mod, '_build_lap_points', return_value=[]) as mock_build:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        for c in mock_build.call_args_list:
            assert c.args[8] == 1  # session_id from session details ID=1


class TestBuildLapPointsSessionId:
    def _laps(self):
        return [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}]

    def _build(self, session_id):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        points = _mod._build_lap_points(
            ctx, self._laps(), 'Jane Doe', None, 'A', None, 0, '42', session_id)
        return points[0].to_line_protocol()

    def test_session_id_tag_integer(self):
        assert 'session_id=7' in self._build(7)

    def test_session_id_tag_string(self):
        assert 'session_id=99' in self._build('99')

    def test_session_id_tag_absent_when_none(self):
        assert 'session_id' not in self._build(None)


class TestBuildLapPointsNonNumericPosition:
    def _ctx(self):
        return _mod.RaceContext('138911', '77', MagicMock(), MagicMock(), 0)

    def _make_lap(self, lap='227', position='1'):
        return {'Lap': lap, 'LapTime': '104', 'Position': position,
                'FlagStatus': 'Green', 'TotalTime': '121'}

    @pytest.mark.parametrize("bad_pos", ['$G', 'P1', '--'])
    def test_non_numeric_position_omits_field(self, bad_pos):
        laps = [self._make_lap(position=bad_pos)]
        points = _mod._build_lap_points(self._ctx(), laps, 'Car 77', None, 'Gold', None, 0, '77')
        assert len(points) == 1
        assert 'position=' not in points[0].to_line_protocol()

    def test_none_position_omits_field(self):
        laps = [self._make_lap(position=None)]
        points = _mod._build_lap_points(self._ctx(), laps, 'Car 77', None, 'Gold', None, 0, '77')
        assert len(points) == 1
        assert 'position=' not in points[0].to_line_protocol()

    def test_lap_number_preserved_when_position_bad(self):
        laps = [self._make_lap(lap='227', position='$G')]
        points = _mod._build_lap_points(self._ctx(), laps, 'Car 77', None, 'Gold', None, 0, '77')
        assert 'lap_no=227i' in points[0].to_line_protocol()

    def test_numeric_position_written_normally(self):
        laps = [self._make_lap(lap='227', position='3')]
        points = _mod._build_lap_points(self._ctx(), laps, 'Car 77', None, 'Gold', None, 0, '77')
        assert 'position=3i' in points[0].to_line_protocol()


class TestPrintRankingsNonNumericPosition:
    def _competitor(self, position='1', number='42', name='Jane', category='1', laps='5'):
        return {
            'Position': position, 'Number': number, 'FirstName': name, 'LastName': '',
            'Laps': laps, 'Category': category, 'Transponder': 'T1',
        }

    def _categories(self):
        return {'1': {'ID': '1', 'Name': 'Gold'}}

    @pytest.mark.parametrize("bad_pos", ['6$G', '$G', 'P1', '--'])
    def test_non_numeric_position_does_not_crash(self, bad_pos):
        competitors = [self._competitor(position=bad_pos)]
        _mod.print_rankings(competitors, False, None, self._categories())

    @pytest.mark.parametrize("bad_pos", ['6$G', '$G', 'P1', '--'])
    def test_non_numeric_position_with_selected_class_does_not_crash(self, bad_pos):
        competitors = [self._competitor(position=bad_pos)]
        _mod.print_rankings(competitors, False, (None, 'gold'), self._categories())


class TestPrintRankingsSortOrder:
    """Behavioral tests using real pandas (conftest mocks pandas; patch it back)."""

    def _competitor(self, position='1', number='42', name='Jane', category='1', laps='5'):
        return {
            'Position': position, 'Number': number, 'FirstName': name, 'LastName': '',
            'Laps': laps, 'Category': category, 'Transponder': 'T1',
        }

    def _categories(self):
        return {'1': {'ID': '1', 'Name': 'Gold'}}

    def test_numeric_position_sorts_before_non_numeric(self, capsys):
        import sys
        # conftest mocks pandas in sys.modules; pop it to import the real one
        pandas_mock = sys.modules.pop('pandas', None)
        try:
            import pandas as real_pandas
        finally:
            if pandas_mock is not None:
                sys.modules['pandas'] = pandas_mock
        competitors = [
            self._competitor(position='6$G', number='99'),
            self._competitor(position='1', number='42'),
        ]
        with patch.object(_mod, 'pandas', real_pandas):
            _mod.print_rankings(competitors, False, None, self._categories())
        out = capsys.readouterr().out
        assert out.index('42') < out.index('99')


class TestWritePointsChunked:
    def test_single_chunk_when_below_batch_size(self):
        write_api = MagicMock()
        points = [MagicMock() for _ in range(10)]
        _mod._write_points_chunked(write_api, points, batch_size=5000)
        write_api.write.assert_called_once_with(bucket='laps', record=points)

    def test_splits_into_two_chunks_at_boundary(self):
        write_api = MagicMock()
        points = [MagicMock() for _ in range(6)]
        _mod._write_points_chunked(write_api, points, batch_size=5)
        assert write_api.write.call_count == 2
        assert write_api.write.call_args_list[0] == call(bucket='laps', record=points[:5])
        assert write_api.write.call_args_list[1] == call(bucket='laps', record=points[5:])

    def test_empty_points_makes_no_calls(self):
        write_api = MagicMock()
        _mod._write_points_chunked(write_api, [], batch_size=5000)
        write_api.write.assert_not_called()


class TestDryRun:
    def _ctx(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), None, 0)
        ctx.delete_api = None
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = {
            'Successful': True,
            'Session': {
                'ID': 1, 'RaceID': 999, 'Name': 'S1', 'SessionStartDateEpoc': 0,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [
                    {
                        'Number': '42', 'Category': '1', 'ID': 1, 'SessionID': 1,
                        'RaceID': 999, 'FirstName': 'Driver', 'LastName': '42',
                        'Position': '1', 'Laps': '2', 'LastLapTime': '',
                        'BestPosition': '1', 'BestLap': '1',
                        'BestLapTime': '0:01:30.000', 'TotalTime': '0:03:00.000',
                        'Transponder': '', 'Nationality': '', 'AdditionalData': '',
                        'LapTimes': [
                            {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                             'FlagStatus': 0, 'TotalTime': '0:01:30.000'},
                            {'Lap': '2', 'LapTime': '0:01:30.000', 'Position': '1',
                             'FlagStatus': 0, 'TotalTime': '0:03:00.000'},
                        ],
                    },
                ],
            },
        }
        return ctx

    def test_does_not_call_push_influx_or_delete(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, dry_run=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1, 2: 1})):
            with patch.object(_mod, 'push_influx') as mock_push:
                with patch.object(_mod, 'delete_existing_laps') as mock_del:
                    with patch.object(_mod, 'push_influx_race') as mock_stamp:
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        mock_push.assert_not_called()
        mock_del.assert_not_called()
        mock_stamp.assert_not_called()

    def test_prints_lap_summary(self, capsys):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, dry_run=True)
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1, 2: 1})):
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        out = capsys.readouterr().out
        assert 'car 42' in out
        assert '2 laps' in out


class TestComputeClassPositionsLive:
    def _resp(self, competitors, classes=None):
        return {
            'Successful': True,
            'Session': {
                'Competitors': {str(i): c for i, c in enumerate(competitors)},
                'Classes': classes or {},
            },
        }

    def _comp(self, number, class_id, position):
        return {'Number': number, 'ClassID': class_id, 'Position': position}

    def test_single_class_ranked_by_position(self):
        resp = self._resp([
            self._comp('42', 'A', '1'),
            self._comp('7',  'A', '2'),
            self._comp('99', 'A', '3'),
        ])
        result = _mod._compute_class_positions_live(resp)
        assert result == {'42': 1, '7': 2, '99': 3}

    def test_multiple_classes_ranked_independently(self):
        resp = self._resp([
            self._comp('42', 'A', '1'),
            self._comp('7',  'B', '2'),
            self._comp('99', 'A', '3'),
            self._comp('5',  'B', '4'),
        ])
        result = _mod._compute_class_positions_live(resp)
        assert result['42'] == 1  # class A, overall 1st
        assert result['99'] == 2  # class A, overall 3rd
        assert result['7']  == 1  # class B, overall 2nd
        assert result['5']  == 2  # class B, overall 4th

    def test_non_numeric_position_excluded(self):
        resp = self._resp([
            self._comp('42', 'A', '1'),
            self._comp('7',  'A', 'N/A'),
        ])
        result = _mod._compute_class_positions_live(resp)
        assert '7' not in result
        assert result['42'] == 1

    def test_single_car_class_gets_position_1(self):
        resp = self._resp([self._comp('42', 'A', '5')])
        result = _mod._compute_class_positions_live(resp)
        assert result == {'42': 1}

    def test_unsuccessful_response_returns_empty(self):
        assert _mod._compute_class_positions_live({'Successful': False}) == {}


class TestPushInfluxStandingsLive:
    def _resp(self, competitors, classes=None):
        return {
            'Successful': True,
            'Session': {
                'Competitors': {str(i): c for i, c in enumerate(competitors)},
                'Classes': classes or {},
            },
        }

    def _comp(self, number='42', class_id='A', position='1', laps='5',
              best='1:30.000', last='1:31.000'):
        return {
            'Number': number, 'ClassID': class_id, 'Position': position,
            'Laps': laps, 'FirstName': 'Ben', 'LastName': 'K',
            'AdditionalData': None, 'BestLapTime': best, 'LastLapTime': last,
        }

    def test_writes_one_point_per_competitor(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp('42'), self._comp('7', position='2')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        mock_write.assert_called_once()
        assert len(mock_write.call_args[0][1]) == 2

    def test_measurement_is_standings(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert lp.startswith('standings,')

    def test_session_id_tagged(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-99')
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'session_id=sess-99' in lp

    def test_session_id_none_omits_tag(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, None)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'session_id' not in lp

    def test_non_numeric_position_skips_competitor(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp(position='N/A')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        mock_write.assert_not_called()

    def test_unparseable_lap_times_omitted(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp(best='', last='')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'best_lap_time' not in lp
        assert 'last_lap_time' not in lp

    def test_unsuccessful_response_writes_nothing(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, {'Successful': False}, 'sess-1')
        mock_write.assert_not_called()

    def test_returns_curr_standings_dict(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp('42', position='1'), self._comp('7', position='2')])
        with patch.object(_mod, '_write_points_chunked'):
            result = _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        assert '42' in result
        assert '7' in result

    def test_unchanged_standings_skips_write(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp('42', position='1', laps='5'),
                           self._comp('7', position='2', laps='5')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            prev = _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        mock_write.assert_called_once()
        with patch.object(_mod, '_write_points_chunked') as mock_write2:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1', prev)
        mock_write2.assert_not_called()

    def test_any_position_change_writes_all_competitors(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp1 = self._resp([self._comp('42', position='2', laps='5'),
                            self._comp('7', position='1', laps='5')])
        resp2 = self._resp([self._comp('42', position='1', laps='5'),
                            self._comp('7', position='2', laps='5')])
        with patch.object(_mod, '_write_points_chunked'):
            prev = _mod.push_influx_standings_live(ctx, resp1, 'sess-1')
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp2, 'sess-1', prev)
        mock_write.assert_called_once()
        assert len(mock_write.call_args[0][1]) == 2

    def test_new_lap_triggers_write(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp1 = self._resp([self._comp('42', position='1', laps='5')])
        resp2 = self._resp([self._comp('42', position='1', laps='6')])
        with patch.object(_mod, '_write_points_chunked'):
            prev = _mod.push_influx_standings_live(ctx, resp1, 'sess-1')
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp2, 'sess-1', prev)
        mock_write.assert_called_once()

    def test_none_prev_standings_writes_all(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp('42'), self._comp('7', position='2')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1', None)
        mock_write.assert_called_once()
        assert len(mock_write.call_args[0][1]) == 2

    def test_unsuccessful_response_returns_prev_standings(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        prev = {'42': (1, 5, None, None, None)}
        result = _mod.push_influx_standings_live(ctx, {'Successful': False}, 'sess-1', prev)
        assert result == prev

    def test_write_failure_returns_prev_standings_for_retry(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp1 = self._resp([self._comp('42', position='1', laps='5')])
        resp2 = self._resp([self._comp('42', position='1', laps='6')])
        with patch.object(_mod, '_write_points_chunked'):
            prev = _mod.push_influx_standings_live(ctx, resp1, 'sess-1')
        with patch.object(_mod, '_write_points_chunked', side_effect=Exception("influx down")):
            result = _mod.push_influx_standings_live(ctx, resp2, 'sess-1', prev)
        assert result == prev


class TestPushInfluxStandingsHistorical:
    def _entry(self, competitors):
        return {
            'session_id': 101,
            'session_name': 'Race',
            'start_epoc': 1700000000,
            'competitors': competitors,
        }

    def _comp(self, car_number='42', position='3', laps='50', class_name='B',
              class_positions=None, best='1:30.000', last='1:31.000'):
        return {
            'car_number': car_number,
            'competitor_name': 'Ben K',
            'car_info': None,
            'class_name': class_name,
            'class_positions': class_positions if class_positions is not None else {50: 2},
            'influx_laps': [],
            'final_position': position,
            'final_laps': laps,
            'best_lap_time': best,
            'last_lap_time': last,
        }

    def test_writes_one_point_per_competitor(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp('42'), self._comp('7', position='5')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        mock_write.assert_called_once()
        assert len(mock_write.call_args[0][1]) == 2

    def test_measurement_is_standings(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert lp.startswith('standings,')

    def test_uses_final_class_position(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        comp = self._comp(class_positions={1: 3, 2: 2, 3: 1})
        entry = self._entry([comp])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'class_position=1i' in lp

    def test_session_id_tagged(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'session_id=101' in lp

    def test_non_numeric_position_skipped(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp(position='DNF')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        mock_write.assert_not_called()

    def test_unparseable_lap_times_omitted(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp(best='', last='')])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'best_lap_time' not in lp
        assert 'last_lap_time' not in lp

    def test_empty_class_positions_omits_class_position_field(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp(class_positions={})])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'class_position' not in lp
