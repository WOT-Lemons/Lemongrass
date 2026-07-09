import logging
import os
import threading
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import MagicMock, call, mock_open, patch

import pytest

import lemongrass._env as _env_mod
import lemongrass.laps as _mod


def _single_car_session_details(car_number='42', category='1', category_name='A',
                                session_id=1, race_id=999, session_name='S1',
                                start_epoc=0):
    """Standard single-competitor session-details payload shared by old_race tests.

    Carries the full ~20-key competitor dict (a superset of what any old_race path
    reads) so callers can delegate here instead of re-declaring it. Override the
    keyword args for the few fields individual tests vary.
    """
    return {
        'Successful': True,
        'Session': {
            'ID': session_id, 'RaceID': race_id, 'Name': session_name,
            'SessionStartDateEpoc': start_epoc,
            'Categories': {category: {'ID': category, 'Name': category_name}},
            'SortedCompetitors': [{
                'Number': car_number, 'Category': category, 'ID': 1,
                'SessionID': session_id, 'RaceID': race_id,
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


@pytest.fixture
def single_car_session():
    """Fixture exposing the shared single-competitor session-details factory."""
    return _single_car_session_details


class TestResolveTokens:
    def test_multi_tokens_returns_list(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN1,TOKEN2'}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == ['TOKEN1', 'TOKEN2']

    def test_single_racemonitor_tokens_returns_string(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN1'}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == 'TOKEN1'

    def test_falls_back_to_racemonitor_token(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKEN': 'FALLBACK'}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == 'FALLBACK'

    def test_returns_empty_string_when_neither_set(self):
        with patch.dict(os.environ, {}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == ''

    def test_racemonitor_tokens_takes_priority(self):
        with patch.dict(os.environ,
                        {'RACEMONITOR_TOKENS': 'MULTI1,MULTI2', 'RACEMONITOR_TOKEN': 'SINGLE'},
                        clear=True):
            result = _env_mod.resolve_tokens()
        assert result == ['MULTI1', 'MULTI2']

    def test_strips_whitespace_from_tokens(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': ' TOKEN1 , TOKEN2 '}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == ['TOKEN1', 'TOKEN2']

    def test_whitespace_only_racemonitor_tokens_returns_empty_string(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': '  ,  , '}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == ''

    @pytest.mark.parametrize("tokens_value", ['  ,  , ', '  ', '', ','])
    def test_empty_ish_racemonitor_tokens_falls_back_to_single_token(self, tokens_value):
        # Whatever the empty-ish shape (all-whitespace slots, no commas, empty,
        # bare comma), an empty RACEMONITOR_TOKENS falls back to RACEMONITOR_TOKEN.
        with patch.dict(os.environ,
                        {'RACEMONITOR_TOKENS': tokens_value, 'RACEMONITOR_TOKEN': 'FALLBACK'},
                        clear=True):
            result = _env_mod.resolve_tokens()
        assert result == 'FALLBACK'

    def test_middle_empty_slot_is_silently_dropped(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN1,,TOKEN2'}, clear=True):
            result = _env_mod.resolve_tokens()
        assert result == ['TOKEN1', 'TOKEN2']


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

        session_resp = {'Successful': True, 'Session': {
            'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}

        call_count = 0
        def fake_get_session(race_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return session_resp

        ctx.client.live.get_session.side_effect = fake_get_session

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod, 'push_influx_standings_live') as mock_standings:
                with patch.object(_mod, 'push_influx'):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop)

        # prev_standings starts as {} on the first poll; the real session_response
        # (not the mock's own recorded arg) is the expected second argument.
        mock_standings.assert_called_once_with(ctx, session_resp, 'sess-1', {})

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

    def test_skips_lap_with_streaming_command_number_and_logs_command_name(self, caplog):
        import logging
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        bad_lap = {'Lap': '$J', 'LapTime': '1:00.000'}

        def fake_refresh(c):
            stop.set()
            return [bad_lap]

        ctx.client.live.get_session.return_value = {'Successful': False}
        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            with caplog.at_level(logging.WARNING):
                _mod.monitor_routine(ctx, [], opts, _stop_event=stop)

        assert 'Passing Information' in caplog.text

    def test_refresh_competitor_exception_does_not_kill_monitor(self):
        """A transient network error mid-poll must cost one poll, not the race."""
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)

        calls = 0

        def flaky_refresh(c):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("wifi blip")
            stop.set()
            return []

        with patch.object(_mod, 'refresh_competitor', side_effect=flaky_refresh):
            with patch.object(ctx.client.live, 'get_session',
                              return_value={'Successful': False}):
                result = _mod.monitor_routine(ctx, [], opts, _stop_event=stop)
        assert calls == 2  # survived the first failure and polled again
        assert result is None

    def test_race_details_exception_does_not_kill_monitor(self):
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True, interval=0)
        ctx.client.race.details.side_effect = ConnectionError("wifi blip")

        def fake_refresh(c):
            stop.set()
            return []

        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            with patch.object(ctx.client.live, 'get_session',
                              return_value={'Successful': False}):
                with patch.object(_mod, 'push_influx_standings_live', return_value={}):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop)
        # no exception raised is the assertion

    def test_get_session_exception_does_not_kill_monitor(self):
        """A get_session network error mid-poll is swallowed; the poll continues."""
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=0)
        ctx.client.live.get_session.side_effect = ConnectionError("wifi blip")

        calls = 0

        def fake_refresh(c):
            nonlocal calls
            calls += 1
            stop.set()
            return []

        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            result = _mod.monitor_routine(ctx, [], opts, _stop_event=stop)
        assert calls == 1  # reached refresh despite get_session raising
        assert result is None

    def test_is_live_exception_does_not_kill_monitor(self):
        """An is_live network error on the periodic liveness check is swallowed;
        the loop keeps polling rather than crashing or ending the race."""
        ctx = _mod.RaceContext('123', '42', MagicMock(), None, 0)
        opts = _mod.RaceOptions(network_mode=False, interval=30)

        n = _mod._LIVE_CHECK_INTERVAL
        poll_count = 0

        def fake_wait(timeout):
            nonlocal poll_count
            poll_count += 1
            return poll_count > n  # stop after one full liveness interval

        stop = MagicMock()
        stop.wait.side_effect = fake_wait
        ctx.client.live.get_session.return_value = {'Successful': False}
        ctx.client.race.is_live.side_effect = ConnectionError("wifi blip")

        with patch.object(_mod, 'refresh_competitor', return_value=[]):
            with patch.object(_mod.threading, 'Event', return_value=stop):
                result = _mod.monitor_routine(ctx, [], opts)
        assert ctx.client.race.is_live.called  # the check fired and raised
        assert result is None  # loop exited via stop, not a crash or RACE_ENDED


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

    @pytest.mark.parametrize("name", ['A', 'Super Street', 'GT3,Pro=Am'])
    def test_class_name_preserved(self, name):
        sd = self._session('42', '1')
        sd['Session']['Categories']['1']['Name'] = name
        class_name, _ = _mod._resolve_class_historical('42', sd)
        assert class_name == name

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

    @pytest.mark.parametrize("name", ['A', 'GT3,Pro=Am'])
    def test_class_name_preserved(self, name):
        session = self._make_session('42', 'classA', 3)
        session['Session']['Classes']['classA']['Description'] = name
        class_name, _ = _mod._resolve_class_live(session, '42')
        assert class_name == name

    def test_non_integer_tracked_position_returns_class_name_and_none(self):
        # tracked car's Position is unparseable (e.g. DNF) — class name still
        # resolves but the class position cannot be computed.
        session = self._make_session('42', 'classA', 3)
        session['Session']['Competitors']['r1']['Position'] = 'DNF'
        class_name, class_pos = _mod._resolve_class_live(session, '42')
        assert class_name == 'A'
        assert class_pos is None

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

    def test_returns_none_for_streaming_command_and_logs_command_name(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            result = _mod._time_to_ms('$F')
        assert result is None
        assert 'Heartbeat' in caplog.text

    def test_returns_none_for_garbage_and_logs_unparseable(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            result = _mod._time_to_ms('not-a-time')
        assert result is None
        assert 'unparseable' in caplog.text

    def test_empty_string_returns_none_without_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            assert _mod._time_to_ms('') is None
        assert caplog.text == ''

    def test_none_returns_none_without_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            assert _mod._time_to_ms(None) is None
        assert caplog.text == ''

    def test_short_fraction_is_padded_not_misread(self):
        assert _mod._time_to_ms('1:23.4') == 1 * 60000 + 23 * 1000 + 400

    def test_long_fraction_is_truncated_to_ms(self):
        assert _mod._time_to_ms('23.4567') == 23 * 1000 + 456


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

    @pytest.mark.parametrize("kwargs,substr,present", [
        # class tag: included when class_name given, omitted otherwise
        ({'class_name': 'A', 'class_positions': {1: 2}}, 'class=A', True),
        ({}, 'class=', False),
        # class_position field: included when class_positions maps the lap
        ({'class_name': 'A', 'class_positions': {1: 2}}, 'class_position=2i', True),
        ({}, 'class_position', False),
        # competitor_name field
        ({'competitor_name': 'Jane Doe'}, 'competitor_name="Jane Doe"', True),
        ({}, 'competitor_name', False),
        # car_info field
        ({'car_info': '2005/Toy/Celica'}, 'car_info="2005/Toy/Celica"', True),
        ({}, 'car_info', False),
    ])
    def test_optional_field_inclusion(self, kwargs, substr, present):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, **kwargs)
        assert (substr in self._record(write_api)) is present

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

    def test_unparseable_total_time_skips_lap_entirely(self):
        """A lap with unparseable TotalTime must not be written at all — anchoring
        it at start_epoc_ms + 0 would silently collide/dedupe with any other lap
        legitimately timestamped at session start."""
        ctx, write_api = self._ctx()  # start_epoc=0
        laps = [{'Lap': '1', 'LapTime': '1:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '3$H'}]
        _mod.push_influx(ctx, laps, False)
        write_api.write.assert_not_called()

    def test_explicit_car_number_overrides_ctx(self):
        ctx, write_api = self._ctx()  # ctx.car_number = '42'
        _mod.push_influx(ctx, self._laps(), False, car_number='99')
        record = self._record(write_api)
        assert 'car_number=99' in record
        assert 'car_number=42' not in record

    def test_passes_session_id_to_build_lap_points(self):
        ctx, write_api = self._ctx()
        _mod.push_influx(ctx, self._laps(), False, session_id=77)
        assert 'session_id=77' in self._record(write_api)


class TestOldRaceSkip:
    def _session_details(self):
        return _single_car_session_details()

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
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(1, 1)):
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
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(1, 1)):
                with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                    with patch.object(_mod, 'push_influx'):
                        with patch.object(_mod, 'push_influx_race'):
                            with patch.object(_mod, 'print_rankings'):
                                with caplog.at_level(logging.INFO):
                                    _mod.old_race(ctx, opts)
        assert any('SKIP' in r.message and '999' in r.message for r in caplog.records)

    def test_writes_when_laps_complete_but_standings_stale(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 1)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(1, 0)):
                with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                    with patch.object(_mod, 'push_influx_race'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        assert ctx.write_api.write.called

    def test_writes_when_laps_complete_but_standings_missing(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 1)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(0, 0)):
                with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                    with patch.object(_mod, 'push_influx_race'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        assert ctx.write_api.write.called

    def test_does_not_query_standings_when_laps_incomplete(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 0)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide') as mock_std:
                with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
                    with patch.object(_mod, 'push_influx_race'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        mock_std.assert_not_called()

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
        # nargs='?' makes the positional car_number optional (defaults to None);
        # this is custom parser wiring main() depends on, not a stdlib default.
        args = _mod._build_parser().parse_args(['12345'])
        assert args.car_number is None


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


class TestExistingStandingsCountsFieldwide:
    def _query_api(self, total_count=0, current_count=0):
        responses = iter([total_count, current_count])

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
        ctx.query_api = self._query_api(total_count=12, current_count=5)
        total, current = _mod.existing_standings_counts_fieldwide(ctx)
        assert total == 12
        assert current == 5

    def test_total_query_filters_standings_position_field(self):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = self._query_api(total_count=1, current_count=1)
        _mod.existing_standings_counts_fieldwide(ctx)
        total_flux = ctx.query_api.query.call_args_list[0].args[0]
        assert '_measurement == "standings"' in total_flux
        assert '_field == "position"' in total_flux
        assert 'race_id == "999"' in total_flux

    def test_current_query_filters_on_schema_version_value(self):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = self._query_api(total_count=1, current_count=1)
        _mod.existing_standings_counts_fieldwide(ctx)
        current_flux = ctx.query_api.query.call_args_list[1].args[0]
        assert '_measurement == "standings"' in current_flux
        assert '_field == "schema_version"' in current_flux
        assert f'_value == {_mod.SCHEMA_VERSION}' in current_flux


class TestOldRaceFieldwide:
    def _session_details(self, car_number='42'):
        return _single_car_session_details(car_number=car_number)

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

    def test_save_file_writes_csv_with_name_and_race_id(self):
        # opts.save_file routes to write_csv with a "<competitor name>-<race_id>"
        # filename built from the live racer detail.
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=False, save_file=True)
        with patch.object(_mod, 'print_rankings'):
            with patch.object(_mod, 'write_csv') as mock_csv:
                _mod.live_race(ctx, opts)
        mock_csv.assert_called_once()
        assert mock_csv.call_args.args[0] == 'Jane Doe-999'


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


class TestLiveRaceMetadataFailure:
    def _ctx(self):
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 1000)
        ctx.metadata = _mod.RaceMetadata('Race', 'Track', None, 9999)
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
        return ctx

    def test_non_monitor_returns_write_failed_when_metadata_write_fails(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, monitor_mode=False, interval=30)
        with patch.object(_mod, 'push_influx_race', return_value=False):
            with patch.object(_mod, 'push_influx_session'):
                with patch.object(_mod, 'push_influx'):
                    with patch.object(_mod, 'push_influx_standings_live'):
                        result = _mod.live_race(ctx, opts)
        assert result is _mod.MonitorStatus.WRITE_FAILED

    def test_non_monitor_returns_none_when_metadata_write_succeeds(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, monitor_mode=False, interval=30)
        with patch.object(_mod, 'push_influx_race', return_value=True):
            with patch.object(_mod, 'push_influx_session'):
                with patch.object(_mod, 'push_influx'):
                    with patch.object(_mod, 'push_influx_standings_live'):
                        result = _mod.live_race(ctx, opts)
        assert result is None

    def test_monitor_retries_metadata_write_until_success(self):
        """A failed metadata write must be retried on later polls, not abort
        the loop or be dropped after one attempt."""
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 1000)
        ctx.metadata = _mod.RaceMetadata('Race', 'Track', None, 9999)
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        call_count = 0
        def fake_get_session(race_id):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                stop.set()
            return {'Successful': True, 'Session': {
                'ID': 'sess-1', 'Name': 'S', 'Competitors': {}, 'Classes': {}}}

        ctx.client.live.get_session.side_effect = fake_get_session

        # First write fails, second succeeds; a third poll must not push again.
        with patch.object(_mod, 'push_influx_race', side_effect=[False, True]) as mock_race:
            with patch.object(_mod, 'refresh_competitor', return_value=[]):
                with patch.object(_mod, 'push_influx_standings_live', return_value={}):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop,
                                         race_meta_written=False)

        assert mock_race.call_count == 2

    def test_monitor_retries_metadata_write_with_wallclock_when_epoch_unknown(self):
        """A failed initial write must still be retried when RaceMonitor never
        posts a start epoch; otherwise the metadata point stays missing while
        the monitor ends RACE_ENDED and the run exits 0."""
        stop = threading.Event()
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 0)
        ctx.metadata = _mod.RaceMetadata('Race', 'Track', None, 9999)
        ctx.client.race.details.return_value = {'Successful': False}
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        def fake_get_session(race_id):
            stop.set()
            return {'Successful': False}

        ctx.client.live.get_session.side_effect = fake_get_session

        with patch.object(_mod, 'push_influx_race', return_value=True) as mock_race:
            with patch.object(_mod, 'refresh_competitor', return_value=[]):
                with patch.object(_mod, 'push_influx_standings_live', return_value={}):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop,
                                         race_meta_written=False)

        assert mock_race.call_count == 1
        assert mock_race.call_args[0][1] > 1_000_000_000_000  # wall-clock ms, not 0


class TestOldRaceClassWiring:
    def _session_details(self, car_number='42', cat_id='1', cat_name='A'):
        return _single_car_session_details(
            car_number=car_number, category=cat_id, category_name=cat_name)

    @staticmethod
    def _capture_points():
        """Return (captured_list, patch_cm) so _build_lap_points runs for real and
        the emitted lap Point objects are captured from _write_points_chunked
        (standings points written in the same pass are filtered out)."""
        captured = []

        def _cap(_api, points):
            captured.extend(p for p in points if p.to_line_protocol().startswith('lap,'))

        cm = patch.object(_mod, '_write_points_chunked', side_effect=_cap)
        return captured, cm

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
        expected_index = _mod._build_class_index(self._session_details())
        mock_resolve.assert_any_call('42', self._session_details(), expected_index)

    def test_passes_class_name_to_build_lap_points(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert 'class=A' in captured[0].to_line_protocol()

    def test_passes_session_start_epoc_to_build_lap_points(self):
        # ctx.start_epoc=9999 is intentionally different from SessionStartDateEpoc=5555;
        # the emitted timestamp must anchor on the session epoch (5555*1000 + 90000ms).
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 9999)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SessionStartDateEpoc'] = 5555
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert captured[0].to_line_protocol().endswith(str(5555 * 1000 + 90000))

    def test_handles_missing_session_start_epoc_key(self):
        # Missing SessionStartDateEpoc -> None passed through -> _build_lap_points
        # falls back to ctx.start_epoc (7000 here), anchoring at 7000*1000 + 90000ms.
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 7000)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        del session['Session']['SessionStartDateEpoc']
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert captured[0].to_line_protocol().endswith(str(7000 * 1000 + 90000))

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
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert 'competitor_name="Jane Doe"' in captured[0].to_line_protocol()

    def test_passes_car_info_to_build_lap_points(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SortedCompetitors'][0]['AdditionalData'] = '2005/Toy/Celica'
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert 'car_info="2005/Toy/Celica"' in captured[0].to_line_protocol()

    def test_competitor_name_none_when_both_name_fields_empty(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        opts = _mod.RaceOptions(network_mode=True)
        session = self._session_details()
        session['Session']['SortedCompetitors'][0]['FirstName'] = ''
        session['Session']['SortedCompetitors'][0]['LastName'] = ''
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'print_rankings'):
                        _mod.old_race(ctx, opts)
        assert 'competitor_name' not in captured[0].to_line_protocol()

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

    def test_deletes_existing_standings_before_historical_write(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        order = []

        def _del(c):
            order.append('delete_standings')
            return True

        def _write(*a, **k):
            order.append('write_standings')
            return True

        with patch.object(_mod, 'delete_existing_laps'):
            with patch.object(_mod, 'delete_existing_standings', side_effect=_del):
                with patch.object(_mod, 'push_influx_standings_historical', side_effect=_write):
                    with patch.object(_mod, '_write_points_chunked'):
                        with patch.object(_mod, 'push_influx_race'):
                            with patch.object(_mod, 'print_rankings'):
                                with patch.object(_mod, '_resolve_class_historical',
                                                  return_value=('A', {1: 1})):
                                    _mod.old_race(ctx, opts)
        assert order == ['delete_standings', 'write_standings']

    def test_clears_partial_standings_when_write_fails(self, caplog):
        # A partial standings write must be wiped so the next run self-heals,
        # rather than left looking "complete" to the skip checks.
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, 'delete_existing_laps'):
            with patch.object(_mod, 'delete_existing_standings',
                              return_value=True) as mock_del_std:
                with patch.object(_mod, 'push_influx_standings_historical', return_value=False):
                    with patch.object(_mod, '_write_points_chunked'):
                        with patch.object(_mod, 'push_influx_race'):
                            with patch.object(_mod, 'print_rankings'):
                                with patch.object(_mod, '_resolve_class_historical',
                                                  return_value=('A', {1: 1})):
                                    _mod.old_race(ctx, opts)
        # initial delete + cleanup delete on failure
        assert mock_del_std.call_count == 2
        assert any(r.levelno == logging.ERROR and 'cleared partial standings' in r.message
                   and '999' in r.message for r in caplog.records)

    def test_cleanup_delete_failure_directs_to_force(self, caplog):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        opts = _mod.RaceOptions(network_mode=True)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        with patch.object(_mod, 'delete_existing_laps'):
            # initial delete succeeds, cleanup delete fails
            with patch.object(_mod, 'delete_existing_standings', side_effect=[True, False]):
                with patch.object(_mod, 'push_influx_standings_historical', return_value=False):
                    with patch.object(_mod, '_write_points_chunked'):
                        with patch.object(_mod, 'push_influx_race'):
                            with patch.object(_mod, 'print_rankings'):
                                with patch.object(_mod, '_resolve_class_historical',
                                                  return_value=('A', {1: 1})):
                                    _mod.old_race(ctx, opts)
        assert any(r.levelno == logging.ERROR and '--force' in r.message
                   and '999' in r.message for r in caplog.records)

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

    @pytest.mark.parametrize("expected", [
        'race_id=999',
        'track_name=Road\\ America',
        'end_time_epoc=1749132000i',
    ])
    def test_line_protocol_contains(self, expected):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_race(ctx, 5000000)
        assert expected in self._record(write_api)

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

    def test_metadata_none_returns_false_and_skips_delete_and_write(self):
        ctx, write_api, delete_api = self._ctx()
        ctx.metadata = None
        assert _mod.push_influx_race(ctx, 1000) is False
        delete_api.delete.assert_not_called()
        write_api.write.assert_not_called()

    def test_returns_true_on_success(self):
        ctx, _write_api, _delete_api = self._ctx()
        assert _mod.push_influx_race(ctx, 5000000) is True

    def test_returns_false_when_delete_fails(self):
        ctx, _write_api, delete_api = self._ctx()
        delete_api.delete.side_effect = Exception("network error")
        assert _mod.push_influx_race(ctx, 5000000) is False

    def test_returns_false_when_write_fails(self):
        """Delete succeeded, write failed: the metadata point is now gone from
        Influx — the caller must know so it can fail the run and retry."""
        ctx, write_api, _delete_api = self._ctx()
        write_api.write.side_effect = Exception("network error")
        assert _mod.push_influx_race(ctx, 5000000) is False


class TestPushInfluxRaceFields:
    def _ctx(self):
        meta = _mod.RaceMetadata(
            race_name='R', track_name='T', series_name='S', end_time_epoc=123)
        return _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0,
                                metadata=meta, delete_api=MagicMock())

    def test_writes_expected_session_and_schema_fields(self):
        ctx = self._ctx()
        ok = _mod.push_influx_race(ctx, 1000, expected_lap_count=42, session_count=3)
        assert ok is True
        point = ctx.write_api.write.call_args.kwargs['record']
        lp = point.to_line_protocol()
        assert 'expected_lap_count=42i' in lp
        assert 'session_count=3i' in lp
        assert f'schema_version={_mod.SCHEMA_VERSION}i' in lp

    def test_omits_new_fields_when_expected_not_supplied(self):
        ctx = self._ctx()
        ok = _mod.push_influx_race(ctx, 1000)
        assert ok is True
        lp = ctx.write_api.write.call_args.kwargs['record'].to_line_protocol()
        assert 'end_time_epoc=' in lp
        assert 'expected_lap_count' not in lp
        assert 'session_count' not in lp
        assert 'schema_version' not in lp


class TestOldRaceMetadataFailure:
    def _session_details(self):
        return _single_car_session_details(start_epoc=1000)

    def _ctx(self):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.query_api = MagicMock()
        ctx.delete_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details()
        return ctx

    def test_skip_path_fails_run_when_metadata_write_fails(self):
        """The skip path deletes-then-rewrites metadata too; a silent failure
        there erases the race from the races bucket while reporting success."""
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 1)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide',
                              return_value=(1, 1)):
                with patch.object(_mod, 'push_influx_race', return_value=False):
                    result = _mod.old_race(ctx, opts)
        assert result == 1

    def test_write_path_fails_run_when_metadata_write_fails(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx_race', return_value=False):
            with patch.object(_mod, 'print_rankings'):
                result = _mod.old_race(ctx, opts)
        assert result == 1

    def test_lap_write_failure_fails_run(self):
        ctx = self._ctx()
        ctx.write_api.write.side_effect = Exception("boom")
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx_race', return_value=True):
            result = _mod.old_race(ctx, opts)
        assert result == 1

    def test_run_race_propagates_old_race_failure(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'old_race', return_value=1):
            result = _mod._run_race(ctx, opts, {'Successful': True, 'IsLive': False})
        assert result == 1

    def test_standings_write_failure_fails_run(self):
        """Standings were wiped after a partial write and need a rewrite — the
        run must report failure so race-backfill retries, matching the
        metadata-write path above."""
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx_race', return_value=True):
            with patch.object(_mod, 'push_influx_standings_historical',
                              return_value=False):
                with patch.object(_mod, 'delete_existing_standings', return_value=True):
                    with patch.object(_mod, 'print_rankings'):
                        result = _mod.old_race(ctx, opts)
        assert result == 1

    def test_standings_cleanup_delete_failure_also_fails_run(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx_race', return_value=True):
            with patch.object(_mod, 'push_influx_standings_historical',
                              return_value=False):
                with patch.object(_mod, 'delete_existing_standings', return_value=False):
                    with patch.object(_mod, 'print_rankings'):
                        result = _mod.old_race(ctx, opts)
        assert result == 1


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

    @pytest.mark.parametrize("expected", [
        'race_id=999',
        'session_id=42',
        'session_name="Day 1"',
        'start_epoc=1700000000i',
    ])
    def test_line_protocol_contains(self, expected):
        ctx, write_api, _delete_api = self._ctx()
        _mod.push_influx_session(ctx, 42, 'Day 1', 1700000000)
        assert expected in self._record(write_api)

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


class TestDeleteExistingStandings:
    def test_deletes_standings_measurement_for_race(self):
        ctx = _mod.RaceContext('777', None, MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        _mod.delete_existing_standings(ctx)
        ctx.delete_api.delete.assert_called_once()
        kwargs = ctx.delete_api.delete.call_args.kwargs
        assert kwargs['predicate'] == '_measurement="standings" AND race_id="777"'
        assert kwargs['bucket'] == 'laps'

    def test_exception_is_logged_not_raised(self, caplog):
        ctx = _mod.RaceContext('777', None, MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        ctx.delete_api.delete.side_effect = Exception("influx down")
        _mod.delete_existing_standings(ctx)  # must not raise
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    def test_returns_true_on_success(self):
        ctx = _mod.RaceContext('777', None, MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        assert _mod.delete_existing_standings(ctx) is True

    def test_returns_false_on_failure(self):
        ctx = _mod.RaceContext('777', None, MagicMock(), MagicMock(), 0)
        ctx.delete_api = MagicMock()
        ctx.delete_api.delete.side_effect = Exception("influx down")
        assert _mod.delete_existing_standings(ctx) is False


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

    @staticmethod
    def _capture_points():
        captured = []

        def _cap(_api, points):
            captured.extend(p for p in points if p.to_line_protocol().startswith('lap,'))

        cm = patch.object(_mod, '_write_points_chunked', side_effect=_cap)
        return captured, cm

    @staticmethod
    def _car_number(point):
        return point.to_line_protocol().split('car_number=', 1)[1].split(',', 1)[0]

    def test_writes_all_competitors_not_just_tracked(self):
        ctx = self._ctx(self._session_details_two_cars())
        opts = _mod.RaceOptions(network_mode=True)
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        assert len(captured) == 2
        assert {self._car_number(p) for p in captured} == {'42', '99'}

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
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        # every emitted point carries an explicit per-competitor car_number tag
        for point in captured:
            assert 'car_number=' in point.to_line_protocol()

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
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        built_car_numbers = {self._car_number(p) for p in captured}
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
        captured, cm = self._capture_points()
        with patch.object(_mod, '_resolve_class_historical', return_value=('A', {1: 1})):
            with cm:
                with patch.object(_mod, 'push_influx_race'):
                    with patch.object(_mod, 'delete_existing_laps'):
                        with patch.object(_mod, 'print_rankings'):
                            _mod.old_race(ctx, opts)
        # session_id tag comes from the session details ID=1
        for point in captured:
            assert 'session_id=1' in point.to_line_protocol()


class TestBuildLapPointsSessionId:
    def _laps(self):
        return [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}]

    def _build(self, session_id):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        points = _mod._build_lap_points(
            ctx, self._laps(), 'Jane Doe', None, 'A', None, 0, '42', session_id)
        return points[0].to_line_protocol()

    @pytest.mark.parametrize("session_id,substr,present", [
        (7, 'session_id=7', True),       # integer
        ('99', 'session_id=99', True),   # string
        (None, 'session_id', False),     # omitted
    ])
    def test_session_id_tag(self, session_id, substr, present):
        assert (substr in self._build(session_id)) is present


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
    """Behavioral tests exercising the real pandas table-building path."""

    def _competitor(self, position='1', number='42', name='Jane', category='1', laps='5'):
        return {
            'Position': position, 'Number': number, 'FirstName': name, 'LastName': '',
            'Laps': laps, 'Category': category, 'Transponder': 'T1',
        }

    def _categories(self):
        return {'1': {'ID': '1', 'Name': 'Gold'}}

    def test_numeric_position_sorts_before_non_numeric(self, capsys):
        competitors = [
            self._competitor(position='6$G', number='99'),
            self._competitor(position='1', number='42'),
        ]
        _mod.print_rankings(competitors, False, None, self._categories())
        out = capsys.readouterr().out
        assert out.index('42') < out.index('99')


class TestPrintRankingsNames:
    """print_rankings collects and returns the non-empty competitor names.

    Regression: the name loop guarded on `item != ''` where `item` is the dict
    KEY (always 'FirstName'/'LastName', never ''), so empty name VALUES were
    collected anyway. The guard must test the value, so blank names are dropped.
    """

    def _categories(self):
        return {'1': {'ID': '1', 'Name': 'Gold'}}

    def _competitor(self, first, last):
        return {'Position': '1', 'Number': '42', 'FirstName': first, 'LastName': last,
                'Laps': '5', 'Category': '1', 'Transponder': 'T1'}

    def test_empty_name_values_excluded_from_returned_names(self):
        competitors = [
            self._competitor('Jane', 'Doe'),   # both present
            self._competitor('', 'Solo'),      # blank first name -> only 'Solo'
            self._competitor('Mono', ''),      # blank last name -> only 'Mono'
        ]
        names = _mod.print_rankings(competitors, False, None, self._categories())
        assert names == ['Jane', 'Doe', 'Solo', 'Mono']
        assert '' not in names


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

    def test_dry_run_refuses_live_race(self, caplog):
        """--dry-run is historical-only; entering the live path with write_api=None
        produces swallowed AttributeErrors instead of writes."""
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, dry_run=True)
        with patch.object(_mod, 'live_race') as mock_live:
            with caplog.at_level(logging.ERROR):
                result = _mod._run_race(ctx, opts, {'Successful': True, 'IsLive': True})
        assert result == 1
        mock_live.assert_not_called()
        assert any('dry-run' in r.message.lower() for r in caplog.records)


class TestRunRaceDispatch:
    def _ctx(self):
        return _mod.RaceContext('999', '42', MagicMock(), None, 0)

    def test_monitor_mode_returns_0_when_race_not_live(self):
        # -m against a completed race disables monitoring and exits cleanly
        # without falling through to the historical old_race backfill.
        ctx = self._ctx()
        opts = _mod.RaceOptions(monitor_mode=True)
        with patch.object(_mod, 'old_race') as mock_old:
            result = _mod._run_race(ctx, opts, {'Successful': True, 'IsLive': False})
        assert result == 0
        mock_old.assert_not_called()

    def test_returns_1_on_write_failed(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions()
        with patch.object(_mod, 'live_race',
                          return_value=_mod.MonitorStatus.WRITE_FAILED):
            result = _mod._run_race(ctx, opts, {'Successful': True, 'IsLive': True})
        assert result == 1


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

    def test_car_info_is_field_not_tag(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        comp = self._comp()
        comp['AdditionalData'] = '2009/Saab/9-3'
        resp = self._resp([comp])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'car_info="2009/Saab/9-3"' in lp
        assert 'competitor_name="Ben K"' in lp

    def test_differing_car_info_yields_same_series_key(self):
        # The production bug: two values for the same car must NOT split the series.
        # Tag set (everything before the first space, minus measurement) must match.
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        full = self._comp()
        full['AdditionalData'] = '2009/Saab/9-3'
        trunc = self._comp(laps='6')
        trunc['AdditionalData'] = '2009/Sa'
        with patch.object(_mod, '_write_points_chunked') as w1:
            prev = _mod.push_influx_standings_live(ctx, self._resp([full]), 'sess-1')
        with patch.object(_mod, '_write_points_chunked') as w2:
            _mod.push_influx_standings_live(ctx, self._resp([trunc]), 'sess-1', prev)
        key1 = w1.call_args[0][1][0].to_line_protocol().split(' ', 1)[0]
        key2 = w2.call_args[0][1][0].to_line_protocol().split(' ', 1)[0]
        assert key1 == key2
        assert 'car_info' not in key1

    def test_schema_version_field_present(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        resp = self._resp([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_live(ctx, resp, 'sess-1')
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert f'schema_version={_mod.SCHEMA_VERSION}i' in lp

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

    def test_returns_true_on_success(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp()])
        with patch.object(_mod, '_write_points_chunked'):
            assert _mod.push_influx_standings_historical(ctx, entry) is True

    def test_returns_false_on_write_failure(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp()])
        with patch.object(_mod, '_write_points_chunked', side_effect=Exception("influx down")):
            assert _mod.push_influx_standings_historical(ctx, entry) is False

    def test_measurement_is_standings(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert lp.startswith('standings,')

    def test_class_position_ranks_by_final_position_within_class(self):
        # Regression: final class position must rank same-class cars by their final
        # overall position. Previously it read each car's last-lap value from the
        # per-lap class_positions dict — computed at each car's own (differing) last
        # lap — so cars finishing at different lap counts collided on the same class
        # position. Here all three cars carry a colliding per-lap dict ({1: 9}) but
        # distinct final positions, so they must come out 1/2/3.
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([
            self._comp(car_number='974', position='33', class_name='B',
                       class_positions={1: 9}),
            self._comp(car_number='60', position='46', class_name='B',
                       class_positions={1: 9}),
            self._comp(car_number='151', position='81', class_name='B',
                       class_positions={1: 9}),
        ])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        by_car = {
            p.to_line_protocol().split('car_number=', 1)[1].split(',', 1)[0]:
                p.to_line_protocol()
            for p in mock_write.call_args[0][1]
        }
        assert 'class_position=1i' in by_car['974']
        assert 'class_position=2i' in by_car['60']
        assert 'class_position=3i' in by_car['151']

    def test_class_positions_independent_across_classes(self):
        # Each class is ranked independently, both starting at 1.
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([
            self._comp(car_number='1', position='2', class_name='A'),
            self._comp(car_number='2', position='5', class_name='B'),
        ])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        by_car = {
            p.to_line_protocol().split('car_number=', 1)[1].split(',', 1)[0]:
                p.to_line_protocol()
            for p in mock_write.call_args[0][1]
        }
        assert 'class_position=1i' in by_car['1']
        assert 'class_position=1i' in by_car['2']

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

    def test_empty_per_lap_class_positions_still_ranks_by_final_position(self):
        # The standings class position is derived from final overall position, so an
        # empty per-lap class_positions dict no longer suppresses it.
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp(class_positions={})])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'class_position=1i' in lp

    def test_car_info_is_field_not_tag(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        comp = self._comp()
        comp['car_info'] = '2009/Saab/9-3'
        entry = self._entry([comp])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert 'car_info="2009/Saab/9-3"' in lp
        assert 'competitor_name="Ben K"' in lp
        assert 'car_info=' not in lp.split(' ', 1)[0]

    def test_schema_version_field_present(self):
        ctx = _mod.RaceContext('123', None, MagicMock(), MagicMock(), 0)
        entry = self._entry([self._comp()])
        with patch.object(_mod, '_write_points_chunked') as mock_write:
            _mod.push_influx_standings_historical(ctx, entry)
        lp = mock_write.call_args[0][1][0].to_line_protocol()
        assert f'schema_version={_mod.SCHEMA_VERSION}i' in lp


class TestBuildLapPointsCorruptedLapNumber:
    """Corrupted Lap field (e.g. '$J') must not crash the write — the lap is skipped."""

    def _ctx(self):
        write_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 0)
        return ctx, write_api

    def _lap(self, lap_num='1'):
        return {'Lap': lap_num, 'LapTime': '0:01:30.000', 'Position': '3',
                'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}

    def test_skips_lap_when_lap_number_unparseable(self):
        ctx, write_api = self._ctx()
        laps = [self._lap('$J')]
        _mod.push_influx(ctx, laps, False)
        write_api.write.assert_not_called()

    def test_logs_warning_when_lap_number_unparseable(self, caplog):
        ctx, _ = self._ctx()
        with caplog.at_level(logging.WARNING):
            _mod.push_influx(ctx, [self._lap('$J')], False)
        assert any('$J' in r.message for r in caplog.records)

    def test_other_laps_still_written_when_one_has_bad_lap_number(self):
        ctx, write_api = self._ctx()
        laps = [self._lap('$J'), self._lap('2')]
        _mod.push_influx(ctx, laps, False)
        write_api.write.assert_called_once()
        record = write_api.write.call_args[1]['record']
        assert len(record) == 1
        assert 'lap_no=2i' in record[0].to_line_protocol()


class TestMonitorRoutineCorruptedLapNumber:
    """A new lap with a corrupted Lap field must not crash the monitor loop."""

    def _ctx(self):
        write_api = MagicMock()
        ctx = _mod.RaceContext('999', '42', MagicMock(), write_api, 0)
        ctx.client.live.get_session.return_value = {'Successful': False}
        return ctx

    def test_bad_lap_number_does_not_crash_monitor_in_network_mode(self):
        stop = threading.Event()
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        corrupted_lap = {'Lap': '$J', 'LapTime': '0:01:30.000', 'Position': '3',
                         'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}

        call_count = 0
        def fake_refresh(c):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return [corrupted_lap]

        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
                with patch.object(_mod, 'push_influx') as mock_push:
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop)
        mock_push.assert_not_called()

    def test_bad_lap_number_logs_warning_in_monitor(self, caplog):
        stop = threading.Event()
        ctx = self._ctx()
        opts = _mod.RaceOptions(network_mode=True, interval=0)

        corrupted_lap = {'Lap': '$J', 'LapTime': '0:01:30.000', 'Position': '3',
                         'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}

        call_count = 0
        def fake_refresh(c):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                stop.set()
            return [corrupted_lap]

        with patch.object(_mod, 'refresh_competitor', side_effect=fake_refresh):
            with patch.object(_mod, '_resolve_class_live', return_value=('A', 1)):
                with caplog.at_level(logging.WARNING):
                    _mod.monitor_routine(ctx, [], opts, _stop_event=stop)
        assert any('$J' in r.message for r in caplog.records)


class TestDescribeBadValue:
    def test_known_streaming_command_includes_token_and_name(self):
        result = _mod._describe_bad_value('$J', 'Lap')
        assert '$J' in result
        assert 'Passing Information' in result
        assert 'known API quirk' in result
        assert 'Lap' in result

    def test_unknown_garbage_says_unparseable(self):
        result = _mod._describe_bad_value('????', 'Lap')
        assert 'unparseable' in result

    def test_includes_field_name_in_garbage_message(self):
        result = _mod._describe_bad_value('????', 'LapTime')
        assert 'LapTime' in result

    def test_non_string_says_unparseable(self):
        result = _mod._describe_bad_value(None, 'Lap')
        assert 'unparseable' in result


class TestBuildLapPointsStreamingToken:
    def test_skips_streaming_command_in_lap_field_and_logs_command_name(self, caplog):
        import logging
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 1000000)
        laps = [{'Lap': '$J', 'LapTime': '1:00.000', 'TotalTime': '1:00.000',
                 'FlagStatus': 'Green', 'Position': '1'}]
        with caplog.at_level(logging.WARNING):
            points = _mod._build_lap_points(ctx, laps, 'Driver', None, None, None, 1000000, '42')
        assert points == []
        assert 'Passing Information' in caplog.text

    def test_omits_position_for_streaming_command_and_logs_command_name(self, caplog):
        import logging
        ctx = _mod.RaceContext('123', '42', MagicMock(), MagicMock(), 1000000)
        laps = [{'Lap': '1', 'LapTime': '1:00.000', 'TotalTime': '1:00.000',
                 'FlagStatus': 'Green', 'Position': '$G'}]
        with caplog.at_level(logging.WARNING):
            points = _mod._build_lap_points(ctx, laps, 'Driver', None, None, None, 1000000, '42')
        assert len(points) == 1  # lap still written, position omitted
        assert 'Race Information' in caplog.text


class TestBuildLapPointsBadTotalTime:
    """A lap whose TotalTime can't be parsed would otherwise be silently anchored
    at session start (start_epoc_ms + 0); the live/monitor path must skip it like
    the historical backfill path already does (old_race pre-filters there)."""

    def test_lap_with_garbage_total_time_excluded_from_points(self):
        laps = [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '$F'}]
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 1000000)
        points = _mod._build_lap_points(ctx, laps, 'Driver', None, None, None, 1000000, '42')
        assert points == []

    def test_logs_warning_for_garbage_total_time(self, caplog):
        laps = [{'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                 'FlagStatus': 'Green', 'TotalTime': '$F'}]
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 1000000)
        with caplog.at_level(logging.WARNING):
            _mod._build_lap_points(ctx, laps, 'Driver', None, None, None, 1000000, '42')
        assert any('$F' in r.message for r in caplog.records)

    def test_other_laps_still_written_when_one_has_bad_total_time(self):
        good = {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                'FlagStatus': 'Green', 'TotalTime': '0:01:30.000'}
        bad = {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '1',
               'FlagStatus': 'Green', 'TotalTime': '$F'}
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 1000000)
        points = _mod._build_lap_points(ctx, [good, bad], 'Driver', None, None, None,
                                         1000000, '42')
        assert len(points) == 1
        assert 'lap_no=1i' in points[0].to_line_protocol()


class TestMainMissingToken:
    def test_logs_error_and_exits_when_no_token_set(self, caplog):
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(_mod.sys, 'argv', ['laps', '12345', '42']):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        assert any('RACEMONITOR_TOKENS' in r.message and 'RACEMONITOR_TOKEN' in r.message
                   for r in caplog.records)

    def test_no_token_error_honors_configured_pool_var(self, caplog, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
        env = {'RACEMONITOR_TOKEN': 'stale-legacy', 'LEMONGRASS_CONFIG': str(cfg)}
        with patch.dict(os.environ, env, clear=True):
            with patch.object(_mod.sys, 'argv', ['laps', '12345', '42']):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        assert any('MY_POOL' in r.message for r in caplog.records)


class TestMainInfluxTokenPreflight:
    _ARGV: ClassVar[list] = ['lemongrass-laps', '-n', '12345', '42']

    def test_logs_error_and_exits_when_influx_token_not_set(self, caplog):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN'}, clear=True):
            with patch.object(_mod.sys, 'argv', self._ARGV):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        assert any('INFLUX_TELEMETRY_TOKEN' in r.message for r in caplog.records)

    def test_honors_configured_token_env_var(self, caplog, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text('[influx]\ntoken_env = "MY_INFLUX_TOKEN"\n')
        env = {
            'RACEMONITOR_TOKENS': 'TOKEN',
            'LEMONGRASS_CONFIG': str(cfg),
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(_mod.sys, 'argv', self._ARGV):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        assert any('MY_INFLUX_TOKEN' in r.message for r in caplog.records)


class TestMainFluxIdValidation:
    """laps.main() interpolates race_id/car_number into Flux delete predicates and
    queries, so it must reject unsafe identifiers before touching RaceMonitor/Influx,
    same as race_diagnose.main() and race_backfill.main() already do."""

    def _fake_args(self, race_id, car_number=None):
        return SimpleNamespace(
            race_id=[race_id], car_number=car_number, verbose=False,
            network_mode=False, monitor_mode=False, save_file=False,
            selected_class=None, interval=30, skip_if_complete=False, dry_run=False,
        )

    def test_race_id_with_quote_exits_1_before_racemonitor_client(self):
        with patch.object(_mod, '_build_parser') as mock_parser_factory:
            mock_parser_factory.return_value.parse_args.return_value = (
                self._fake_args('x"y'))
            with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN'}, clear=True):
                with patch.object(_mod, 'RaceMonitorClient') as mock_client:
                    mock_client.side_effect = AssertionError(
                        'must not reach RaceMonitorClient with an invalid id')
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        mock_client.assert_not_called()

    def test_car_number_with_quote_exits_1_before_racemonitor_client(self):
        with patch.object(_mod, '_build_parser') as mock_parser_factory:
            mock_parser_factory.return_value.parse_args.return_value = (
                self._fake_args('12345', car_number='4"2'))
            with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN'}, clear=True):
                with patch.object(_mod, 'RaceMonitorClient') as mock_client:
                    mock_client.side_effect = AssertionError(
                        'must not reach RaceMonitorClient with an invalid id')
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        mock_client.assert_not_called()

    def test_logs_invalid_identifier_error(self, caplog):
        with patch.object(_mod, '_build_parser') as mock_parser_factory:
            mock_parser_factory.return_value.parse_args.return_value = (
                self._fake_args('x"y'))
            with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN'}, clear=True):
                with patch.object(_mod, 'RaceMonitorClient'):
                    with caplog.at_level(logging.ERROR):
                        with pytest.raises(SystemExit):
                            _mod.main()
        assert any('x"y' in r.message for r in caplog.records)


class TestLiveRaceNoData:
    def _ctx(self):
        ctx = _mod.RaceContext('123', '99', MagicMock(), None, 0)
        ctx.client.live.get_session.return_value = {'Successful': False}
        ctx.client.live.get_racer.return_value = {'Successful': False}
        return ctx

    def test_unsuccessful_get_racer_returns_no_live_data(self):
        """A car number missing from the live feed must fail cleanly, not KeyError."""
        opts = _mod.RaceOptions()
        with patch.object(_mod, 'print_rankings'):
            result = _mod.live_race(self._ctx(), opts)
        assert result is _mod.MonitorStatus.NO_LIVE_DATA

    def test_unsuccessful_get_racer_logs_error(self, caplog):
        opts = _mod.RaceOptions()
        with patch.object(_mod, 'print_rankings'):
            with caplog.at_level(logging.ERROR):
                _mod.live_race(self._ctx(), opts)
        assert any('99' in r.message for r in caplog.records)

    def test_run_race_returns_1_on_no_live_data(self):
        ctx = self._ctx()
        opts = _mod.RaceOptions()
        with patch.object(_mod, 'live_race',
                          return_value=_mod.MonitorStatus.NO_LIVE_DATA):
            result = _mod._run_race(ctx, opts, {'Successful': True, 'IsLive': True})
        assert result == 1


class TestOldRaceSessionFetching:
    def _session_details(self, sid=1):
        return _single_car_session_details(
            session_id=sid, session_name=f'S{sid}', start_epoc=1000)

    def test_zero_sessions_returns_cleanly(self, caplog):
        """A race with no posted sessions must log and return, not UnboundLocalError."""
        ctx = _mod.RaceContext('999', None, MagicMock(), None, 0)
        ctx.client.results.sessions_for_race.return_value = {'Sessions': []}
        opts = _mod.RaceOptions(network_mode=False)
        with caplog.at_level(logging.WARNING):
            _mod.old_race(ctx, opts)  # must not raise
        assert any('999' in r.message for r in caplog.records)

    def test_display_mode_fetches_only_final_session(self):
        """Non-network mode prints one rankings table; fetching every session
        burns the RaceMonitor rate limit for data that is discarded."""
        ctx = _mod.RaceContext('999', None, MagicMock(), None, 0)
        ctx.client.results.sessions_for_race.return_value = {
            'Sessions': [{'ID': 1}, {'ID': 2}, {'ID': 3}]}
        ctx.client.results.session_details.return_value = self._session_details(3)
        opts = _mod.RaceOptions(network_mode=False)
        with patch.object(_mod, 'print_rankings'):
            _mod.old_race(ctx, opts)
        ctx.client.results.session_details.assert_called_once_with(
            3, include_lap_times=True)

    def test_network_mode_still_fetches_all_sessions(self):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {
            'Sessions': [{'ID': 1}, {'ID': 2}]}
        ctx.client.results.session_details.side_effect = [
            self._session_details(1), self._session_details(2)]
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx_race', return_value=True):
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        assert ctx.client.results.session_details.call_count == 2


class TestOldRaceExpectedCountFiltering:
    """One garbage lap must not make expected != written forever, which defeats
    --skip-if-complete and re-deletes/rewrites the race on every backfill run."""

    def _session_details(self, lap_times):
        return {
            'Successful': True,
            'Session': {
                'ID': 1, 'Name': 'S1', 'SessionStartDateEpoc': 1000,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [{
                    'Number': '42', 'Category': '1', 'FirstName': 'Jane',
                    'LastName': 'Doe', 'Position': '1', 'Laps': '2',
                    'BestLapTime': '', 'LastLapTime': '', 'Transponder': '',
                    'AdditionalData': '', 'LapTimes': lap_times,
                }],
            },
        }

    def _ctx(self, lap_times):
        ctx = _mod.RaceContext('999', '42', MagicMock(), MagicMock(), 0)
        ctx.query_api = MagicMock()
        ctx.delete_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = self._session_details(lap_times)
        return ctx

    def test_garbage_lap_number_excluded_from_expected_count(self):
        """Stored: 1 lap (the good one). Expected must also be 1 → SKIP."""
        good = {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                'FlagStatus': 0, 'TotalTime': '0:01:30.000'}
        garbage = {'Lap': '$F', 'LapTime': '0:01:31.000', 'Position': '2',
                   'FlagStatus': 0, 'TotalTime': '0:03:01.000'}
        ctx = self._ctx([good, garbage])
        opts = _mod.RaceOptions(network_mode=True, skip_if_complete=True)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(1, 1)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide',
                              return_value=(1, 1)):
                with patch.object(_mod, 'push_influx_race', return_value=True):
                    with patch.object(_mod, 'delete_existing_laps') as mock_del:
                        _mod.old_race(ctx, opts)
        mock_del.assert_not_called()

    def test_garbage_total_time_excluded_from_write(self):
        """A lap whose TotalTime cannot anchor a timestamp would collide at the
        session start and be silently deduplicated by InfluxDB — exclude it."""
        good = {'Lap': '1', 'LapTime': '0:01:30.000', 'Position': '1',
                'FlagStatus': 0, 'TotalTime': '0:01:30.000'}
        bad_time = {'Lap': '2', 'LapTime': '0:01:31.000', 'Position': '1',
                    'FlagStatus': 0, 'TotalTime': '$F'}
        ctx = self._ctx([good, bad_time])
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, 'push_influx_race', return_value=True):
            with patch.object(_mod, 'print_rankings'):
                _mod.old_race(ctx, opts)
        lap_write = ctx.write_api.write.call_args_list[0]
        assert len(lap_write.kwargs['record']) == 1


class TestClassIndex:
    def _session_details(self):
        def comp(number, category, laps):
            return {'Number': number, 'Category': category,
                    'LapTimes': [{'Lap': str(ln), 'Position': str(p)}
                                 for ln, p in laps]}
        return {
            'Session': {
                'Categories': {'1': {'Name': 'A'}, '2': {'Name': 'B'}},
                'SortedCompetitors': [
                    comp('10', '1', [(1, 1), (2, 1)]),
                    comp('11', '1', [(1, 3), (2, 2)]),
                    comp('20', '2', [(1, 2), (2, 3)]),
                ],
            },
        }

    def test_index_matches_per_car_resolution(self):
        details = self._session_details()
        index = _mod._build_class_index(details)
        # Hard-coded expected class positions catch regressions the self-comparison
        # below would miss (the no-index path builds the same index internally).
        assert _mod._resolve_class_historical('10', details, index) == ('A', {1: 1, 2: 1})
        assert _mod._resolve_class_historical('11', details, index) == ('A', {1: 2, 2: 2})
        assert _mod._resolve_class_historical('20', details, index) == ('B', {1: 1, 2: 1})
        for number in ('10', '11', '20'):
            assert (_mod._resolve_class_historical(number, details)
                    == _mod._resolve_class_historical(number, details, index))

    def test_unknown_car_returns_none(self):
        details = self._session_details()
        index = _mod._build_class_index(details)
        assert _mod._resolve_class_historical('99', details, index) == (None, {})

    def test_malformed_laps_skipped_in_index(self):
        # A lap with an unparseable Lap number and one missing Position must be
        # skipped without aborting the rest of the car's index.
        details = {
            'Session': {
                'Categories': {'1': {'Name': 'A'}},
                'SortedCompetitors': [
                    {'Number': '10', 'Category': '1', 'LapTimes': [
                        {'Lap': '1', 'Position': '1'},
                        {'Lap': '$J', 'Position': '2'},  # unparseable Lap -> skipped
                        {'Lap': '3'},                    # missing Position -> skipped
                    ]},
                ],
            },
        }
        _, _, laps_by_car, _ = _mod._build_class_index(details)
        assert laps_by_car['10'] == {1: 1}

    def test_old_race_builds_index_once_per_session(self):
        session = {
            'Successful': True,
            'Session': {
                'ID': 1, 'Name': 'S1', 'SessionStartDateEpoc': 1000,
                'Categories': {'1': {'ID': '1', 'Name': 'A'}},
                'SortedCompetitors': [
                    {'Number': str(n), 'Category': '1', 'FirstName': 'F',
                     'LastName': f'L{n}', 'Position': str(n), 'Laps': '1',
                     'BestLapTime': '', 'LastLapTime': '', 'Transponder': '',
                     'AdditionalData': '',
                     'LapTimes': [{'Lap': '1', 'LapTime': '0:01:30.000',
                                   'Position': str(n), 'FlagStatus': 0,
                                   'TotalTime': '0:01:30.000'}]}
                    for n in (1, 2, 3)
                ],
            },
        }
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = MagicMock()
        ctx.delete_api = MagicMock()
        ctx.client.results.sessions_for_race.return_value = {'Sessions': [{'ID': 1}]}
        ctx.client.results.session_details.return_value = session
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_build_class_index',
                          wraps=_mod._build_class_index) as mock_index:
            with patch.object(_mod, 'push_influx_race', return_value=True):
                with patch.object(_mod, 'print_rankings'):
                    _mod.old_race(ctx, opts)
        assert mock_index.call_count == 1  # once per session, not per competitor


class TestStoredRaceCompleteness:
    def _ctx(self, record_values):
        """record_values: dict for the pivoted race row, or None for 'no race point'."""
        api = MagicMock()

        def fake_query(_flux):
            if record_values is None:
                return []
            table = MagicMock()
            rec = MagicMock()
            rec.values = record_values
            table.records = [rec]
            return [table]

        api.query.side_effect = fake_query
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = api
        return ctx

    def test_returns_none_when_no_race_point(self):
        ctx = self._ctx(None)
        assert _mod.stored_race_completeness(ctx) is None

    def test_reads_all_fields(self):
        ctx = self._ctx({
            'schema_version': _mod.SCHEMA_VERSION,
            'expected_lap_count': 42,
            'end_time_epoc': 123,
        })
        result = _mod.stored_race_completeness(ctx)
        assert result == _mod.StoredRace(
            schema_version=_mod.SCHEMA_VERSION, expected_lap_count=42, end_time_epoc=123)

    def test_missing_new_fields_come_back_none(self):
        ctx = self._ctx({'end_time_epoc': 123})
        result = _mod.stored_race_completeness(ctx)
        assert result.schema_version is None
        assert result.expected_lap_count is None
        assert result.end_time_epoc == 123


class TestStoredEndSettled:
    _NOW: ClassVar[int] = 1_000_000_000

    def _stored(self, end_epoc):
        return _mod.StoredRace(schema_version=4, expected_lap_count=1, end_time_epoc=end_epoc)

    @pytest.mark.parametrize("end_epoc", [
        0,                # unknown end
        None,             # missing end
        _NOW + 5000,      # future end
        _NOW - 3600,      # ended an hour ago, still well within the settle buffer
    ])
    def test_unknown_future_or_recent_end_is_not_settled(self, end_epoc):
        # None of these are settled well behind us; a later session under the same
        # race_id could still be live, so each must fall through to the is_live check.
        with patch.object(_mod.time, 'time', return_value=self._NOW):
            assert _mod.stored_end_settled(self._stored(end_epoc)) is False

    def test_end_exactly_at_buffer_is_not_settled(self):
        with patch.object(_mod.time, 'time', return_value=self._NOW):
            end = self._NOW - _mod._SETTLED_BUFFER_S
            assert _mod.stored_end_settled(self._stored(end)) is False

    def test_end_just_inside_buffer_is_not_settled(self):
        with patch.object(_mod.time, 'time', return_value=self._NOW):
            end = self._NOW - _mod._SETTLED_BUFFER_S + 1
            assert _mod.stored_end_settled(self._stored(end)) is False

    def test_end_beyond_buffer_is_settled(self):
        with patch.object(_mod.time, 'time', return_value=self._NOW):
            end = self._NOW - _mod._SETTLED_BUFFER_S - 1
            assert _mod.stored_end_settled(self._stored(end)) is True


class TestRaceCompleteInInflux:
    def _ctx(self):
        ctx = _mod.RaceContext('999', None, MagicMock(), MagicMock(), 0)
        ctx.query_api = MagicMock()
        return ctx

    def _stored(self, schema=None, expected=None):
        schema = _mod.SCHEMA_VERSION if schema is None else schema
        return _mod.StoredRace(
            schema_version=schema, expected_lap_count=expected, end_time_epoc=123)

    def test_none_stored_is_false(self):
        assert _mod.race_complete_in_influx(self._ctx(), None) is False

    def test_stale_schema_is_false(self):
        stored = self._stored(schema=_mod.SCHEMA_VERSION - 1, expected=10)
        assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_missing_expected_is_false(self):
        stored = self._stored(expected=None)
        assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_zero_expected_is_false(self):
        stored = self._stored(expected=0)
        assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_laps_below_expected_is_false(self):
        stored = self._stored(expected=10)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(9, 9)):
            assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_laps_stale_schema_is_false(self):
        stored = self._stored(expected=10)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(10, 9)):
            assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_standings_missing_is_false(self):
        stored = self._stored(expected=10)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(10, 10)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(0, 0)):
                assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_standings_present_but_stale_is_false(self):
        # Standings exist (std_total > 0) but some predate the current schema
        # (std_current < std_total) — a prior run whose standings phase went stale.
        stored = self._stored(expected=10)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(10, 10)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(5, 3)):
                assert _mod.race_complete_in_influx(self._ctx(), stored) is False

    def test_all_good_is_true(self):
        stored = self._stored(expected=10)
        with patch.object(_mod, 'existing_lap_counts_fieldwide', return_value=(10, 10)):
            with patch.object(_mod, 'existing_standings_counts_fieldwide', return_value=(5, 5)):
                assert _mod.race_complete_in_influx(self._ctx(), stored) is True


class TestInfluxOnlySkip:
    def _conn_ctx(self):
        """Patch _influx.connect to yield a client whose query_api() is a MagicMock."""
        cm = patch.object(_mod._influx, 'connect')
        mock_conn = cm.start()
        mock_conn.return_value.__enter__.return_value.query_api.return_value = MagicMock()
        return cm

    def test_true_when_complete_and_ended(self):
        cm = self._conn_ctx()
        try:
            complete = _mod.StoredRace(_mod.SCHEMA_VERSION, 10, 1000)
            with patch.object(_mod, 'stored_race_completeness', return_value=complete):
                with patch.object(_mod, 'stored_end_settled', return_value=True):
                    with patch.object(_mod, 'race_complete_in_influx', return_value=True):
                        assert _mod._influx_only_skip('999') is True
        finally:
            cm.stop()

    def test_false_when_no_stored_race(self):
        cm = self._conn_ctx()
        try:
            with patch.object(_mod, 'stored_race_completeness', return_value=None):
                assert _mod._influx_only_skip('999') is False
        finally:
            cm.stop()

    def test_false_when_not_ended(self):
        cm = self._conn_ctx()
        try:
            complete = _mod.StoredRace(_mod.SCHEMA_VERSION, 10, 0)
            with patch.object(_mod, 'stored_race_completeness', return_value=complete):
                with patch.object(_mod, 'stored_end_settled', return_value=False):
                    assert _mod._influx_only_skip('999') is False
        finally:
            cm.stop()


class TestMainFastPath:
    _ARGV: ClassVar[list] = ['lemongrass-laps', '-n', '--skip-if-complete', '999']
    _ENV: ClassVar[dict] = {'RACEMONITOR_TOKENS': 'T', 'INFLUX_TELEMETRY_TOKEN': 'I'}

    def test_skips_without_racemonitor_when_complete(self):
        with patch.object(_mod.sys, 'argv', self._ARGV):
            with patch.dict(os.environ, self._ENV, clear=True):
                with patch.object(_mod, '_influx_only_skip', return_value=True):
                    with patch.object(_mod, 'RaceMonitorClient') as mock_rm:
                        result = _mod.main()
        assert result == 0
        mock_rm.assert_not_called()

    def test_falls_through_to_racemonitor_when_not_complete(self):
        client = MagicMock()
        client.race.details.return_value = {'Successful': False}
        client.race.is_live.return_value = {'Successful': True, 'IsLive': False}
        with patch.object(_mod.sys, 'argv', self._ARGV):
            with patch.dict(os.environ, self._ENV, clear=True):
                with patch.object(_mod, '_influx_only_skip', return_value=False):
                    with patch.object(_mod, 'RaceMonitorClient') as mock_rm:
                        mock_rm.return_value.__enter__.return_value = client
                        with patch.object(_mod._influx, 'connect') as mock_connect:
                            mock_connect.return_value.__enter__.return_value = MagicMock()
                            with patch.object(_mod, '_run_race', return_value=0):
                                result = _mod.main()
        assert result == 0
        mock_rm.assert_called_once()


class TestBackfillRace:
    """The in-process backfill seam reused by race-backfill --upgrade-stored."""

    def _details(self):
        return {'Successful': True, 'Race': {
            'Name': 'Test', 'StartDateEpoc': 0, 'EndDateEpoc': 0, 'Track': 'T'}}

    def test_returns_1_for_live_race_without_car_number(self):
        """The historical batch path passes car_number=None; a race that is
        unexpectedly live must fail just that race (return 1), not sys.exit and
        abort an entire --upgrade-stored run."""
        client = MagicMock()
        client.race.details.return_value = self._details()
        client.race.is_live.return_value = {'Successful': True, 'IsLive': True}
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_race_metadata', return_value=None):
            assert _mod.backfill_race('101', None, client, opts) == 1

    def test_uses_injected_client_for_historical_backfill(self):
        """A completed race is dispatched through _run_race using the caller's
        client, and the function returns _run_race's status."""
        client = MagicMock()
        client.race.details.return_value = self._details()
        client.race.is_live.return_value = {'Successful': True, 'IsLive': False}
        opts = _mod.RaceOptions(network_mode=True)
        with patch.object(_mod, '_resolve_race_metadata', return_value=None), \
             patch.object(_mod._influx, 'connect') as mock_connect, \
             patch.object(_mod, '_run_race', return_value=0) as mock_run_race:
            mock_connect.return_value.__enter__.return_value = MagicMock()
            result = _mod.backfill_race('101', None, client, opts)
        assert result == 0
        ctx = mock_run_race.call_args.args[0]
        assert ctx.client is client
