from unittest.mock import MagicMock, patch

import pytest
from race_monitor import RaceMonitorError
from textual.widgets import DataTable, Input, Label, ListView, RichLog

from lemongrass._laps_tui import (
    CarSelectScreen,
    ImportConfirmScreen,
    ImportScreen,
    LapBoardModel,
    LapsApp,
    LapsFlowMixin,
    MonitorScreen,
    PickerScreen,
)


def _client_with_race(is_live=False):
    client = MagicMock()
    client.race.details.return_value = {
        'Successful': True, 'Race': {'Name': 'Sears Pointless', 'Track': 'Sears'}}
    client.race.is_live.return_value = {'Successful': True, 'IsLive': is_live}
    client.results.search_results.return_value = {'Races': [
        {'ID': 42, 'Name': 'Sears Pointless', 'StartDateEpoc': 0}]}
    return client


def _lap(no, lt='1:47.0', pos='3'):
    return {'Lap': no, 'LapTime': lt, 'TotalTime': '9:00.0', 'Position': pos}


class TestLapBoardModel:
    def test_set_and_add_laps_produce_rows(self):
        m = LapBoardModel()
        m.set_laps([_lap('1'), _lap('2')])
        m.add_lap(_lap('3'))
        rows = m.lap_rows()
        assert [r[0] for r in rows] == [1, 2, 3]

    def test_lap_rows_skip_non_integer_lap(self):
        m = LapBoardModel()
        m.set_laps([_lap('1'), _lap('CAUTION')])
        assert [r[0] for r in m.lap_rows()] == [1]

    def test_leaderboard_sorted_by_position(self):
        m = LapBoardModel()
        session = {'Successful': True, 'Session': {'Competitors': {
            'a': {'Number': '7', 'Position': '2', 'Laps': '99',
                  'FirstName': 'Jo', 'LastName': 'X', 'BestLapTime': '1:47.0'},
            'b': {'Number': '9', 'Position': '1', 'Laps': '100',
                  'FirstName': 'Al', 'LastName': 'Y', 'BestLapTime': '1:46.0'},
            'c': {'Number': '10', 'Position': '10', 'Laps': '98',
                  'FirstName': 'Bo', 'LastName': 'Z', 'BestLapTime': '1:48.0'},
        }}}
        m.set_standings(session)
        rows = m.leaderboard_rows()
        assert [r[0] for r in rows] == [1, 2, 10]
        assert rows[0][1] == '9'

    def test_set_standings_ignores_unsuccessful(self):
        m = LapBoardModel()
        m.set_standings({'Successful': False})
        assert m.leaderboard_rows() == []

    def test_set_standings_skips_non_numeric_position(self):
        m = LapBoardModel()
        session = {'Successful': True, 'Session': {'Competitors': {
            'a': {'Number': '5', 'Position': '1', 'Laps': '100',
                  'FirstName': 'Bo', 'LastName': 'A', 'BestLapTime': '1:46.0'},
            'b': {'Number': '8', 'Position': 'DNF', 'Laps': '95',
                  'FirstName': 'Jo', 'LastName': 'B', 'BestLapTime': '1:47.5'},
        }}}
        m.set_standings(session)
        rows = m.leaderboard_rows()
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == '5'


class TestPickerScreen:
    @pytest.mark.asyncio
    async def test_numeric_id_resolves_directly(self):
        client = _client_with_race(is_live=True)
        app = LapsApp(client)  # on_mount pushes PickerScreen automatically
        async with app.run_test() as pilot:
            await pilot.pause()
            app.screen.query_one('#query', Input).value = '42'
            await pilot.press('enter')
            await app.workers.wait_for_complete()  # deterministic: await the resolve worker
            await pilot.pause()
        client.race.details.assert_called_with(42)
        assert app.picked is not None
        assert app.picked[1] is True  # is_live

    @pytest.mark.asyncio
    async def test_non_live_race_pushes_import_confirm_screen(self):
        client = _client_with_race(is_live=False)
        app = LapsApp(client)  # on_mount pushes PickerScreen automatically
        async with app.run_test() as pilot:
            await pilot.pause()
            app.screen.query_one('#query', Input).value = '42'
            await pilot.press('enter')
            await app.workers.wait_for_complete()  # deterministic: await the resolve worker
            await pilot.pause()
            assert isinstance(app.screen, ImportConfirmScreen)
        client.race.details.assert_called_with(42)

    @pytest.mark.asyncio
    async def test_name_query_lists_hits(self):
        client = _client_with_race()
        app = LapsApp(client)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.screen.query_one('#query', Input).value = 'sears'
            await pilot.press('enter')
            await app.workers.wait_for_complete()  # deterministic: await the search worker
            await pilot.pause()
            hits = app.screen.query_one('#hits', ListView)
            assert len(hits.children) == 1


def _client_live_session():
    client = MagicMock()
    client.live.get_session.return_value = {'Successful': True, 'Session': {
        'ID': 's1', 'Name': 'S1', 'Competitors': {
            'a': {'Number': '7', 'FirstName': 'Jo', 'LastName': 'X'}}}}
    return client


class TestCarSelectScreen:
    @pytest.mark.asyncio
    async def test_lists_competitors(self):
        client = _client_live_session()
        app = LapsApp(client)
        async with app.run_test() as pilot:
            app.push_screen(CarSelectScreen(client, 42, 'Sears'))
            await app.workers.wait_for_complete()  # await the get_session worker
            await pilot.pause()
            cars = app.screen.query_one('#cars', ListView)
            assert len(cars.children) == 1

    @pytest.mark.asyncio
    async def test_typed_number_confirms_with_defaults(self):
        client = _client_live_session()
        app = LapsApp(client)
        # _start_monitor pushes MonitorScreen (Task 8), whose worker would launch
        # a real live_race against the mock — patch it out. The default 'Write to
        # InfluxDB' checkbox is on, so the worker also opens a real _influx.connect()
        # before live_race; patch that too so the worker thread never touches the
        # network or requires INFLUX_TELEMETRY_TOKEN to be set.
        with patch('lemongrass.laps.live_race', return_value=None), \
                patch('lemongrass._influx.connect'):
            async with app.run_test() as pilot:
                app.push_screen(CarSelectScreen(client, 42, 'Sears'))
                await app.workers.wait_for_complete()
                await pilot.pause()
                number = app.screen.query_one('#car-number', Input)
                number.focus()          # Enter routes to the focused Input
                number.value = '7'
                await pilot.press('enter')
                await pilot.pause()
        assert app.monitor_args == (42, '7', True, 30)


class TestMonitorScreen:
    @pytest.mark.asyncio
    async def test_observer_updates_lap_table(self):
        client = MagicMock()
        app = LapsApp(client)

        # Stub live_race so no real polling happens; drive the observer directly.
        # The signature MUST include _stop_event — MonitorScreen._run passes it,
        # and patch forwards all kwargs to the side_effect (a missing param would
        # raise TypeError before the body runs, leaving the table empty).
        def fake_live_race(ctx, opts, observer=None, _stop_event=None):
            observer.on_laps([{'Lap': '1', 'LapTime': '1:47', 'Position': '3'}])
            observer.on_lap({'Lap': '2', 'LapTime': '1:46', 'Position': '2'})
            return None

        async with app.run_test() as pilot:
            with patch('lemongrass.laps.live_race', side_effect=fake_live_race):
                app.push_screen(MonitorScreen(client, 42, '7', False, 0))
                await app.workers.wait_for_complete()
                await pilot.pause()
                table = app.screen.query_one('#laps', DataTable)
                assert table.row_count == 2

    @pytest.mark.asyncio
    async def test_lap_table_shows_newest_lap_first(self):
        client = MagicMock()
        app = LapsApp(client)

        def fake_live_race(ctx, opts, observer=None, _stop_event=None):
            observer.on_laps([{'Lap': '1', 'LapTime': '1:50', 'Position': '6'},
                              {'Lap': '2', 'LapTime': '1:48', 'Position': '4'}])
            observer.on_lap({'Lap': '3', 'LapTime': '1:46', 'Position': '2'})
            return None

        async with app.run_test() as pilot:
            with patch('lemongrass.laps.live_race', side_effect=fake_live_race):
                app.push_screen(MonitorScreen(client, 42, '7', False, 0))
                await app.workers.wait_for_complete()
                await pilot.pause()
                table = app.screen.query_one('#laps', DataTable)
                # Row 0 is the most recent lap (3), row order descends.
                first_cell = table.get_row_at(0)[0]
                last_cell = table.get_row_at(table.row_count - 1)[0]
                assert first_cell == '3'
                assert last_cell == '1'

    @pytest.mark.asyncio
    async def test_run_logs_race_monitor_error_when_not_cancelled(self):
        # Guards the normal (not-cancelled) error path: MonitorScreen._run's
        # except RaceMonitorError guards call_from_thread with
        # get_current_worker().is_cancelled (mirroring _TuiObserver._call) so a
        # 429-style error mid-teardown doesn't crash the app. This test confirms
        # the guard doesn't also suppress error reporting in the common case
        # where the worker is still running.
        client = MagicMock()
        app = LapsApp(client)

        async with app.run_test() as pilot:
            with patch('lemongrass.laps.live_race', side_effect=RaceMonitorError('boom')):
                app.push_screen(MonitorScreen(client, 42, '7', False, 0))
                await app.workers.wait_for_complete()
                await pilot.pause()
                log = app.screen.query_one('#log', RichLog)
                text = '\n'.join(str(line) for line in log.lines)
                assert 'boom' in text

    def test_network_ctx_populates_write_handles(self):
        # Finding: network mode must build a write-enabled ctx (write/delete/query
        # handles + metadata), else every InfluxDB write silently no-ops.
        client = MagicMock()
        client.race.details.return_value = {'Successful': True, 'Race': {
            'Name': 'R', 'Track': 'T', 'StartDateEpoc': 111}}
        from lemongrass import laps as laps_mod
        screen = MonitorScreen(client, 42, '7', True, 0)
        influx = MagicMock()
        ctx = screen._network_ctx(laps_mod, influx)
        assert ctx.write_api is influx.write_api.return_value
        assert ctx.delete_api is influx.delete_api.return_value
        assert ctx.query_api is influx.query_api.return_value
        assert ctx.metadata is not None
        assert ctx.start_epoc == 111

    @pytest.mark.asyncio
    async def test_run_survives_non_race_monitor_error(self):
        # Finding 2: MonitorScreen._run only caught RaceMonitorError, so any other
        # exception (e.g. a bad Influx token raising something else) would exit the
        # whole Textual app via the worker's default exit_on_error=True instead of
        # surfacing in the log pane.
        client = MagicMock()
        app = LapsApp(client)

        async with app.run_test() as pilot:
            with patch('lemongrass.laps.live_race', side_effect=ValueError('boom')):
                app.push_screen(MonitorScreen(client, 42, '7', False, 0))
                await app.workers.wait_for_complete()
                await pilot.pause()
                assert app.is_running
                log = app.screen.query_one('#log', RichLog)
                text = '\n'.join(str(line) for line in log.lines)
                assert 'boom' in text


class TestImportFlow:
    @pytest.mark.asyncio
    async def test_confirm_starts_import(self):
        client = MagicMock()
        app = LapsApp(client)
        # The pushed ImportScreen's worker runs backfill_race; patch it so no
        # real import launches against the mock client.
        with patch('lemongrass.laps.backfill_race', return_value=0):
            async with app.run_test() as pilot:
                app.push_screen(ImportConfirmScreen(client, 42, 'Sears'))
                await pilot.pause()
                await pilot.press('enter')
                await pilot.pause()
                assert isinstance(app.screen, ImportScreen)

    @pytest.mark.asyncio
    async def test_import_runs_backfill_and_reports(self):
        client = MagicMock()
        app = LapsApp(client)
        with patch('lemongrass.laps.backfill_race', return_value=0) as bf:
            async with app.run_test() as pilot:
                app.push_screen(ImportScreen(client, 42, 'Sears'))
                await app.workers.wait_for_complete()
                await pilot.pause()
        bf.assert_called_once()
        assert bf.call_args.args[0] == '42'

    @pytest.mark.asyncio
    async def test_uncancelled_worker_surfaces_summary(self):
        # Guards against the is_cancelled check (added for #q-during-import) silently
        # swallowing the summary on the normal, not-cancelled completion path.
        client = MagicMock()
        app = LapsApp(client)
        with patch('lemongrass.laps.backfill_race', return_value=0):
            async with app.run_test() as pilot:
                screen = ImportScreen(client, 42, 'Sears')
                app.push_screen(screen)
                await app.workers.wait_for_complete()
                await pilot.pause()
                title = screen.query_one('#title', Label)
                assert str(title.render()) == 'Import complete.'

    @pytest.mark.asyncio
    async def test_backfill_stdout_is_captured_not_leaked_to_terminal(self):
        # Finding 1: backfill_race does unconditional print() calls (race header,
        # completed-race import path via _StdoutObserver). _routed_output only
        # routes logging.*, not stdout, so those prints would corrupt the Textual
        # display. ImportScreen._run must bind its sink (via _sink_bound) for the
        # worker so print() routes into the same sink the log-pane timer drains.
        client = MagicMock()
        app = LapsApp(client)

        def fake_backfill_race(race_id, car_number, client, opts, observer=None):
            print('RACE HEADER LINE')
            return 0

        with patch('lemongrass.laps.backfill_race', side_effect=fake_backfill_race):
            async with app.run_test() as pilot:
                screen = ImportScreen(client, 42, 'Sears')
                app.push_screen(screen)
                await app.workers.wait_for_complete()
                await pilot.pause()
                buffered = list(screen.sink.lines)
                log = screen.query_one('#log', RichLog)
                drained_text = '\n'.join(str(line) for line in log.lines)
                assert 'RACE HEADER LINE' in buffered or 'RACE HEADER LINE' in drained_text

    @pytest.mark.asyncio
    async def test_run_survives_non_race_monitor_error(self):
        # Finding 2: ImportScreen._run only caught RaceMonitorError, so any other
        # exception raised out of backfill_race (e.g. a bad Influx token) would
        # exit the whole Textual app instead of surfacing in the UI.
        client = MagicMock()
        app = LapsApp(client)

        with patch('lemongrass.laps.backfill_race', side_effect=ValueError('bad influx token')):
            async with app.run_test() as pilot:
                screen = ImportScreen(client, 42, 'Sears')
                app.push_screen(screen)
                await app.workers.wait_for_complete()
                await pilot.pause()
                assert app.is_running
                title = screen.query_one('#title', Label)
                log = screen.query_one('#log', RichLog)
                log_text = '\n'.join(str(line) for line in log.lines)
                assert 'bad influx token' in str(title.render()) or 'bad influx token' in log_text


class TestFinalImportPrompt:
    @pytest.mark.asyncio
    async def test_offer_then_confirm_pushes_import(self):
        client = MagicMock()
        app = LapsApp(client)
        # Confirming pushes ImportScreen, whose worker runs backfill_race — patch it.
        with patch('lemongrass.laps.backfill_race', return_value=0):
            async with app.run_test() as pilot:
                app.offer_final_import(42)
                await pilot.pause()
                await pilot.press('y')
                await pilot.pause()
                assert isinstance(app.screen, ImportScreen)


class TestLapsFlowMixin:
    @pytest.mark.asyncio
    async def test_start_laps_pushes_picker(self):
        app = LapsApp(_client_with_race())
        async with app.run_test() as pilot:
            await pilot.pause()
            assert isinstance(app.screen, PickerScreen)

    def test_lapsapp_uses_mixin(self):
        assert issubclass(LapsApp, LapsFlowMixin)
