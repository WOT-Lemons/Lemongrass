"""Tests for the races browser list view (RacesBrowserScreen)."""

from unittest.mock import MagicMock, patch

import pytest
from textual.app import App
from textual.widgets import Input, Label, SelectionList

from lemongrass._races_tui import (
    DiagnoseCarScreen,
    DiagnoseOutputScreen,
    RacesBrowserScreen,
    _row_label,
    distinct_car_numbers,
)
from lemongrass._tui import _TuiLogHandler


def _rows():
    return [
        {'race_id': '144185', 'name': 'Sears Pointless', 'date': '2026-06-01',
         'total': 100, 'current': 100, 'schema_version': 3},
        {'race_id': '120037', 'name': 'Arse Freeze', 'date': '2026-01-10',
         'total': 50, 'current': 10, 'schema_version': 3},
    ]


class _Host(App):
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.log_handler = _TuiLogHandler()

    def on_mount(self):
        self.push_screen(RacesBrowserScreen())


def test_row_label_marks_stale_and_current():
    assert 'current' in _row_label(_rows()[0])
    assert 'stale' in _row_label(_rows()[1])


@pytest.mark.asyncio
async def test_prune_deletes_checked_and_reloads():
    app = _Host(MagicMock())
    delete_api = MagicMock()
    fake_influx = MagicMock()
    fake_influx.delete_api.return_value = delete_api
    with patch('lemongrass._races_tui._influx.influx_token_present', return_value=True), \
         patch('lemongrass._races_tui.fetch_race_rows', return_value=_rows()), \
         patch('lemongrass._races_tui._influx.connect') as connect:
        connect.return_value.__enter__.return_value = fake_influx
        async with app.run_test() as pilot:
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()
            app.screen.query_one('#races', SelectionList).select(0)  # check race 144185
            await pilot.pause()
            await pilot.press('p')                                   # open modal
            await pilot.pause()
            await pilot.press('y')                                   # confirm
            await app.workers.wait_for_complete()
            await pilot.pause()
    # prune deleted across buckets for the checked race
    assert delete_api.delete.called


@pytest.mark.asyncio
async def test_browser_loads_rows_into_selection_list():
    app = _Host(MagicMock())
    with patch('lemongrass._races_tui._influx.influx_token_present', return_value=True), \
         patch('lemongrass._races_tui.fetch_race_rows', return_value=_rows()), \
         patch('lemongrass._races_tui._influx.connect') as connect:
        connect.return_value.__enter__.return_value = MagicMock()
        async with app.run_test() as pilot:
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()
            sl = app.screen.query_one('#races', SelectionList)
            assert len(sl._options) == 2


@pytest.mark.asyncio
async def test_diagnose_output_runs_both_and_streams():
    client = MagicMock()
    app = _Host(client)
    calls = {}

    def _api(c, rid, car):
        print(f'API {rid} {car}')
        calls['api'] = (rid, car)
        return (0, 0)

    def _influx_diag(q, rid, car, start_epoc=0, end_epoc=0):
        print(f'INFLUX {rid} {car}')
        calls['influx'] = (rid, car)

    with patch('lemongrass._races_tui.diagnose_api', _api), \
         patch('lemongrass._races_tui.diagnose_influx', _influx_diag), \
         patch('lemongrass._races_tui._influx.connect') as connect:
        connect.return_value.__enter__.return_value = MagicMock()
        async with app.run_test() as pilot:
            await pilot.pause()
            app.push_screen(DiagnoseOutputScreen('144185', '252'))
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()
            assert calls['api'] == ('144185', '252')
            assert calls['influx'] == ('144185', '252')


def test_distinct_car_numbers_extracts_values():
    rec = MagicMock()
    rec.get_value.return_value = '252'
    table = MagicMock()
    table.records = [rec]
    api = MagicMock()
    api.query.return_value = [table]
    assert distinct_car_numbers(api, '144185') == ['252']


@pytest.mark.asyncio
async def test_diagnose_car_rejects_invalid_number():
    app = _Host(MagicMock())
    with patch('lemongrass._races_tui._influx.connect') as connect:
        connect.return_value.__enter__.return_value = MagicMock()
        with patch('lemongrass._races_tui.distinct_car_numbers', return_value=[]):
            async with app.run_test() as pilot:
                await pilot.pause()
                screen = DiagnoseCarScreen('144185', 'Sears')
                app.push_screen(screen)
                await pilot.pause()
                car_input = screen.query_one('#car', Input)
                car_input.focus()  # ListView auto-focuses on mount; Input needs it explicitly
                await pilot.pause()
                car_input.value = 'bad;id'
                await pilot.press('enter')
                await pilot.pause()
                # invalid car number must not push a DiagnoseOutputScreen
                assert not isinstance(app.screen, DiagnoseOutputScreen)
                status = str(screen.query_one('#status', Label).render())
                assert 'invalid car number' in status


@pytest.mark.asyncio
async def test_reimport_pushes_import_screen():
    from lemongrass._laps_tui import ImportScreen
    app = _Host(MagicMock())
    with patch('lemongrass._races_tui._influx.influx_token_present', return_value=True), \
         patch('lemongrass._races_tui.fetch_race_rows', return_value=_rows()), \
         patch('lemongrass._races_tui._influx.connect') as connect, \
         patch('lemongrass.laps.backfill_race', return_value=0):
        connect.return_value.__enter__.return_value = MagicMock()
        async with app.run_test() as pilot:
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()
            app.screen.query_one('#races', SelectionList).highlighted = 0
            await pilot.pause()
            await pilot.press('r')
            await pilot.pause()
            assert isinstance(app.screen, ImportScreen)


@pytest.mark.asyncio
async def test_backfill_run_calls_run_backfill():
    app = _Host(MagicMock())
    from lemongrass._races_tui import BackfillRunScreen
    with patch('lemongrass._races_tui.run_backfill', return_value=[]) as rb:
        async with app.run_test() as pilot:
            await pilot.pause()
            app.push_screen(BackfillRunScreen([{'ID': 1, 'Name': 'X', 'StartDateEpoc': 0}]))
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()
    rb.assert_called_once()
    assert rb.call_args.kwargs == {'dry_run': False, 'force': False}
