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
