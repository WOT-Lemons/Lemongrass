"""Tests for the races browser list view (RacesBrowserScreen)."""

from unittest.mock import MagicMock, patch

import pytest
from textual.app import App
from textual.widgets import SelectionList

from lemongrass._races_tui import RacesBrowserScreen, _row_label
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
