from unittest.mock import MagicMock

import pytest
from textual.widgets import Input, ListView

from lemongrass._laps_tui import LapBoardModel, LapsApp


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
