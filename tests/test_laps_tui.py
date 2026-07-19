from lemongrass._laps_tui import LapBoardModel


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
        }}}
        m.set_standings(session)
        rows = m.leaderboard_rows()
        assert [r[0] for r in rows] == [1, 2]
        assert rows[0][1] == '9'

    def test_set_standings_ignores_unsuccessful(self):
        m = LapBoardModel()
        m.set_standings({'Successful': False})
        assert m.leaderboard_rows() == []
