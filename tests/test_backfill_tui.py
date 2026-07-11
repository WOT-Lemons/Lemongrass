from unittest.mock import MagicMock

import pytest
from race_monitor import RaceMonitorError
from textual.widgets import Input, Label, ListView, SelectionList

from lemongrass._backfill_tui import BackfillApp, RaceListModel


def _race(id, name, start_epoc):
    return {'ID': id, 'Name': name, 'StartDateEpoc': start_epoc}


def _model(races_by_term=None, terms=('t1',), start_epoc=0, series=None):
    return RaceListModel(terms, races_by_term or {}, start_epoc, series=series)


def _app(races_by_term=None, terms=('t1',), start_epoc=0, client=None,
         series=None, series_error=None):
    model = RaceListModel(terms, races_by_term or {}, start_epoc, series=series)
    return BackfillApp(client or MagicMock(), model, series_error=series_error)


class TestRaceListModel:
    def test_all_races_checked_initially(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        assert m.checked == {1, 2}

    def test_races_deduped_across_terms_and_date_sorted(self):
        shared = _race(2, 'shared', 200)
        m = _model({'t1': [shared, _race(3, 'three', 300)],
                    't2': [shared, _race(1, 'one', 100)]},
                   terms=('t1', 't2'))
        assert [r['ID'] for r in m.races()] == [1, 2, 3]

    def test_races_filtered_by_start_epoc(self):
        m = _model({'t1': [_race(1, 'old', 50), _race(2, 'new', 150)]},
                   start_epoc=100)
        assert [r['ID'] for r in m.races()] == [2]
        assert m.checked == {2}

    def test_toggle_unchecks_then_rechecks(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.toggle(1)
        assert m.checked == set()
        m.toggle(1)
        assert m.checked == {1}

    def test_set_all_and_invert(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        m.set_all(False)
        assert m.checked == set()
        m.toggle(1)
        m.invert()
        assert m.checked == {2}
        m.set_all(True)
        assert m.checked == {1, 2}

    def test_add_term_merges_prechecked_preserving_existing_state(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        m.toggle(1)  # user unchecked race 1
        m.add_term('t2', [_race(2, 'two', 200), _race(3, 'three', 300)])
        assert [r['ID'] for r in m.races()] == [1, 2, 3]
        assert m.checked == {2, 3}  # 1 stays unchecked, new 3 pre-checked

    def test_add_term_filters_by_start_epoc(self):
        m = _model({'t1': [_race(1, 'one', 150)]}, start_epoc=100)
        m.add_term('t2', [_race(2, 'old', 50)])
        assert [r['ID'] for r in m.races()] == [1]

    def test_add_blank_or_duplicate_term_is_noop(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.add_term('  ', [])
        m.add_term('t1', [_race(9, 'dup', 900)])
        assert m.terms == ['t1']
        assert [r['ID'] for r in m.races()] == [1]

    def test_remove_term_drops_only_races_matched_solely_by_that_term(self):
        shared = _race(2, 'shared', 200)
        m = _model({'t1': [_race(1, 'one', 100), shared],
                    't2': [shared, _race(3, 'three', 300)]},
                   terms=('t1', 't2'))
        m.remove_term('t2')
        assert [r['ID'] for r in m.races()] == [1, 2]
        assert m.checked == {1, 2}

    def test_readd_removed_term_uses_cache(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.add_term('t2', [_race(2, 'two', 200)])
        m.remove_term('t2')
        assert m.has_cached('t2')
        m.add_term('t2')  # no results arg: served from session cache
        assert [r['ID'] for r in m.races()] == [1, 2]
        assert m.checked == {1, 2}  # reappearing race is pre-checked again

    def test_terms_changed_tracks_difference_from_initial(self):
        m = _model({'t1': [], 't2': []}, terms=('t1', 't2'))
        assert m.terms_changed is False
        m.remove_term('t2')
        assert m.terms_changed is True
        m.add_term('t2')
        assert m.terms_changed is False  # back to the initial tuple

    def test_selected_returns_checked_races_date_sorted(self):
        m = _model({'t1': [_race(2, 'two', 200), _race(1, 'one', 100),
                           _race(3, 'three', 300)]})
        m.toggle(2)
        assert [r['ID'] for r in m.selected()] == [1, 3]

    def test_seeded_series_races_visible_and_prechecked(self):
        m = _model({'t1': [_race(1, 'one', 100)]},
                   series=(1234, 'Lemons', [_race(9, 'series', 300)]))
        assert [r['ID'] for r in m.races()] == [1, 9]
        assert m.checked == {1, 9}
        assert m.series == (1234, 'Lemons', 1)
        assert m.series_id == 1234
        assert m.series_changed is False

    def test_no_series_by_default(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        assert m.series is None
        assert m.series_id == 0
        assert m.series_changed is False

    def test_set_series_prechecks_new_preserves_existing_state(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        m.toggle(1)  # user unchecked race 1
        m.set_series(1234, 'Lemons', [_race(2, 'two', 200), _race(9, 'new', 300)])
        assert [r['ID'] for r in m.races()] == [1, 2, 9]
        assert m.checked == {2, 9}  # 1 stays unchecked, new 9 pre-checked
        assert m.series_changed is True

    def test_set_series_replaces_previous_series(self):
        m = _model({'t1': [_race(1, 'one', 100)]},
                   series=(1234, 'Lemons', [_race(9, 'old-series', 300)]))
        m.set_series(5678, 'Other', [_race(7, 'new-series', 400)])
        assert [r['ID'] for r in m.races()] == [1, 7]
        assert m.checked == {1, 7}  # 9 dropped with the old series
        assert m.series == (5678, 'Other', 1)

    def test_repin_same_series_id_is_not_changed(self):
        m = _model({'t1': []}, series=(1234, 'Lemons', []))
        m.set_series(1234, 'Lemons', [])
        assert m.series_changed is False

    def test_series_races_filtered_by_start_epoc(self):
        m = _model({'t1': [_race(1, 'one', 150)]}, start_epoc=100,
                   series=(1234, 'Lemons', [_race(8, 'old', 50), _race(9, 'new', 200)]))
        assert [r['ID'] for r in m.races()] == [1, 9]
        assert m.series == (1234, 'Lemons', 1)

    def test_remove_term_keeps_series_races(self):
        shared = _race(2, 'shared', 200)
        m = _model({'t1': [_race(1, 'one', 100), shared]},
                   series=(1234, 'Lemons', [shared]))
        m.remove_term('t1')
        assert [r['ID'] for r in m.races()] == [2]
        assert m.terms == []

    def test_cache_results_does_not_activate_term(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.cache_results('gp', [_race(2, 'two', 200)])
        assert m.terms == ['t1']
        assert [r['ID'] for r in m.races()] == [1]
        assert m.has_cached('gp')
        m.add_term('gp')  # served from cache, no results arg needed
        assert [r['ID'] for r in m.races()] == [1, 2]


class TestBackfillAppCore:
    @pytest.mark.asyncio
    async def test_enter_confirms_all_races_by_default(self):
        app = _app({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        async with app.run_test() as pilot:
            await pilot.press('enter')
        result = app.return_value
        assert [r['ID'] for r in result.races] == [1, 2]
        assert result.terms == ('t1',)
        assert result.terms_changed is False

    @pytest.mark.asyncio
    async def test_space_toggles_highlighted_race(self):
        app = _app({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        async with app.run_test() as pilot:
            # this Textual patch version needs an explicit move to highlight row 0
            await pilot.press('down')
            await pilot.press('space')  # checklist is focused; row 1 highlighted
            await pilot.press('enter')
        assert [r['ID'] for r in app.return_value.races] == [2]

    @pytest.mark.asyncio
    async def test_select_all_and_invert_keys(self):
        app = _app({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        async with app.run_test() as pilot:
            await pilot.press('i')  # invert: all -> none
            # this Textual patch version needs an explicit move to highlight row 0
            await pilot.press('down')
            await pilot.press('space')  # check row 1
            await pilot.press('enter')
        assert [r['ID'] for r in app.return_value.races] == [1]

    @pytest.mark.asyncio
    async def test_q_cancels_with_none(self):
        app = _app({'t1': [_race(1, 'one', 100)]})
        async with app.run_test() as pilot:
            await pilot.press('q')
        assert app.return_value is None

    @pytest.mark.asyncio
    async def test_escape_cancels_with_none(self):
        app = _app({'t1': [_race(1, 'one', 100)]})
        async with app.run_test() as pilot:
            await pilot.press('escape')
        assert app.return_value is None


class TestBackfillAppSeries:
    @pytest.mark.asyncio
    async def test_seeded_series_shown_and_confirmed_in_result(self):
        app = _app({'t1': [_race(1, 'one', 100)]},
                   series=(1234, 'Lemons', [_race(9, 'series', 300)]))
        async with app.run_test() as pilot:
            label = str(app.query_one('#series', Label).content)
            assert 'Lemons' in label and '1 races' in label
            await pilot.press('enter')
        result = app.return_value
        assert [r['ID'] for r in result.races] == [1, 9]
        assert result.series_id == 1234
        assert result.series_changed is False

    @pytest.mark.asyncio
    async def test_no_series_shows_hint(self):
        app = _app({'t1': [_race(1, 'one', 100)]})
        async with app.run_test():
            label = str(app.query_one('#series', Label).content)
        assert 'press s' in label

    @pytest.mark.asyncio
    async def test_series_error_shows_state_and_notifies(self):
        app = _app({'t1': [_race(1, 'one', 100)]},
                   series_error=RaceMonitorError('beta broke'))
        app.notify = MagicMock()
        async with app.run_test() as pilot:
            label = str(app.query_one('#series', Label).content)
            assert 'failed' in label
            await pilot.press('enter')
        app.notify.assert_called_once()
        assert app.notify.call_args.kwargs.get('severity') == 'error'
        assert app.return_value.series_id == 0


class TestBackfillAppTerms:
    async def _type_term(self, pilot, app, term):
        app.query_one('#new-term', Input).focus()
        await pilot.pause()
        for ch in term:
            await pilot.press(ch)
        await pilot.press('enter')
        await app.workers.wait_for_complete()
        await pilot.pause()

    @pytest.mark.asyncio
    async def test_add_term_searches_and_merges_prechecked(self):
        client = MagicMock()
        client.results.search_results.return_value = {
            'Races': [_race(3, 'three', 300)]}
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._type_term(pilot, app, 'gp')
            app.query_one('#races', SelectionList).focus()
            await pilot.pause()
            await pilot.press('enter')
        client.results.search_results.assert_called_once_with('gp')
        result = app.return_value
        assert [r['ID'] for r in result.races] == [1, 3]
        assert result.terms == ('t1', 'gp')
        assert result.terms_changed is True

    @pytest.mark.asyncio
    async def test_add_term_filters_new_results_by_start_epoc(self):
        client = MagicMock()
        client.results.search_results.return_value = {
            'Races': [_race(3, 'too-old', 50)]}
        app = _app({'t1': [_race(1, 'one', 150)]}, start_epoc=100, client=client)
        async with app.run_test() as pilot:
            await self._type_term(pilot, app, 'gp')
            app.query_one('#races', SelectionList).focus()
            await pilot.pause()
            await pilot.press('enter')
        assert [r['ID'] for r in app.return_value.races] == [1]

    @pytest.mark.asyncio
    async def test_search_failure_notifies_and_does_not_add_term(self):
        client = MagicMock()
        client.results.search_results.side_effect = RaceMonitorError('boom')
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._type_term(pilot, app, 'gp')
        assert app.model.terms == ['t1']

    @pytest.mark.asyncio
    async def test_remove_term_key_drops_its_races(self):
        app = _app({'t1': [_race(1, 'one', 100)], 't2': [_race(2, 'two', 200)]},
                   terms=('t1', 't2'))
        async with app.run_test() as pilot:
            app.query_one('#terms', ListView).focus()
            await pilot.pause()
            # this Textual patch version starts with no highlighted row, so two
            # presses are needed to reach index 1 ('t2')
            await pilot.press('down')
            await pilot.press('down')  # highlight 't2'
            await pilot.press('d')
            app.query_one('#races', SelectionList).focus()
            await pilot.pause()
            await pilot.press('enter')
        result = app.return_value
        assert [r['ID'] for r in result.races] == [1]
        assert result.terms == ('t1',)

    @pytest.mark.asyncio
    async def test_readding_removed_term_hits_cache_not_client(self):
        client = MagicMock()
        client.results.search_results.return_value = {
            'Races': [_race(2, 'two', 200)]}
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._type_term(pilot, app, 'gp')     # live search (1 call)
            app.query_one('#terms', ListView).focus()
            await pilot.pause()
            # this Textual patch version starts with no highlighted row, so two
            # presses are needed to reach index 1 ('gp')
            await pilot.press('down')
            await pilot.press('down')
            await pilot.press('d')                       # remove 'gp'
            await self._type_term(pilot, app, 'gp')     # re-add: cache hit
            app.query_one('#races', SelectionList).focus()
            await pilot.pause()
            await pilot.press('enter')
        assert client.results.search_results.call_count == 1
        assert [r['ID'] for r in app.return_value.races] == [1, 2]
