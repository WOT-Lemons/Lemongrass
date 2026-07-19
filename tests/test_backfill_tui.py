import logging
from unittest.mock import MagicMock

import pytest
from race_monitor import RaceMonitorError
from textual.widgets import Input, Label, ListView, RichLog, SelectionList

from lemongrass._backfill_tui import (
    BackfillApp,
    RaceListModel,
    SeriesSearchModal,
)


def _race(id, name, start_epoc):
    return {'ID': id, 'Name': name, 'StartDateEpoc': start_epoc}


def _model(races_by_term=None, terms=('t1',), start_epoc=0, series=None):
    return RaceListModel(terms, races_by_term or {}, start_epoc, series=series)


def _app(races_by_term=None, terms=('t1',), start_epoc=0, client=None,
         series=None, series_error=None):
    model = RaceListModel(terms, races_by_term or {}, start_epoc, series=series)
    return BackfillApp(client or MagicMock(), model, series_error=series_error)


def _info_record(message):
    return logging.LogRecord('httpx', logging.INFO, __file__, 0,
                             message, None, None)


class TestBackfillAppLogPane:
    @pytest.mark.asyncio
    async def test_buffered_lines_drain_into_pane(self):
        app = _app({'t1': [_race(1, 'one', 100)]})
        async with app.run_test() as pilot:
            app.log_handler.emit(_info_record('sleeping 9.77s'))
            await pilot.pause(0.4)  # past the 0.25s drain interval
            log_view = app.query_one('#log', RichLog)
            assert len(log_view.lines) >= 1


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

    def test_no_series_by_default(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        assert m.series is None
        assert m.series_id == 0
        assert m.series_changed is False

    def test_repin_same_series_id_is_not_changed(self):
        m = _model({'t1': []}, series=(1234, 'Lemons', []))
        m.set_series(1234, 'Lemons', [])
        assert m.series_changed is False

    def test_cache_results_does_not_activate_term(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.cache_results('gp', [_race(2, 'two', 200)])
        assert m.terms == ['t1']
        assert [r['ID'] for r in m.races()] == [1]
        assert m.has_cached('gp')
        m.add_term('gp')  # served from cache, no results arg needed
        assert [r['ID'] for r in m.races()] == [1, 2]

    def test_pinned_series_filtered_by_terms(self):
        m = _model({'t1': [_race(1, 'one t1', 100)]},
                   series=(1234, 'Lemons', [_race(9, 'gp t1 2024', 300),
                                            _race(8, 'other race', 200)]))
        # Series is the only candidate set: term race 1 is not in it, 8
        # doesn't match 't1'.
        assert [r['ID'] for r in m.races()] == [9]
        assert m.checked == {9}
        assert m.series == (1234, 'Lemons', 1, 2)
        assert m.series_id == 1234

    def test_pinned_empty_terms_shows_whole_series(self):
        m = _model({}, terms=(),
                   series=(1234, 'Lemons', [_race(8, 'a', 100),
                                            _race(9, 'b', 200)]))
        assert [r['ID'] for r in m.races()] == [8, 9]
        assert m.series == (1234, 'Lemons', 2, 2)

    def test_pin_mid_session_switches_to_intersection(self):
        m = _model({'t1': [_race(1, 'one t1', 100), _race(2, 't1 shared', 200)]})
        m.toggle(1)  # user unchecked race 1
        m.set_series(1234, 'Lemons',
                     [_race(2, 't1 shared', 200), _race(9, 'gp t1', 300),
                      _race(7, 'no match', 250)])
        # 1 dropped (not in series), 7 hidden (no term match).
        assert [r['ID'] for r in m.races()] == [2, 9]
        assert m.checked == {2, 9}  # 2 was checked and survives; 9 is new
        assert m.series_changed is True

    def test_set_series_replaces_previous_series(self):
        m = _model({}, terms=(),
                   series=(1234, 'Lemons', [_race(9, 'old-series', 300)]))
        m.set_series(5678, 'Other', [_race(7, 'new-series', 400)])
        assert [r['ID'] for r in m.races()] == [7]
        assert m.checked == {7}
        assert m.series == (5678, 'Other', 1, 1)

    def test_pinned_add_term_narrows_and_prunes_checked(self):
        m = _model({}, terms=(),
                   series=(1234, 'Lemons', [_race(8, 'gp du lac', 100),
                                            _race(9, 'hoopties', 200)]))
        m.add_term('hoop')  # no cache/results needed when pinned
        assert [r['ID'] for r in m.races()] == [9]
        assert m.checked == {9}
        assert m.series == (1234, 'Lemons', 1, 2)

    def test_pinned_remove_last_term_broadens_and_checks_new(self):
        m = _model({}, terms=('hoop',),
                   series=(1234, 'Lemons', [_race(8, 'gp du lac', 100),
                                            _race(9, 'hoopties', 200)]))
        assert m.checked == {9}
        m.remove_term('hoop')
        assert [r['ID'] for r in m.races()] == [8, 9]
        assert m.checked == {8, 9}

    def test_pinned_series_start_epoc_filters_matched_and_total(self):
        m = _model({}, terms=(), start_epoc=150,
                   series=(1234, 'Lemons', [_race(8, 'old', 50),
                                            _race(9, 'new', 200)]))
        assert [r['ID'] for r in m.races()] == [9]
        assert m.series == (1234, 'Lemons', 1, 1)

    def test_unpinned_add_term_still_requires_results(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        with pytest.raises(ValueError):
            m.add_term('nocache')


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
    async def test_seeded_series_filtered_and_confirmed_in_result(self):
        app = _app({'t1': [_race(1, 'one t1', 100)]},
                   series=(1234, 'Lemons', [_race(9, 'series t1', 300),
                                            _race(8, 'unmatched', 200)]))
        async with app.run_test() as pilot:
            label = str(app.query_one('#series', Label).content)
            assert 'Lemons' in label and '1 of 2 races' in label
            await pilot.press('enter')
        result = app.return_value
        assert [r['ID'] for r in result.races] == [9]
        assert result.series_id == 1234
        assert result.series_changed is False

    @pytest.mark.asyncio
    async def test_add_term_with_pinned_series_is_local(self):
        client = MagicMock()
        app = _app({}, terms=(), client=client,
                   series=(1234, 'Lemons', [_race(8, 'gp du lac', 100),
                                            _race(9, 'hoopties', 200)]))
        async with app.run_test() as pilot:
            app.query_one('#new-term', Input).focus()
            await pilot.pause()
            for ch in 'hoop':
                await pilot.press(ch)
            await pilot.press('enter')
            await pilot.pause()
            label = str(app.query_one('#series', Label).content)
            assert '1 of 2 races' in label
        client.results.search_results.assert_not_called()
        assert app.model.terms == ['hoop']

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


class TestSeriesSearchModal:
    def _client(self, hits=None, series_id=1234, series_races=None,
                details_error=None, search_error=None):
        """Client stub for the modal flow: search → details → past_races."""
        client = MagicMock()
        if search_error:
            client.results.search_results.side_effect = search_error
        else:
            client.results.search_results.return_value = {'Races': hits or []}
        if details_error:
            client.race.details.side_effect = details_error
        else:
            client.race.details.return_value = {
                'Successful': True, 'Race': {'ID': 1, 'SeriesID': series_id}}
        client.common.past_races.return_value = {
            'Successful': True, 'Races': series_races or []}
        return client

    async def _search(self, pilot, app, query):
        """Open the modal, type query, submit, and wait for the worker."""
        await pilot.press('s')
        await pilot.pause()
        for ch in query:
            await pilot.press(ch)
        await pilot.press('enter')
        await app.workers.wait_for_complete()
        await pilot.pause()

    async def _pick_first_hit(self, pilot, app):
        """Select the first hit in the modal's results list."""
        app.screen.query_one('#series-hits', ListView).focus()
        await pilot.pause()
        await pilot.press('down')
        await pilot.press('enter')
        await app.workers.wait_for_complete()
        await pilot.pause()

    @pytest.mark.asyncio
    async def test_s_opens_modal_escape_closes(self):
        app = _app({'t1': [_race(1, 'one', 100)]})
        async with app.run_test() as pilot:
            await pilot.press('s')
            await pilot.pause()
            assert isinstance(app.screen, SeriesSearchModal)
            await pilot.press('escape')
            await pilot.pause()
            assert not isinstance(app.screen, SeriesSearchModal)
        assert app.model.series is None

    @pytest.mark.asyncio
    async def test_full_flow_pins_series(self):
        hit = {'ID': 55, 'Name': 'a lemons race', 'StartDateEpoc': 100}
        series_race = {'ID': 9, 'Name': 'series-race t1', 'StartDateEpoc': 300,
                       'HasResults': True, 'SeriesName': '24 Hours of Lemons'}
        client = self._client(hits=[hit], series_races=[series_race])
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')
            await self._pick_first_hit(pilot, app)
            assert not isinstance(app.screen, SeriesSearchModal)
            label = str(app.query_one('#series', Label).content)
            assert '24 Hours of Lemons' in label
            app.query_one('#races', SelectionList).focus()
            await pilot.pause()
            await pilot.press('enter')
        client.race.details.assert_called_once_with(55)
        result = app.return_value
        assert result.series_id == 1234
        assert result.series_changed is True
        assert [r['ID'] for r in result.races] == [9]

    @pytest.mark.asyncio
    async def test_full_flow_series_race_missing_series_name(self):
        # A series race lacking SeriesName must not crash the resolve worker;
        # the label falls back to "series <id>".
        hit = {'ID': 55, 'Name': 'a lemons race', 'StartDateEpoc': 100}
        series_race = {'ID': 9, 'Name': 'series-race t1', 'StartDateEpoc': 300,
                       'HasResults': True}
        client = self._client(hits=[hit], series_races=[series_race])
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')
            await self._pick_first_hit(pilot, app)
            assert not isinstance(app.screen, SeriesSearchModal)
            label = str(app.query_one('#series', Label).content)
            assert 'series 1234' in label
            app.query_one('#races', SelectionList).focus()
            await pilot.pause()
            await pilot.press('enter')
        result = app.return_value
        assert result.series_id == 1234
        assert [r['ID'] for r in result.races] == [9]

    @pytest.mark.asyncio
    async def test_modal_resubmit_cached_term_skips_client(self):
        # Resubmitting an already-cached term reuses the cache instead of
        # hitting the rate-limited endpoint again.
        hit = {'ID': 55, 'Name': 'a lemons race', 'StartDateEpoc': 100}
        client = self._client(hits=[hit])
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')     # live search (1 call)
            app.screen.query_one(Input).focus()
            await pilot.pause()
            await pilot.press('enter')               # resubmit: cache hit
            await pilot.pause()
        assert client.results.search_results.call_count == 1

    @pytest.mark.asyncio
    async def test_modal_search_results_cached_for_terms(self):
        hit = {'ID': 55, 'Name': 'a lemons race', 'StartDateEpoc': 100}
        client = self._client(hits=[hit])
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')
            await pilot.press('escape')
            await pilot.pause()
        assert app.model.has_cached('gp')
        assert client.results.search_results.call_count == 1

    @pytest.mark.asyncio
    async def test_search_error_keeps_modal_open(self):
        client = self._client(search_error=RaceMonitorError('boom'))
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')
            assert isinstance(app.screen, SeriesSearchModal)
        assert app.model.series is None

    @pytest.mark.asyncio
    async def test_series_id_zero_rejected(self):
        # A race with SeriesID=0 (not in a series) must not pin: past_races
        # treats 0 as "return all", and 0 is the disabled sentinel.
        hit = {'ID': 55, 'Name': 'a lemons race', 'StartDateEpoc': 100}
        client = self._client(hits=[hit], series_id=0)
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')
            await self._pick_first_hit(pilot, app)
            assert isinstance(app.screen, SeriesSearchModal)
        client.common.past_races.assert_not_called()
        assert app.model.series is None

    @pytest.mark.asyncio
    async def test_details_error_keeps_modal_open(self):
        hit = {'ID': 55, 'Name': 'a lemons race', 'StartDateEpoc': 100}
        client = self._client(hits=[hit], details_error=RaceMonitorError('boom'))
        app = _app({'t1': [_race(1, 'one', 100)]}, client=client)
        async with app.run_test() as pilot:
            await self._search(pilot, app, 'gp')
            await self._pick_first_hit(pilot, app)
            assert isinstance(app.screen, SeriesSearchModal)
        assert app.model.series is None


@pytest.mark.asyncio
async def test_enter_in_series_modal_does_not_confirm_app():
    # RefineScreen's 'enter' binding is a priority binding, but Textual only
    # consults a priority binding against the *active* screen's chain — with
    # SeriesSearchModal on top, RefineScreen.action_confirm must not fire.
    app = _app({'t1': [_race(1, 'one', 100)]})
    async with app.run_test() as pilot:
        await pilot.press('s')            # open SeriesSearchModal
        await pilot.pause()
        await pilot.press('enter')        # must submit the modal search, not exit app
        await pilot.pause()
        assert app.return_value is None   # app has not exited with a RefineResult
