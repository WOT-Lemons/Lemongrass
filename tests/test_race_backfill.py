import importlib
import logging
import os
import sys
import tomllib
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest
from race_monitor import RaceMonitorError

import lemongrass.race_backfill as _mod
from lemongrass.laps import SCHEMA_VERSION


def _make_race(id, name, start_epoc):
    return {'ID': id, 'Name': name, 'StartDateEpoc': start_epoc}


EPOC_2020 = 1577836800
EPOC_2021 = 1609459200
EPOC_2022 = 1640995200
EPOC_2023 = 1672531200


@contextmanager
def _inprocess_backfill(results=None):
    """Patch the in-process backfill seam (shared RaceMonitorClient +
    resolve_tokens + laps.backfill_race), yielding the backfill_race mock.

    The mock carries a `.calls` list recording each invocation as
    SimpleNamespace(race_id, car_number, client, opts), so tests assert on named
    arguments and an observable client identity rather than brittle positional
    call_args indices. It also exposes `.client_cls`, the patched
    RaceMonitorClient, for asserting how many clients were created. `results`,
    when given, is the per-call outcome list — an int is returned as the exit
    code, a BaseException instance is raised; the default is 0 for every call.
    """
    calls = []
    outcomes = iter(results) if results is not None else None

    def record(race_id, car_number, client, opts):
        calls.append(SimpleNamespace(race_id=race_id, car_number=car_number,
                                     client=client, opts=opts))
        if outcomes is None:
            return 0
        outcome = next(outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()) as mk_client, \
         patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
         patch('lemongrass.laps.backfill_race', side_effect=record) as mk_backfill:
        mk_backfill.calls = calls
        mk_backfill.client_cls = mk_client
        yield mk_backfill


class TestFindMatchingRaces:
    def _client(self, responses_by_term):
        """responses_by_term: dict mapping search term → list of race dicts."""
        client = MagicMock()
        client.results.search_results.side_effect = lambda term: {
            'Races': responses_by_term.get(term, [])
        }
        return client

    def test_searches_each_lemons_term(self):
        client = self._client({})
        _mod.find_matching_races(client, start_epoc=EPOC_2021)
        called_terms = [c.args[0] for c in client.results.search_results.call_args_list]
        for term in _mod.LEMONS_SEARCH_TERMS:
            assert term in called_terms

    def test_returns_races_at_or_after_start_year(self):
        client = self._client({
            'Real Hoopties': [_make_race(1, 'Real Hoopties 2022', EPOC_2022)],
        })
        races = _mod.find_matching_races(client, start_epoc=EPOC_2021)
        assert len(races) == 1
        assert races[0]['ID'] == 1

    def test_excludes_races_before_start_year(self):
        client = self._client({
            'Real Hoopties': [_make_race(1, 'Real Hoopties 2020', EPOC_2020)],
        })
        races = _mod.find_matching_races(client, start_epoc=EPOC_2021)
        assert races == []

    def test_deduplicates_races_appearing_in_multiple_searches(self):
        client = self._client({
            'Real Hoopties': [_make_race(1, 'Real Hoopties Halloween 2022', EPOC_2022)],
            'GP du Lac': [],
            'Halloween Hoop': [_make_race(1, 'Real Hoopties Halloween 2022', EPOC_2022)],
        })
        races = _mod.find_matching_races(client, start_epoc=EPOC_2021)
        assert len(races) == 1

    def test_returns_races_sorted_by_start_date(self):
        client = self._client({
            'Real Hoopties': [
                _make_race(2, 'Real Hoopties 2023', EPOC_2023),
                _make_race(1, 'Real Hoopties 2022', EPOC_2022),
            ],
        })
        races = _mod.find_matching_races(client, start_epoc=EPOC_2021)
        assert [r['ID'] for r in races] == [1, 2]


class TestSearchRacesByTerm:
    def _client(self, responses_by_term):
        client = MagicMock()
        client.results.search_results.side_effect = lambda term: {
            'Races': responses_by_term.get(term, [])
        }
        return client

    def test_one_search_call_per_term_in_order(self):
        client = self._client({})
        _mod.search_races_by_term(client, ('alpha', 'beta'), start_epoc=EPOC_2021)
        called = [c.args[0] for c in client.results.search_results.call_args_list]
        assert called == ['alpha', 'beta']

    def test_filters_races_before_start_epoc(self):
        client = self._client({'alpha': [_make_race(1, 'old', EPOC_2020),
                                         _make_race(2, 'new', EPOC_2022)]})
        by_term = _mod.search_races_by_term(client, ('alpha',), start_epoc=EPOC_2021)
        assert [r['ID'] for r in by_term['alpha']] == [2]

    def test_keeps_per_term_attribution_without_dedup(self):
        shared = _make_race(2, 'shared', EPOC_2022)
        client = self._client({'alpha': [shared], 'beta': [shared]})
        by_term = _mod.search_races_by_term(client, ('alpha', 'beta'),
                                            start_epoc=EPOC_2021)
        assert by_term['alpha'] == [shared]
        assert by_term['beta'] == [shared]


class TestMergeRaces:
    def test_dedups_by_id_and_sorts_by_start_date(self):
        by_term = {
            'alpha': [_make_race(2, 'later', EPOC_2022),
                      _make_race(1, 'early', EPOC_2021)],
            'beta': [_make_race(2, 'later', EPOC_2022)],
        }
        merged = _mod.merge_races(by_term)
        assert [r['ID'] for r in merged] == [1, 2]


def _series_race(id, start_epoc, has_results=True):
    return {'ID': id, 'Name': f'race-{id}', 'StartDateEpoc': start_epoc,
            'HasResults': has_results, 'SeriesName': '24 Hours of Lemons'}


class TestEnumerateSeries:
    def _client(self, responses):
        client = MagicMock()
        client.common.past_races.side_effect = responses
        return client

    def test_filters_has_results_and_start_epoc(self):
        client = self._client([{'Successful': True, 'Races': [
            _series_race(1, EPOC_2020),                      # too old
            _series_race(2, EPOC_2022),                      # kept
            _series_race(3, EPOC_2023, has_results=False),   # no results
        ]}])
        races = _mod.enumerate_series(client, 1234, EPOC_2021)
        assert [r['ID'] for r in races] == [2]

    def test_single_short_page_makes_one_call(self):
        client = self._client([{'Successful': True,
                                'Races': [_series_race(1, EPOC_2022)]}])
        _mod.enumerate_series(client, 1234, 0)
        client.common.past_races.assert_called_once_with(
            series_id=1234, first_result=0, max_results=100)

    def test_paginates_until_short_page(self):
        page1 = [_series_race(i, EPOC_2022) for i in range(100)]
        page2 = [_series_race(100, EPOC_2022), _series_race(101, EPOC_2022)]
        client = self._client([{'Successful': True, 'Races': page1},
                               {'Successful': True, 'Races': page2}])
        races = _mod.enumerate_series(client, 1234, 0)
        assert len(races) == 102
        offsets = [c.kwargs['first_result']
                   for c in client.common.past_races.call_args_list]
        assert offsets == [0, 100]

    def test_unsuccessful_response_raises(self):
        client = self._client([{'Successful': False, 'Races': []}])
        with pytest.raises(RaceMonitorError):
            _mod.enumerate_series(client, 1234, 0)

    def test_missing_races_key_raises(self):
        client = self._client([{'Successful': True}])
        with pytest.raises(RaceMonitorError):
            _mod.enumerate_series(client, 1234, 0)


class TestRunBackfill:
    def _races(self):
        return [
            _make_race(101, 'Real Hoopties 2022', EPOC_2022),
            _make_race(202, 'Halloween Hoop 2022', EPOC_2022),
        ]

    def test_calls_laps_for_each_race(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_backfill(self._races())
        assert mk_backfill.call_count == 2

    def test_dry_run_does_not_backfill(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()) as mk_client, \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_backfill(self._races(), dry_run=True)
        mk_backfill.assert_not_called()
        mk_client.assert_not_called()

    def test_dry_run_logs_race_name(self, caplog):
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.run_backfill(self._races(), dry_run=True)
        assert any('Real Hoopties 2022' in r.message for r in caplog.records)

    def test_failure_summary_logged_when_any_race_fails(self, caplog):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=1):
            with caplog.at_level(logging.ERROR, logger='root'):
                _mod.run_backfill(self._races())
        assert any('2 race(s) failed' in r.message for r in caplog.records)

    def test_no_failure_summary_when_all_succeed(self, caplog):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=0):
            with caplog.at_level(logging.ERROR, logger='root'):
                _mod.run_backfill(self._races())
        assert not any('failed' in r.message for r in caplog.records)

    def test_returns_failed_race_ids(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', side_effect=[0, 1]):
            failures = _mod.run_backfill(self._races())
        assert failures == ['202']

    def test_returns_empty_list_when_all_succeed(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=0):
            failures = _mod.run_backfill(self._races())
        assert failures == []

    def test_passes_skip_if_complete_by_default(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False) as mk_skip, \
             patch('lemongrass.laps.backfill_race', return_value=0):
            _mod.run_backfill(self._races()[:1])
        mk_skip.assert_called()

    # --- in-process backfill: one shared client/rate-limiter across all races ---

    def test_reuses_single_client_across_races(self):
        """One RaceMonitorClient (and its rate-limiter window) is shared across
        every race, instead of a fresh subprocess/window per race."""
        with _inprocess_backfill() as mk_backfill, \
             patch('lemongrass.laps._influx_only_skip', return_value=False):
            _mod.run_backfill(self._races())
        assert mk_backfill.client_cls.call_count == 1
        assert mk_backfill.call_count == 2
        clients = {c.client for c in mk_backfill.calls}
        assert len(clients) == 1

    def test_backfill_called_fieldwide_with_race_id(self):
        with _inprocess_backfill() as mk_backfill, \
             patch('lemongrass.laps._influx_only_skip', return_value=False):
            _mod.run_backfill(self._races()[:1])
        assert mk_backfill.calls[0].race_id == '101'
        assert mk_backfill.calls[0].car_number is None

    def test_skip_if_complete_skips_without_client_or_racemonitor(self):
        """A race already complete in Influx is skipped with no client created and
        no backfill_race call — the whole point of --skip-if-complete."""
        with patch.object(_mod, 'RaceMonitorClient') as mk_client, \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=True), \
             patch('lemongrass.laps.backfill_race') as mk_backfill:
            _mod.run_backfill(self._races()[:1])
        mk_backfill.assert_not_called()
        mk_client.assert_not_called()

    def test_force_bypasses_skip_check(self):
        """force=True disables skip_if_complete, so _influx_only_skip is never
        consulted and every race is backfilled."""
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=True) as mk_skip, \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_backfill(self._races()[:1], force=True)
        mk_skip.assert_not_called()
        assert mk_backfill.call_count == 1


class TestBackfillOneRace:
    """Direct coverage of the per-race exception/interrupt/return contract that
    both run_backfill and run_upgrade_stored delegate to."""

    def _call(self, **backfill_kwargs):
        failures = []
        with patch('lemongrass.laps.backfill_race', **backfill_kwargs):
            stop = _mod._backfill_one_race(MagicMock(), '101', MagicMock(),
                                           failures, 'Backfill')
        return stop, failures

    def test_racemonitor_error_recorded_and_continues(self):
        from race_monitor import RaceMonitorHTTPError
        stop, failures = self._call(side_effect=RaceMonitorHTTPError(429, 'rate'))
        assert stop is False
        assert failures == ['101']

    def test_keyboard_interrupt_signals_stop(self):
        stop, failures = self._call(side_effect=KeyboardInterrupt())
        assert stop is True
        assert failures == []

    def test_systemexit_130_signals_stop(self):
        stop, failures = self._call(side_effect=SystemExit(130))
        assert stop is True
        assert failures == []

    def test_systemexit_non_130_recorded_and_continues(self):
        stop, failures = self._call(side_effect=SystemExit(2))
        assert stop is False
        assert failures == ['101']

    def test_programming_bug_propagates(self):
        # A non-RaceMonitorError (bug or systematic outage) must crash rather than
        # be recorded per-race, so it surfaces as a bug not a data problem.
        with pytest.raises(AttributeError):
            self._call(side_effect=AttributeError('boom'))

    def test_nonzero_return_recorded_as_failure(self):
        stop, failures = self._call(return_value=1)
        assert stop is False
        assert failures == ['101']

    def test_zero_return_no_failure(self):
        stop, failures = self._call(return_value=0)
        assert stop is False
        assert failures == []


class TestValidateBackfill:
    def _make_query_api(self, race_name='My Race', lap_count=0,
                        race_start=None, race_end_epoc=1672531200):
        if race_start is None:
            race_start = datetime(2022, 1, 1, tzinfo=UTC)

        def fake_query(flux):
            table = MagicMock()
            if 'bucket: "races"' in flux:
                if race_name is None:
                    table.records = []
                else:
                    rec = MagicMock()
                    rec.values = {'race_name': race_name}
                    rec.get_time.return_value = race_start
                    rec.get_value.return_value = race_end_epoc
                    table.records = [rec]
            else:
                if lap_count:
                    rec = MagicMock()
                    rec.get_value.return_value = lap_count
                    table.records = [rec]
                else:
                    table.records = []
            return [table]

        api = MagicMock()
        api.query.side_effect = fake_query
        return api

    def test_returns_true_and_logs_ok_when_race_has_laps(self, caplog):
        query_api = self._make_query_api(race_name='Lemons 2026', lap_count=15)
        with caplog.at_level(logging.INFO, logger='root'):
            assert _mod.validate_backfill(['101'], query_api) is True
        assert any('OK' in r.message and '15' in r.message and 'Lemons 2026' in r.message
                   for r in caplog.records)

    def test_returns_false_and_warns_when_race_has_no_laps(self, caplog):
        query_api = self._make_query_api(lap_count=0)
        with caplog.at_level(logging.WARNING, logger='root'):
            assert _mod.validate_backfill(['101'], query_api) is False
        assert any('NO laps' in r.message for r in caplog.records)

    def test_returns_false_when_race_metadata_missing(self):
        query_api = self._make_query_api(race_name=None, lap_count=15)
        assert _mod.validate_backfill(['101'], query_api) is False

    def test_laps_not_queried_when_race_metadata_missing(self):
        query_api = self._make_query_api(race_name=None)
        _mod.validate_backfill(['101'], query_api)
        assert query_api.query.call_count == 1

    def test_races_query_filters_by_end_time_epoc_field(self):
        query_api = self._make_query_api(lap_count=10)
        _mod.validate_backfill(['101'], query_api)
        races_flux = query_api.query.call_args_list[0].args[0]
        assert '_field == "end_time_epoc"' in races_flux

    def test_logs_warning_when_end_time_epoc_zero(self, caplog):
        query_api = self._make_query_api(lap_count=10, race_end_epoc=0)
        with caplog.at_level(logging.WARNING, logger='root'):
            _mod.validate_backfill(['101'], query_api)
        assert any('end_time_epoc' in r.message for r in caplog.records
                   if r.levelno == logging.WARNING)


class TestRunUpgradeStored:
    def _query_api(self, stored_races=None, total_by_race=None, current_by_race=None,
                   std_total_by_race=None, std_current_by_race=None):
        """
        stored_races: dict of race_id -> race_name
        total_by_race: dict of race_id -> total lap count
        current_by_race: dict of race_id -> current-schema lap count
        std_total_by_race: dict of race_id -> total standings count
        std_current_by_race: dict of race_id -> current-schema standings count
        """
        stored_races = stored_races or {}
        total_by_race = total_by_race or {}
        current_by_race = current_by_race or {}
        std_total_by_race = std_total_by_race or {}
        std_current_by_race = std_current_by_race or {}

        def _count_for(source, flux):
            count = 0
            for race_id, c in source.items():
                if f'race_id == "{race_id}"' in flux:
                    count = c
            rec = MagicMock()
            rec.get_value.return_value = count
            return [rec]

        def fake_query(flux):
            table = MagicMock()
            if 'bucket: "races"' in flux:
                table.records = []
                for race_id, name in stored_races.items():
                    rec = MagicMock()
                    rec.values = {'race_id': race_id, 'race_name': name}
                    table.records.append(rec)
            elif '_measurement == "standings"' in flux:
                # standings query — current (schema_version == SCHEMA_VERSION) vs total (position)
                if 'r._field == "schema_version"' in flux:
                    source = std_current_by_race
                else:
                    source = std_total_by_race
                table.records = _count_for(source, flux)
            elif '"schema_version"' in flux:
                table.records = []
                for race_id, count in current_by_race.items():
                    if f'race_id == "{race_id}"' in flux:
                        rec = MagicMock()
                        rec.get_value.return_value = count
                        table.records.append(rec)
            else:
                # total laps query
                table.records = _count_for(total_by_race, flux)
            return [table]

        api = MagicMock()
        api.query.side_effect = fake_query
        return api

    @contextmanager
    def _inprocess(self, results=None):
        """Patch the in-process backfill seam, yielding the backfill_race mock
        (see module-level _inprocess_backfill)."""
        with _inprocess_backfill(results) as mk_backfill:
            yield mk_backfill

    def test_skips_race_already_at_current_schema(self, caplog):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 8},
            std_current_by_race={'101': 8},
        )
        with self._inprocess() as mk_backfill:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api)
        mk_backfill.assert_not_called()
        assert any('skipping' in r.message and '101' in r.message for r in caplog.records)

    def test_standings_freshness_filters_on_current_schema_value(self):
        # The freshness contract requires filtering standings on the current
        # schema value, not merely the presence of a schema_version field.
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 8},
            std_current_by_race={'101': 8},
        )
        with self._inprocess():
            _mod.run_upgrade_stored(query_api)
        std_current_queries = [
            c.args[0] for c in query_api.query.call_args_list
            if '_measurement == "standings"' in c.args[0]
            and 'r._field == "schema_version"' in c.args[0]
        ]
        assert std_current_queries
        assert all(f'r._value == {SCHEMA_VERSION}' in q for q in std_current_queries)

    def test_rebackfills_when_laps_current_but_standings_stale(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 8},
            std_current_by_race={'101': 3},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1
        assert mk_backfill.calls[0].race_id == '101'
        assert mk_backfill.calls[0].car_number is None

    def test_rebackfills_when_laps_current_but_standings_missing(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 0},
            std_current_by_race={'101': 0},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1

    def test_rebackfills_stale_race(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1
        assert mk_backfill.calls[0].race_id == '101'
        assert mk_backfill.calls[0].car_number is None

    def test_dry_run_does_not_backfill(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api, dry_run=True)
        mk_backfill.assert_not_called()

    def test_skips_race_with_no_stored_laps(self, caplog):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 0},
            current_by_race={},
        )
        with self._inprocess() as mk_backfill:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api)
        mk_backfill.assert_not_called()

    def test_processes_races_in_ascending_numeric_order(self):
        # race_id keys are Influx tag strings; a plain sorted() orders them
        # lexicographically, so a shorter id sorts into the middle of longer ones.
        # Sort numerically so the run is a predictable ascending sweep.
        query_api = self._query_api(
            stored_races={'113450': 'C', '9793': 'A', '100247': 'B'},
            total_by_race={'113450': 5, '9793': 5, '100247': 5},
            current_by_race={'113450': 0, '9793': 0, '100247': 0},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api, force=True)
        called_ids = [c.race_id for c in mk_backfill.calls]
        assert called_ids == ['9793', '100247', '113450']

    def test_returns_failed_race_ids(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess(results=[1]):
            failures = _mod.run_upgrade_stored(query_api)
        assert failures == ['101']

    def test_returns_empty_list_on_success(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess():
            failures = _mod.run_upgrade_stored(query_api)
        assert failures == []

    def test_stops_on_interrupt(self):
        query_api = self._query_api(
            stored_races={'101': 'Race 1', '202': 'Race 2'},
            total_by_race={'101': 5, '202': 5},
            current_by_race={'101': 0, '202': 0},
        )
        with self._inprocess(results=[SystemExit(130), 0]) as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1

    def test_force_rebackfills_race_already_at_current_schema(self, caplog):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already fully current
        )
        with self._inprocess() as mk_backfill:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api, force=True)
        assert mk_backfill.call_count == 1
        assert mk_backfill.calls[0].race_id == '101'
        assert mk_backfill.calls[0].car_number is None
        assert any('already current' in r.message and 'force re-backfilling' in r.message
                   for r in caplog.records)

    def test_dry_run_force_does_not_backfill(self, caplog):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already current; only --force would touch it
        )
        with self._inprocess() as mk_backfill:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api, dry_run=True, force=True)
        mk_backfill.assert_not_called()
        assert any('would force re-backfill' in r.message for r in caplog.records)

    def test_force_still_skips_race_with_no_stored_laps(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 0},
            current_by_race={},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api, force=True)
        mk_backfill.assert_not_called()

    # --- in-process backfill: one shared client/rate-limiter across all races ---

    def test_reuses_single_client_across_races(self):
        """One RaceMonitorClient (and its rate-limiter window) is shared across
        every race, instead of a fresh subprocess/window per race."""
        query_api = self._query_api(
            stored_races={'101': 'Race 1', '202': 'Race 2'},
            total_by_race={'101': 10, '202': 10},
            current_by_race={'101': 5, '202': 5},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.client_cls.call_count == 1
        assert mk_backfill.call_count == 2
        clients = {c.client for c in mk_backfill.calls}
        assert len(clients) == 1

    def test_backfill_called_fieldwide_with_race_id(self):
        """Each race is backfilled fieldwide: race_id passed, car_number None."""
        query_api = self._query_api(
            stored_races={'101': 'Race 1'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1
        assert mk_backfill.calls[0].race_id == '101'
        assert mk_backfill.calls[0].car_number is None


class TestParseStartDate:
    def test_valid_date_returns_utc_midnight_epoch(self):
        assert _mod._parse_start_date('2021-01-01') == EPOC_2021

    def test_bad_format_exits_1(self):
        with pytest.raises(SystemExit) as exc:
            _mod._parse_start_date('01/01/2021')
        assert exc.value.code == 1


class TestArgParsing:
    @pytest.mark.parametrize('argv, attr, expected', [
        (['--dry-run'], 'dry_run', True),
        (['--validate'], 'validate', True),
        (['--force'], 'force', True),
        (['--upgrade-stored'], 'upgrade_stored', True),
        (['--start-date', '2023-06-01'], 'start_date', '2023-06-01'),
    ])
    def test_flag_sets_attribute(self, argv, attr, expected):
        args = _mod._build_parser().parse_args(argv)
        assert getattr(args, attr) == expected

    def test_upgrade_stored_and_force_not_mutually_exclusive(self):
        # Previously mutually exclusive — must now parse together without error.
        args = _mod._build_parser().parse_args(['--upgrade-stored', '--force'])
        assert args.upgrade_stored is True
        assert args.force is True


class TestOpenClient:
    def test_exits_when_no_token(self):
        with patch.object(_mod, 'resolve_tokens', return_value=[]):
            with pytest.raises(SystemExit) as exc:
                _mod._open_client()
        assert exc.value.code == 1

    def test_returns_client_built_from_resolved_tokens(self):
        with patch.object(_mod, 'resolve_tokens', return_value=['tok']):
            with patch.object(_mod, 'RaceMonitorClient', return_value='CLIENT') as mk:
                client = _mod._open_client()
        assert client == 'CLIENT'
        mk.assert_called_once_with(api_token=['tok'])


class TestMainTokenResolution:
    def test_main_wires_resolve_tokens_through(self):
        """Smoke test: main() resolves tokens and passes them to the client.
        Thorough token-resolution coverage lives in test_race_diagnose.py."""
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'TOKEN1,TOKEN2'}, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with patch('lemongrass.race_backfill.RaceMonitorClient') as mock_rm_cls:
                    mock_rm_cls.return_value.__enter__.return_value = MagicMock()
                    with patch.object(_mod, 'search_races_by_term', return_value={'term': []}):
                        with patch.object(_mod, 'run_backfill', return_value=[]):
                            _mod.main()
        mock_rm_cls.assert_called_once_with(api_token=['TOKEN1', 'TOKEN2'])


class TestMainInteractive:
    RACES: ClassVar[list] = [_make_race(1, 'one', EPOC_2022), _make_race(2, 'two', EPOC_2023)]

    @contextmanager
    def _main_harness(self, tty, refine_result='unset'):
        """Run main() with discovery stubbed; yields (run_backfill, refine) mocks."""
        client = MagicMock()
        client.__enter__ = MagicMock(return_value=client)
        client.__exit__ = MagicMock(return_value=False)
        by_term = {t: list(self.RACES) for t in _mod.LEMONS_SEARCH_TERMS}
        with patch.object(_mod, 'RaceMonitorClient', return_value=client), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch.object(_mod, 'search_races_by_term', return_value=by_term), \
             patch.object(_mod, 'run_backfill', return_value=[]) as backfill, \
             patch.object(_mod.sys.stdin, 'isatty', return_value=tty), \
             patch.object(_mod.sys.stdout, 'isatty', return_value=tty), \
             patch('lemongrass._backfill_tui.refine_races') as refine:
            if refine_result != 'unset':
                refine.return_value = refine_result
            yield backfill, refine

    def test_non_tty_skips_tui_and_backfills_all(self):
        with self._main_harness(tty=False) as (backfill, refine):
            with patch.object(sys, 'argv', ['race-backfill']):
                _mod.main()
        refine.assert_not_called()
        races = backfill.call_args.args[0]
        assert [r['ID'] for r in races] == [1, 2]

    def test_tty_cancel_exits_zero_without_backfill(self):
        with self._main_harness(tty=True, refine_result=None) as (backfill, _):
            with patch.object(sys, 'argv', ['race-backfill']):
                with pytest.raises(SystemExit) as exc:
                    _mod.main()
        assert exc.value.code == 0
        backfill.assert_not_called()

    def test_tty_confirm_backfills_selected_subset(self):
        from lemongrass._backfill_tui import RefineResult
        subset = RefineResult(races=[self.RACES[1]],
                              terms=tuple(_mod.LEMONS_SEARCH_TERMS),
                              terms_changed=False)
        with self._main_harness(tty=True, refine_result=subset) as (backfill, refine):
            with patch.object(sys, 'argv', ['race-backfill']):
                _mod.main()
        refine.assert_called_once()
        races = backfill.call_args.args[0]
        assert [r['ID'] for r in races] == [2]

    def test_tty_validate_uses_selected_subset(self):
        from lemongrass._backfill_tui import RefineResult
        subset = RefineResult(races=[self.RACES[0]],
                              terms=tuple(_mod.LEMONS_SEARCH_TERMS),
                              terms_changed=False)
        with self._main_harness(tty=True, refine_result=subset):
            with patch.object(_mod, 'validate_backfill', return_value=True) as val, \
                 patch.object(_mod._influx, 'connect', MagicMock()):
                with patch.object(sys, 'argv', ['race-backfill', '--validate']):
                    with pytest.raises(SystemExit) as exc:
                        _mod.main()
        assert exc.value.code == 0
        assert val.call_args.args[0] == ['1']


class TestMain:
    """main() control flow: dispatch, exit codes, and interrupt handling."""

    def test_upgrade_stored_with_start_date_exits_1(self):
        with patch.object(sys, 'argv',
                          ['race-backfill', '--upgrade-stored', '--start-date', '1999-01-01']):
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        assert exc.value.code == 1

    def test_upgrade_stored_with_validate_exits_1(self):
        with patch.object(sys, 'argv', ['race-backfill', '--upgrade-stored', '--validate']):
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        assert exc.value.code == 1

    def _run_upgrade_main(self, run_kwargs):
        with patch.object(sys, 'argv', ['race-backfill', '--upgrade-stored']):
            with patch('lemongrass._influx.connect') as mk_connect:
                mk_connect.return_value.__enter__.return_value = MagicMock()
                with patch.object(_mod, 'run_upgrade_stored', **run_kwargs) as mk_run:
                    with pytest.raises(SystemExit) as exc:
                        _mod.main()
        return exc, mk_run

    def test_upgrade_stored_dispatches_and_exits_0_on_success(self):
        exc, mk_run = self._run_upgrade_main({'return_value': []})
        mk_run.assert_called_once()
        assert exc.value.code == 0

    def test_upgrade_stored_exits_1_on_failures(self):
        exc, _ = self._run_upgrade_main({'return_value': ['101']})
        assert exc.value.code == 1

    def test_upgrade_stored_keyboard_interrupt_exits_130(self):
        exc, _ = self._run_upgrade_main({'side_effect': KeyboardInterrupt()})
        assert exc.value.code == 130

    def _run_validate_main(self, ok):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'T'}, clear=True):
            with patch.object(sys, 'argv', ['race-backfill', '--validate']):
                with patch('lemongrass.race_backfill.RaceMonitorClient') as mk_rm:
                    mk_rm.return_value.__enter__.return_value = MagicMock()
                    with patch.object(
                            _mod, 'search_races_by_term',
                            return_value={'term': [_make_race(101, 'R', EPOC_2022)]}):
                        with patch('lemongrass._influx.connect') as mk_connect:
                            mk_connect.return_value.__enter__.return_value = MagicMock()
                            with patch.object(_mod, 'validate_backfill',
                                              return_value=ok) as mk_val:
                                with pytest.raises(SystemExit) as exc:
                                    _mod.main()
        return exc, mk_val

    def test_validate_dispatches_and_exits_0_when_ok(self):
        exc, mk_val = self._run_validate_main(ok=True)
        mk_val.assert_called_once()
        assert mk_val.call_args.args[0] == ['101']
        assert exc.value.code == 0

    def test_validate_exits_1_when_not_ok(self):
        exc, _ = self._run_validate_main(ok=False)
        assert exc.value.code == 1

    def test_backfill_failures_exit_1(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'T'}, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with patch('lemongrass.race_backfill.RaceMonitorClient') as mk_rm:
                    mk_rm.return_value.__enter__.return_value = MagicMock()
                    with patch.object(_mod, 'search_races_by_term', return_value={'term': []}):
                        with patch.object(_mod, 'run_backfill', return_value=['101']):
                            with pytest.raises(SystemExit) as exc:
                                _mod.main()
        assert exc.value.code == 1

    def test_backfill_keyboard_interrupt_exits_130(self):
        with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'T'}, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with patch('lemongrass.race_backfill.RaceMonitorClient') as mk_rm:
                    mk_rm.return_value.__enter__.return_value = MagicMock()
                    with patch.object(_mod, 'search_races_by_term',
                                      side_effect=KeyboardInterrupt()):
                        with pytest.raises(SystemExit) as exc:
                            _mod.main()
        assert exc.value.code == 130

    def test_no_token_exits_1(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with pytest.raises(SystemExit) as exc:
                    _mod.main()
        assert exc.value.code == 1


class TestMainConfiguresLogging:
    def test_main_configures_info_logging(self):
        # race-backfill runs under the `lemongrass` console script, whose dispatch
        # path (cli.main -> races.main -> race_backfill.main) never configures
        # logging — the basicConfig in the __main__ guard doesn't run on import.
        # main() must configure INFO itself, or every progress logging.info() line
        # (SKIP / re-backfilling / summary) is silently dropped at the default
        # WARNING level. Observe the effect (root level set to INFO) rather than
        # that basicConfig was called, by simulating the unconfigured console-script
        # environment where the root logger has no handlers.
        root = logging.getLogger()
        saved_handlers = root.handlers[:]
        saved_level = root.level
        root.handlers = []
        try:
            with patch.dict(os.environ, {'RACEMONITOR_TOKENS': 'T'}, clear=True):
                with patch.object(sys, 'argv', ['race-backfill']):
                    with patch('lemongrass.race_backfill.RaceMonitorClient') as mock_rm_cls:
                        mock_rm_cls.return_value.__enter__.return_value = MagicMock()
                        with patch.object(_mod, 'search_races_by_term',
                                          return_value={'term': []}):
                            with patch.object(_mod, 'run_backfill', return_value=[]):
                                _mod.main()
            assert root.level == logging.INFO
        finally:
            root.handlers = saved_handlers
            root.setLevel(saved_level)


class TestValidateWindowPadding:
    def test_lap_query_padded_one_day_each_side(self):
        query_api = MagicMock()

        race_record = MagicMock()
        race_record.values = {'race_name': 'Test Race', 'race_id': '999'}
        race_record.get_time.return_value = datetime(2020, 1, 2, tzinfo=UTC)
        race_record.get_value.return_value = int(
            datetime(2020, 1, 4, tzinfo=UTC).timestamp())
        race_table = MagicMock()
        race_table.records = [race_record]

        lap_table = MagicMock()
        lap_table.records = []
        query_api.query.side_effect = [[race_table], [lap_table]]

        _mod.validate_backfill(['999'], query_api)
        lap_flux = query_api.query.call_args_list[1].args[0]
        assert 'start: 2020-01-01T00:00:00Z' in lap_flux
        assert 'stop: 2020-01-05T00:00:00Z' in lap_flux


def test_backfill_defaults_come_from_config(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text(
        '[races.backfill]\n'
        'search_terms = ["Enduro X"]\n'
        'default_start_date = "2019-03-15"\n'
    )
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    from lemongrass import race_backfill
    try:
        reloaded = importlib.reload(race_backfill)
        assert reloaded.LEMONS_SEARCH_TERMS == ("Enduro X",)
        assert reloaded.DEFAULT_START_DATE == "2019-03-15"
    finally:
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        importlib.reload(race_backfill)  # restore default module state


def _refine_result(terms=('a', 'b'), changed=True):
    from lemongrass._backfill_tui import RefineResult
    return RefineResult(races=[], terms=tuple(terms), terms_changed=changed)


class TestSaveSearchTerms:
    def test_updates_terms_preserving_comments_and_other_keys(self, tmp_path):
        cfg = tmp_path / 'lemongrass.toml'
        cfg.write_text('# my config\n'
                       '[races.backfill]\n'
                       'search_terms = ["old"]\n'
                       '\n'
                       '[influx]\n'
                       'url = "http://example"\n')
        assert _mod._save_search_terms(str(cfg), ('a', 'b')) is True
        text = cfg.read_text()
        assert '# my config' in text
        data = tomllib.loads(text)
        assert data['races']['backfill']['search_terms'] == ['a', 'b']
        assert data['influx']['url'] == 'http://example'

    def test_creates_missing_tables(self, tmp_path):
        cfg = tmp_path / 'lemongrass.toml'
        cfg.write_text('[influx]\nurl = "http://example"\n')
        assert _mod._save_search_terms(str(cfg), ('a',)) is True
        data = tomllib.loads(cfg.read_text())
        assert data['races']['backfill']['search_terms'] == ['a']

    def test_returns_false_on_unreadable_path(self, tmp_path, caplog):
        missing = tmp_path / 'nope' / 'lemongrass.toml'
        with caplog.at_level(logging.WARNING):
            assert _mod._save_search_terms(str(missing), ('a',)) is False
        assert any('could not save' in r.message for r in caplog.records)

    def test_failed_write_leaves_original_intact_and_no_temp_files(
            self, tmp_path, monkeypatch, caplog):
        cfg = tmp_path / 'lemongrass.toml'
        original = '[races.backfill]\nsearch_terms = ["old"]\n'
        cfg.write_text(original)

        def _boom(*_):
            raise OSError('disk full')
        monkeypatch.setattr(os, 'replace', _boom)
        with caplog.at_level(logging.WARNING):
            assert _mod._save_search_terms(str(cfg), ('a',)) is False
        assert cfg.read_text() == original
        assert list(tmp_path.iterdir()) == [cfg]

    def test_preserves_file_mode(self, tmp_path):
        cfg = tmp_path / 'lemongrass.toml'
        cfg.write_text('')
        os.chmod(cfg, 0o644)
        assert _mod._save_search_terms(str(cfg), ('a',)) is True
        assert os.stat(cfg).st_mode & 0o777 == 0o644

    def test_bare_filename_saves_relative_to_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'lemongrass.toml').write_text('')
        assert _mod._save_search_terms('lemongrass.toml', ('a',)) is True
        data = tomllib.loads((tmp_path / 'lemongrass.toml').read_text())
        assert data['races']['backfill']['search_terms'] == ['a']


class TestMaybeSaveTerms:
    def test_unchanged_terms_never_prompt(self, monkeypatch, capsys):
        monkeypatch.setenv('LEMONGRASS_CONFIG', '/some/path.toml')
        monkeypatch.setattr('builtins.input',
                            lambda *_: pytest.fail('should not prompt'))
        _mod._maybe_save_terms(_refine_result(changed=False))
        assert capsys.readouterr().out == ''

    def test_no_config_prints_snippet_without_prompt(self, monkeypatch, capsys):
        monkeypatch.delenv('LEMONGRASS_CONFIG', raising=False)
        monkeypatch.setattr('builtins.input',
                            lambda *_: pytest.fail('should not prompt'))
        _mod._maybe_save_terms(_refine_result(terms=('a', 'b')))
        out = capsys.readouterr().out
        assert '[races.backfill]' in out
        assert 'search_terms = ["a", "b"]' in out

    def test_snippet_escapes_toml_special_characters(self, monkeypatch, capsys):
        monkeypatch.delenv('LEMONGRASS_CONFIG', raising=False)
        terms = ('say "hi"', 'back\\slash')
        _mod._maybe_save_terms(_refine_result(terms=terms))
        out = capsys.readouterr().out
        snippet = out[out.index('[races.backfill]'):]
        data = tomllib.loads(snippet)
        assert data['races']['backfill']['search_terms'] == list(terms)

    def test_yes_saves_to_config_path(self, monkeypatch, tmp_path):
        cfg = tmp_path / 'lemongrass.toml'
        cfg.write_text('')
        monkeypatch.setenv('LEMONGRASS_CONFIG', str(cfg))
        monkeypatch.setattr('builtins.input', lambda *_: 'y')
        _mod._maybe_save_terms(_refine_result(terms=('a',)))
        data = tomllib.loads(cfg.read_text())
        assert data['races']['backfill']['search_terms'] == ['a']

    def test_default_answer_is_no(self, monkeypatch, tmp_path):
        cfg = tmp_path / 'lemongrass.toml'
        cfg.write_text('')
        monkeypatch.setenv('LEMONGRASS_CONFIG', str(cfg))
        monkeypatch.setattr('builtins.input', lambda *_: '')
        _mod._maybe_save_terms(_refine_result())
        assert cfg.read_text() == ''

    def test_eof_on_prompt_treated_as_no(self, monkeypatch, tmp_path):
        cfg = tmp_path / 'lemongrass.toml'
        cfg.write_text('')
        monkeypatch.setenv('LEMONGRASS_CONFIG', str(cfg))

        def _eof(*_):
            raise EOFError
        monkeypatch.setattr('builtins.input', _eof)
        _mod._maybe_save_terms(_refine_result())
        assert cfg.read_text() == ''

    def test_failed_save_falls_back_to_snippet(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setenv('LEMONGRASS_CONFIG', str(tmp_path / 'nope' / 'x.toml'))
        monkeypatch.setattr('builtins.input', lambda *_: 'y')
        _mod._maybe_save_terms(_refine_result(terms=('a',)))
        assert '[races.backfill]' in capsys.readouterr().out
