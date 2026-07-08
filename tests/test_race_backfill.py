import importlib
import os
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

import lemongrass.race_backfill as _mod
from lemongrass.laps import SCHEMA_VERSION


def _make_race(id, name, start_epoc):
    return {'ID': id, 'Name': name, 'StartDateEpoc': start_epoc}


EPOC_2020 = 1577836800
EPOC_2021 = 1609459200
EPOC_2022 = 1640995200
EPOC_2023 = 1672531200


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
        import logging
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.run_backfill(self._races(), dry_run=True)
        assert any('Real Hoopties 2022' in r.message for r in caplog.records)

    def test_failure_summary_logged_when_any_race_fails(self, caplog):
        import logging
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=1):
            with caplog.at_level(logging.ERROR, logger='root'):
                _mod.run_backfill(self._races())
        assert any('2 race(s) failed' in r.message for r in caplog.records)

    def test_no_failure_summary_when_all_succeed(self, caplog):
        import logging
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
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()) as mk_client, \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_backfill(self._races())
        assert mk_client.call_count == 1
        assert mk_backfill.call_count == 2
        clients = {c.args[2] for c in mk_backfill.call_args_list}
        assert len(clients) == 1

    def test_backfill_called_fieldwide_with_race_id(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_backfill(self._races()[:1])
        assert mk_backfill.call_args.args[0] == '101'  # race_id
        assert mk_backfill.call_args.args[1] is None   # car_number (fieldwide)

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

    def test_backfill_exception_records_failure_and_continues(self):
        from race_monitor import RaceMonitorHTTPError
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race',
                   side_effect=[RaceMonitorHTTPError(429, 'rate'), 0]) as mk_backfill:
            failures = _mod.run_backfill(self._races())
        assert mk_backfill.call_count == 2
        assert failures == ['101']

    def test_keyboard_interrupt_stops_backfill(self):
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps._influx_only_skip', return_value=False), \
             patch('lemongrass.laps.backfill_race',
                   side_effect=[KeyboardInterrupt(), 0]) as mk_backfill:
            failures = _mod.run_backfill(self._races())
        assert mk_backfill.call_count == 1
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

    def test_returns_true_when_race_has_laps(self):
        query_api = self._make_query_api(lap_count=15)
        assert _mod.validate_backfill(['101'], query_api) is True

    def test_returns_false_when_race_has_no_laps(self):
        query_api = self._make_query_api(lap_count=0)
        assert _mod.validate_backfill(['101'], query_api) is False

    def test_returns_false_when_race_metadata_missing(self):
        query_api = self._make_query_api(race_name=None, lap_count=15)
        assert _mod.validate_backfill(['101'], query_api) is False

    def test_logs_race_name_in_output(self, caplog):
        import logging
        query_api = self._make_query_api(race_name='Lemons 2026', lap_count=15)
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.validate_backfill(['101'], query_api)
        assert any('Lemons 2026' in r.message for r in caplog.records)

    def test_logs_ok_with_lap_count(self, caplog):
        import logging
        query_api = self._make_query_api(lap_count=15)
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.validate_backfill(['101'], query_api)
        assert any('OK' in r.message and '15' in r.message for r in caplog.records)

    def test_logs_warning_when_no_laps(self, caplog):
        import logging
        query_api = self._make_query_api(lap_count=0)
        with caplog.at_level(logging.WARNING, logger='root'):
            _mod.validate_backfill(['101'], query_api)
        assert any('NO laps' in r.message for r in caplog.records)

    def test_laps_not_queried_when_race_metadata_missing(self):
        query_api = self._make_query_api(race_name=None)
        _mod.validate_backfill(['101'], query_api)
        assert query_api.query.call_count == 1

    def test_laps_query_scoped_to_race_time_bounds(self):
        race_start = datetime(2022, 6, 1, tzinfo=UTC)
        query_api = self._make_query_api(lap_count=10, race_start=race_start,
                                         race_end_epoc=1672531200)
        _mod.validate_backfill(['101'], query_api)
        laps_flux = query_api.query.call_args_list[1].args[0]
        assert '1970-01-01' not in laps_flux
        assert '2022-05-31' in laps_flux  # race_start (2022-06-01) padded back one day

    def test_races_query_filters_by_end_time_epoc_field(self):
        query_api = self._make_query_api(lap_count=10)
        _mod.validate_backfill(['101'], query_api)
        races_flux = query_api.query.call_args_list[0].args[0]
        assert '_field == "end_time_epoc"' in races_flux

    def test_logs_warning_when_end_time_epoc_zero(self, caplog):
        import logging
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
                    # the freshness contract requires filtering on the current value,
                    # not merely the presence of a schema_version field
                    assert f'r._value == {SCHEMA_VERSION}' in flux
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
    def _inprocess(self, **backfill_kwargs):
        """Patch the in-process backfill seam (shared client + resolve_tokens +
        laps.backfill_race), yielding the backfill_race mock."""
        backfill_kwargs.setdefault('return_value', 0)
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps.backfill_race', **backfill_kwargs) as mk_backfill:
            yield mk_backfill

    def test_skips_race_already_at_current_schema(self, caplog):
        import logging
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
        assert mk_backfill.call_args.args[0] == '101'
        assert mk_backfill.call_args.args[1] is None

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
        assert mk_backfill.call_args.args[0] == '101'
        assert mk_backfill.call_args.args[1] is None

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
        import logging
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 0},
            current_by_race={},
        )
        with self._inprocess() as mk_backfill:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api)
        mk_backfill.assert_not_called()

    def test_returns_failed_race_ids(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess(return_value=1):
            failures = _mod.run_upgrade_stored(query_api)
        assert failures == ['101']

    def test_returns_empty_list_on_success(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with self._inprocess(return_value=0):
            failures = _mod.run_upgrade_stored(query_api)
        assert failures == []

    def test_stops_on_interrupt(self):
        query_api = self._query_api(
            stored_races={'101': 'Race 1', '202': 'Race 2'},
            total_by_race={'101': 5, '202': 5},
            current_by_race={'101': 0, '202': 0},
        )
        with self._inprocess(side_effect=[SystemExit(130), 0]) as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1

    def test_force_rebackfills_race_already_at_current_schema(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already fully current
        )
        with self._inprocess() as mk_backfill:
            _mod.run_upgrade_stored(query_api, force=True)
        assert mk_backfill.call_count == 1
        assert mk_backfill.call_args.args[0] == '101'
        assert mk_backfill.call_args.args[1] is None

    def test_force_logs_already_current_message(self, caplog):
        import logging
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already fully current
        )
        with self._inprocess(return_value=0):
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api, force=True)
        assert any('already current' in r.message and 'force re-backfilling' in r.message
                   for r in caplog.records)

    def test_dry_run_force_does_not_backfill(self, caplog):
        import logging
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
        fake_client = MagicMock()
        with patch.object(_mod, 'RaceMonitorClient', return_value=fake_client) as mk_client, \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_client.call_count == 1
        assert mk_backfill.call_count == 2
        clients = {c.args[2] for c in mk_backfill.call_args_list}
        assert clients == {fake_client}

    def test_backfill_called_fieldwide_with_race_id(self):
        """Each race is backfilled fieldwide: race_id passed, car_number None."""
        query_api = self._query_api(
            stored_races={'101': 'Race 1'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps.backfill_race', return_value=0) as mk_backfill:
            _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1
        args = mk_backfill.call_args.args
        assert args[0] == '101'   # race_id
        assert args[1] is None    # car_number (fieldwide)

    def test_backfill_exception_records_failure_and_continues(self):
        """A per-race error (e.g. 429 exhaustion) is recorded and the run
        continues to the next race, matching the old continue-on-failure model."""
        from race_monitor import RaceMonitorHTTPError
        query_api = self._query_api(
            stored_races={'101': 'Race 1', '202': 'Race 2'},
            total_by_race={'101': 10, '202': 10},
            current_by_race={'101': 5, '202': 5},
        )
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps.backfill_race',
                   side_effect=[RaceMonitorHTTPError(429, 'rate'), 0]) as mk_backfill:
            failures = _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 2
        assert failures == ['101']

    def test_keyboard_interrupt_stops_upgrade(self):
        """Ctrl-C during a race stops the whole upgrade (no further races)."""
        query_api = self._query_api(
            stored_races={'101': 'Race 1', '202': 'Race 2'},
            total_by_race={'101': 10, '202': 10},
            current_by_race={'101': 5, '202': 5},
        )
        with patch.object(_mod, 'RaceMonitorClient', return_value=MagicMock()), \
             patch.object(_mod, 'resolve_tokens', return_value=['tok']), \
             patch('lemongrass.laps.backfill_race',
                   side_effect=[KeyboardInterrupt(), 0]) as mk_backfill:
            failures = _mod.run_upgrade_stored(query_api)
        assert mk_backfill.call_count == 1
        assert failures == []


class TestUpgradeStoredArgParsing:
    def test_upgrade_stored_flag_accepted(self):
        args = _mod._build_parser().parse_args(['--upgrade-stored'])
        assert args.upgrade_stored is True

    def test_upgrade_stored_default_false(self):
        args = _mod._build_parser().parse_args([])
        assert args.upgrade_stored is False

    def test_upgrade_stored_and_force_accepted_together(self):
        # Previously mutually exclusive — must now parse without error
        args = _mod._build_parser().parse_args(['--upgrade-stored', '--force'])
        assert args.upgrade_stored is True
        assert args.force is True


class TestParseStartDate:
    def test_valid_date_returns_utc_midnight_epoch(self):
        assert _mod._parse_start_date('2021-01-01') == EPOC_2021

    def test_bad_format_exits_1(self):
        with pytest.raises(SystemExit) as exc:
            _mod._parse_start_date('01/01/2021')
        assert exc.value.code == 1

    def test_start_date_arg_default_from_config(self):
        args = _mod._build_parser().parse_args([])
        assert args.start_date == _mod.DEFAULT_START_DATE

    def test_start_date_arg_override(self):
        args = _mod._build_parser().parse_args(['--start-date', '2023-06-01'])
        assert args.start_date == '2023-06-01'


class TestArgParsing:
    def test_dry_run_flag(self):
        args = _mod._build_parser().parse_args(['--dry-run'])
        assert args.dry_run is True

    def test_dry_run_default(self):
        args = _mod._build_parser().parse_args([])
        assert args.dry_run is False

    def test_validate_flag_accepted(self):
        args = _mod._build_parser().parse_args(['--validate'])
        assert args.validate is True

    def test_validate_defaults_to_false(self):
        args = _mod._build_parser().parse_args([])
        assert args.validate is False

    def test_force_flag_accepted(self):
        args = _mod._build_parser().parse_args(['--force'])
        assert args.force is True

    def test_force_defaults_to_false(self):
        args = _mod._build_parser().parse_args([])
        assert args.force is False


class TestMainTokenResolution:
    def _run_main(self, env):
        mock_client = MagicMock()
        with patch.dict(os.environ, env, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with patch('lemongrass.race_backfill.RaceMonitorClient') as mock_rm_cls:
                    mock_rm_cls.return_value.__enter__.return_value = mock_client
                    with patch.object(_mod, 'find_matching_races', return_value=[]):
                        with patch.object(_mod, 'run_backfill', return_value=[]):
                            _mod.main()
        return mock_rm_cls

    def test_uses_racemonitor_tokens_when_set(self):
        mock_rm_cls = self._run_main({'RACEMONITOR_TOKENS': 'TOKEN1'})
        mock_rm_cls.assert_called_once_with(api_token='TOKEN1')

    def test_uses_multi_token_list_when_multiple_tokens_set(self):
        mock_rm_cls = self._run_main({'RACEMONITOR_TOKENS': 'TOKEN1,TOKEN2'})
        mock_rm_cls.assert_called_once_with(api_token=['TOKEN1', 'TOKEN2'])

    def test_falls_back_to_racemonitor_token(self):
        mock_rm_cls = self._run_main({'RACEMONITOR_TOKEN': 'FALLBACK'})
        mock_rm_cls.assert_called_once_with(api_token='FALLBACK')

    def test_exits_when_no_token_set(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with pytest.raises(SystemExit) as exc_info:
                    _mod.main()
        assert exc_info.value.code == 1

    def test_no_token_error_honors_configured_pool_var(self, caplog, tmp_path):
        import logging
        cfg = tmp_path / "c.toml"
        cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
        env = {'RACEMONITOR_TOKEN': 'stale-legacy', 'LEMONGRASS_CONFIG': str(cfg)}
        with patch.dict(os.environ, env, clear=True):
            with patch.object(sys, 'argv', ['race-backfill']):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(SystemExit) as exc_info:
                        _mod.main()
        assert exc_info.value.code == 1
        assert any('MY_POOL' in r.message for r in caplog.records)


class TestValidateWindowPadding:
    def test_lap_query_padded_one_day_each_side(self):
        import lemongrass.race_backfill as rb
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

        rb.validate_backfill(['999'], query_api)
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

