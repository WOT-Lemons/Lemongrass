import os
import sys
from datetime import datetime, timezone
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
        _mod.find_matching_races(client, start_year_epoc=EPOC_2021)
        called_terms = [c.args[0] for c in client.results.search_results.call_args_list]
        for term in _mod.LEMONS_SEARCH_TERMS:
            assert term in called_terms

    def test_returns_races_at_or_after_start_year(self):
        client = self._client({
            'Real Hoopties': [_make_race(1, 'Real Hoopties 2022', EPOC_2022)],
        })
        races = _mod.find_matching_races(client, start_year_epoc=EPOC_2021)
        assert len(races) == 1
        assert races[0]['ID'] == 1

    def test_excludes_races_before_start_year(self):
        client = self._client({
            'Real Hoopties': [_make_race(1, 'Real Hoopties 2020', EPOC_2020)],
        })
        races = _mod.find_matching_races(client, start_year_epoc=EPOC_2021)
        assert races == []

    def test_deduplicates_races_appearing_in_multiple_searches(self):
        client = self._client({
            'Real Hoopties': [_make_race(1, 'Real Hoopties Halloween 2022', EPOC_2022)],
            'GP du Lac': [],
            'Halloween Hoop': [_make_race(1, 'Real Hoopties Halloween 2022', EPOC_2022)],
        })
        races = _mod.find_matching_races(client, start_year_epoc=EPOC_2021)
        assert len(races) == 1

    def test_returns_races_sorted_by_start_date(self):
        client = self._client({
            'Real Hoopties': [
                _make_race(2, 'Real Hoopties 2023', EPOC_2023),
                _make_race(1, 'Real Hoopties 2022', EPOC_2022),
            ],
        })
        races = _mod.find_matching_races(client, start_year_epoc=EPOC_2021)
        assert [r['ID'] for r in races] == [1, 2]


class TestResolveCarNumber:
    def test_returns_default_when_no_override(self):
        assert _mod.resolve_car_number('101', default='252', overrides={}) == '252'

    def test_returns_override_when_present(self):
        assert _mod.resolve_car_number('101', default='252', overrides={'101': '253'}) == '253'

    def test_override_does_not_affect_other_races(self):
        assert _mod.resolve_car_number('202', default='252', overrides={'101': '253'}) == '252'


class TestRunBackfill:
    def _races(self):
        return [
            _make_race(101, 'Real Hoopties 2022', EPOC_2022),
            _make_race(202, 'Halloween Hoop 2022', EPOC_2022),
        ]

    def test_calls_laps_for_each_race(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_backfill(self._races(), default_car='252', overrides={})
        assert mock_run.call_count == 2

    def test_uses_override_car_number(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_backfill(self._races(), default_car='252', overrides={'101': '253'})
        calls = [c.args[0] for c in mock_run.call_args_list]
        assert '253' in calls[0]
        assert '252' in calls[1]

    def test_dry_run_does_not_call_subprocess(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            _mod.run_backfill(self._races(), default_car='252', overrides={}, dry_run=True)
        mock_run.assert_not_called()

    def test_dry_run_logs_race_name_and_car(self, caplog):
        import logging
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.run_backfill(self._races(), default_car='252', overrides={}, dry_run=True)
        assert any('Real Hoopties 2022' in r.message and '252' in r.message
                   for r in caplog.records)

    def test_dry_run_logs_override_car(self, caplog):
        import logging
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.run_backfill(self._races(), default_car='252',
                              overrides={'101': '253'}, dry_run=True)
        assert any('Real Hoopties 2022' in r.message and '253' in r.message
                   for r in caplog.records)

    def test_subprocess_invokes_lemongrass_laps(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_backfill(self._races()[:1], default_car='252', overrides={})
        cmd = mock_run.call_args.args[0]
        assert cmd[:2] == ['lemongrass', 'laps']

    def test_failure_summary_logged_when_any_race_fails(self, caplog):
        import logging
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with caplog.at_level(logging.ERROR, logger='root'):
                _mod.run_backfill(self._races(), default_car='252', overrides={})
        assert any('2 race(s) failed' in r.message for r in caplog.records)

    def test_no_failure_summary_when_all_succeed(self, caplog):
        import logging
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            with caplog.at_level(logging.ERROR, logger='root'):
                _mod.run_backfill(self._races(), default_car='252', overrides={})
        assert not any('failed' in r.message for r in caplog.records)

    def test_returns_failed_race_ids(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.side_effect = [MagicMock(returncode=0), MagicMock(returncode=1)]
            failures = _mod.run_backfill(self._races(), default_car='252', overrides={})
        assert failures == [('202', '252')]

    def test_returns_empty_list_when_all_succeed(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            failures = _mod.run_backfill(self._races(), default_car='252', overrides={})
        assert failures == []

    def test_passes_skip_if_complete_by_default(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_backfill(self._races()[:1], default_car='252', overrides={})
        assert '--skip-if-complete' in mock_run.call_args.args[0]

    def test_omits_skip_if_complete_when_forced(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_backfill(self._races()[:1], default_car='252', overrides={}, force=True)
        assert '--skip-if-complete' not in mock_run.call_args.args[0]


class TestBuildPairs:
    def test_builds_pairs_from_races(self):
        races = [_make_race(101, 'Real Hoopties 2022', EPOC_2022)]
        pairs = _mod.build_pairs(races, default_car='252', overrides={})
        assert pairs == [('101', '252')]

    def test_applies_override_to_matching_race(self):
        races = [_make_race(101, 'Real Hoopties 2022', EPOC_2022)]
        pairs = _mod.build_pairs(races, default_car='252', overrides={'101': '253'})
        assert pairs == [('101', '253')]

    def test_builds_multiple_pairs(self):
        races = [
            _make_race(101, 'Real Hoopties 2022', EPOC_2022),
            _make_race(202, 'GP du Lac 2022', EPOC_2022),
        ]
        pairs = _mod.build_pairs(races, default_car='252', overrides={'101': '253'})
        assert pairs == [('101', '253'), ('202', '252')]


class TestValidateBackfill:
    def _make_query_api(self, race_name='My Race', actual_cars=None,
                        race_start=None, race_end_epoc=1672531200):
        if actual_cars is None:
            actual_cars = {}
        if race_start is None:
            race_start = datetime(2022, 1, 1, tzinfo=timezone.utc)

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
                table.records = []
                for car, count in actual_cars.items():
                    rec = MagicMock()
                    rec.values = {'car_number': car, '_value': count}
                    table.records.append(rec)
            return [table]

        api = MagicMock()
        api.query.side_effect = fake_query
        return api

    def test_returns_true_when_all_expected_cars_present(self):
        query_api = self._make_query_api(actual_cars={'42': 10, '99': 5})
        result = _mod.validate_backfill([('101', '42'), ('101', '99')], query_api)
        assert result is True

    def test_returns_false_when_a_car_is_missing(self):
        query_api = self._make_query_api(actual_cars={'42': 10})
        result = _mod.validate_backfill([('101', '42'), ('101', '99')], query_api)
        assert result is False

    def test_returns_false_when_race_metadata_missing(self):
        query_api = self._make_query_api(race_name=None, actual_cars={'42': 10})
        result = _mod.validate_backfill([('101', '42')], query_api)
        assert result is False

    def test_logs_race_name_in_output(self, caplog):
        import logging
        query_api = self._make_query_api(race_name='Lemons 2026', actual_cars={'42': 10})
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.validate_backfill([('101', '42')], query_api)
        assert any('Lemons 2026' in r.message for r in caplog.records)

    def test_logs_ok_with_car_numbers_and_lap_counts(self, caplog):
        import logging
        query_api = self._make_query_api(actual_cars={'42': 10, '99': 5})
        with caplog.at_level(logging.INFO, logger='root'):
            _mod.validate_backfill([('101', '42'), ('101', '99')], query_api)
        assert any('OK' in r.message and '42' in r.message and '10' in r.message
                   for r in caplog.records)

    def test_logs_missing_cars(self, caplog):
        import logging
        query_api = self._make_query_api(actual_cars={'42': 10})
        with caplog.at_level(logging.WARNING, logger='root'):
            _mod.validate_backfill([('101', '42'), ('101', '99')], query_api)
        assert any('MISSING' in r.message and '99' in r.message for r in caplog.records)

    def test_laps_not_queried_when_race_metadata_missing(self):
        query_api = self._make_query_api(race_name=None)
        _mod.validate_backfill([('101', '42')], query_api)
        assert query_api.query.call_count == 1

    def test_laps_query_scoped_to_race_time_bounds(self):
        race_start = datetime(2022, 6, 1, tzinfo=timezone.utc)
        query_api = self._make_query_api(actual_cars={'42': 10},
                                         race_start=race_start, race_end_epoc=1672531200)
        _mod.validate_backfill([('101', '42')], query_api)
        laps_flux = query_api.query.call_args_list[1].args[0]
        assert '1970-01-01' not in laps_flux
        assert '2022-06-01' in laps_flux

    def test_races_query_filters_by_end_time_epoc_field(self):
        query_api = self._make_query_api(actual_cars={'42': 10})
        _mod.validate_backfill([('101', '42')], query_api)
        races_flux = query_api.query.call_args_list[0].args[0]
        assert '_field == "end_time_epoc"' in races_flux

    def test_logs_warning_when_end_time_epoc_zero(self, caplog):
        import logging
        query_api = self._make_query_api(actual_cars={'42': 10}, race_end_epoc=0)
        with caplog.at_level(logging.WARNING, logger='root'):
            _mod.validate_backfill([('101', '42')], query_api)
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

    def test_skips_race_already_at_current_schema(self, caplog):
        import logging
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 8},
            std_current_by_race={'101': 8},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api)
        mock_run.assert_not_called()
        assert any('skipping' in r.message and '101' in r.message for r in caplog.records)

    def test_rebackfills_when_laps_current_but_standings_stale(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 8},
            std_current_by_race={'101': 3},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_upgrade_stored(query_api)
        assert mock_run.call_count == 1
        assert mock_run.call_args.args[0] == ['lemongrass', 'laps', '-n', '101']

    def test_rebackfills_when_laps_current_but_standings_missing(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},
            std_total_by_race={'101': 0},
            std_current_by_race={'101': 0},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_upgrade_stored(query_api)
        assert mock_run.call_count == 1

    def test_rebackfills_stale_race(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_upgrade_stored(query_api)
        assert mock_run.call_count == 1
        cmd = mock_run.call_args.args[0]
        assert cmd == ['lemongrass', 'laps', '-n', '101']

    def test_dry_run_does_not_call_subprocess(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            _mod.run_upgrade_stored(query_api, dry_run=True)
        mock_run.assert_not_called()

    def test_skips_race_with_no_stored_laps(self, caplog):
        import logging
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 0},
            current_by_race={},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api)
        mock_run.assert_not_called()

    def test_returns_failed_race_ids(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            failures = _mod.run_upgrade_stored(query_api)
        assert failures == ['101']

    def test_returns_empty_list_on_success(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 5},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            failures = _mod.run_upgrade_stored(query_api)
        assert failures == []

    def test_stops_on_interrupt(self):
        query_api = self._query_api(
            stored_races={'101': 'Race 1', '202': 'Race 2'},
            total_by_race={'101': 5, '202': 5},
            current_by_race={'101': 0, '202': 0},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=130)
            _mod.run_upgrade_stored(query_api)
        assert mock_run.call_count == 1

    def test_force_rebackfills_race_already_at_current_schema(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already fully current
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_upgrade_stored(query_api, force=True)
        assert mock_run.call_count == 1
        cmd = mock_run.call_args.args[0]
        assert cmd == ['lemongrass', 'laps', '-n', '101']

    def test_force_logs_already_current_message(self, caplog):
        import logging
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already fully current
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api, force=True)
        assert any('already current' in r.message and 'force re-backfilling' in r.message
                   for r in caplog.records)

    def test_dry_run_force_does_not_call_subprocess(self, caplog):
        import logging
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 10},
            current_by_race={'101': 10},  # already current; only --force would touch it
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            with caplog.at_level(logging.INFO):
                _mod.run_upgrade_stored(query_api, dry_run=True, force=True)
        mock_run.assert_not_called()
        assert any('would force re-backfill' in r.message for r in caplog.records)

    def test_force_still_skips_race_with_no_stored_laps(self):
        query_api = self._query_api(
            stored_races={'101': 'Lemons 2024'},
            total_by_race={'101': 0},
            current_by_race={},
        )
        with patch.object(_mod.subprocess, 'run') as mock_run:
            _mod.run_upgrade_stored(query_api, force=True)
        mock_run.assert_not_called()


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


class TestArgParsing:
    def test_dry_run_flag(self):
        args = _mod._build_parser().parse_args(['--dry-run'])
        assert args.dry_run is True

    def test_dry_run_default(self):
        args = _mod._build_parser().parse_args([])
        assert args.dry_run is False

    def test_override_single(self):
        args = _mod._build_parser().parse_args(['--override', '12345:253'])
        assert args.overrides == {'12345': '253'}

    def test_override_multiple(self):
        args = _mod._build_parser().parse_args(
            ['--override', '12345:253', '--override', '99999:84'])
        assert args.overrides == {'12345': '253', '99999': '84'}

    def test_override_default_empty(self):
        args = _mod._build_parser().parse_args([])
        assert args.overrides == {}

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

