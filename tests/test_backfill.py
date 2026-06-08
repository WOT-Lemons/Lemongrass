import importlib.util
import pathlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

_spec = importlib.util.spec_from_file_location(
    "backfill",
    pathlib.Path(__file__).parent.parent / "backfill.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


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

    def test_subprocess_uses_absolute_path_to_laps_py(self):
        with patch.object(_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _mod.run_backfill(self._races()[:1], default_car='252', overrides={})
        laps_path = mock_run.call_args.args[0][1]
        assert pathlib.Path(laps_path).is_absolute()
        assert laps_path.endswith('laps.py')

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

