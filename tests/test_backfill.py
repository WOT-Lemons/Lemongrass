import importlib.util
import pathlib
from unittest.mock import MagicMock, call, patch

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

    def test_validate_default_false(self):
        args = _mod._build_parser().parse_args([])
        assert args.validate is False
