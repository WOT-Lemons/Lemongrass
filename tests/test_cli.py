import sys
from unittest.mock import MagicMock, patch

import pytest

import lemongrass.cli as cli


class TestDispatcher:
    def test_no_args_exits_nonzero(self):
        with patch.object(sys, 'argv', ['lemongrass']):
            with pytest.raises(SystemExit) as exc:
                cli.main()
        assert exc.value.code != 0

    def test_unknown_command_exits_nonzero(self):
        with patch.object(sys, 'argv', ['lemongrass', 'notacommand']):
            with pytest.raises(SystemExit) as exc:
                cli.main()
        assert exc.value.code != 0

    def test_routes_to_laps(self):
        mock_main = MagicMock()
        with patch.object(sys, 'argv', ['lemongrass', 'laps', '--help']):
            with patch('lemongrass.laps.main', mock_main):
                cli.main()
        mock_main.assert_called_once()

    def test_routes_to_telem(self):
        mock_main = MagicMock()
        with patch.object(sys, 'argv', ['lemongrass', 'telem']):
            with patch('lemongrass.telem.main', mock_main):
                cli.main()
        mock_main.assert_called_once()

    def test_shifts_argv_for_subcommand(self):
        """Subcommand sees its own args at sys.argv[1], not the subcommand name."""
        captured = {}

        def capture_main():
            captured['argv'] = sys.argv[:]

        with patch.object(sys, 'argv', ['lemongrass', 'race-diagnose', 'R001', '42']):
            with patch('lemongrass.race_diagnose.main', capture_main):
                cli.main()

        assert captured['argv'][1] == 'R001'
        assert captured['argv'][2] == '42'
