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
        mock_main = MagicMock(return_value=None)
        with patch.object(sys, 'argv', ['lemongrass', 'laps', '--help']):
            with patch('lemongrass.laps.main', mock_main):
                with pytest.raises(SystemExit) as exc:
                    cli.main()
        assert exc.value.code is None
        mock_main.assert_called_once()

    def test_routes_to_telem(self):
        mock_main = MagicMock(return_value=None)
        with patch.object(sys, 'argv', ['lemongrass', 'telem']):
            with patch('lemongrass.telem.main', mock_main):
                with pytest.raises(SystemExit):
                    cli.main()
        mock_main.assert_called_once()

    def test_shifts_argv_for_subcommand(self):
        """Subcommand sees its own args at sys.argv[1], not the subcommand name."""
        captured = {}

        def capture_main():
            captured['argv'] = sys.argv[:]

        with patch.object(sys, 'argv', ['lemongrass', 'race-diagnose', 'R001', '42']):
            with patch('lemongrass.race_diagnose.main', capture_main):
                with pytest.raises(SystemExit):
                    cli.main()

        assert captured['argv'][1] == 'R001'
        assert captured['argv'][2] == '42'


class TestInfluxErrorHandling:
    def test_api_exception_exits_1_with_clean_message(self, capsys):
        from influxdb_client.rest import ApiException

        exc = ApiException(status=530)
        exc.body = (
            '{"title":"Error 1033: Cloudflare Tunnel error",'
            '"detail":"The host is currently unable to be reached."}'
        )

        def raise_api():
            raise exc

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_api):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        err = capsys.readouterr().err
        assert 'cannot reach InfluxDB' in err
        assert '530' in err
        assert 'host is currently unable to be reached' in err

    def test_connection_error_exits_1_with_clean_message(self, capsys):
        from urllib3.exceptions import MaxRetryError

        def raise_conn():
            raise MaxRetryError(pool=None, url='http://localhost:8086', reason='refused')

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_conn):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        assert 'cannot reach InfluxDB' in capsys.readouterr().err

    def test_status_zero_reported_as_unreachable(self, capsys):
        """The rest layer turns a urllib3 SSLError into ApiException(status=0);
        that's a connectivity failure, so it must read 'cannot reach', not 'request failed'."""
        from influxdb_client.rest import ApiException

        def raise_ssl():
            raise ApiException(status=0, reason='SSLError\nbad handshake')

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_ssl):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        err = capsys.readouterr().err
        assert 'cannot reach InfluxDB' in err
        assert 'request failed' not in err
        # Bodyless ApiExceptions carry their reason in .reason; surface it (collapsed
        # to one line) rather than printing a bare "HTTP 0".
        assert 'SSLError bad handshake' in err

    def test_normal_systemexit_passes_through(self):
        """A command exiting 0 must not be swallowed or rewritten by the handler."""
        def clean_exit():
            sys.exit(0)

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', clean_exit):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 0

    def test_4xx_reported_as_request_failed_not_unreachable(self, capsys):
        """A 4xx (e.g. bad token) is reached-but-rejected, so it must NOT be
        labeled 'cannot reach' — but still exits cleanly without a traceback."""
        from influxdb_client.rest import ApiException

        exc = ApiException(status=401)
        exc.body = '{"message":"unauthorized access"}'

        def raise_api():
            raise exc

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_api):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        err = capsys.readouterr().err
        assert 'cannot reach InfluxDB' not in err
        assert 'request failed' in err
        assert '401' in err
        # InfluxDB 2.x puts the human-readable reason under "message"; surface it.
        assert 'unauthorized access' in err

    def test_4xx_message_field_is_surfaced(self, capsys):
        """InfluxDB 2.x API errors (404 bucket, 422 write) carry their reason in
        the JSON 'message' field, not 'detail'/'title'."""
        from influxdb_client.rest import ApiException

        exc = ApiException(status=404)
        exc.body = '{"code":"not found","message":"bucket \\"laps\\" not found"}'

        def raise_api():
            raise exc

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_api):
                with pytest.raises(SystemExit):
                    cli.main()
        err = capsys.readouterr().err
        assert 'bucket "laps" not found' in err


class TestExitCodePropagation:
    def test_subcommand_return_value_becomes_exit_code(self):
        """laps.main() returns 1 on failure; the dispatcher must not swallow it."""
        with patch.object(sys, 'argv', ['lemongrass', 'laps', '123']):
            with patch('lemongrass.laps.main', return_value=1):
                with pytest.raises(SystemExit) as exc:
                    cli.main()
        assert exc.value.code == 1

    def test_none_return_exits_zero(self):
        with patch.object(sys, 'argv', ['lemongrass', 'laps', '123']):
            with patch('lemongrass.laps.main', return_value=None):
                with pytest.raises(SystemExit) as exc:
                    cli.main()
        assert exc.value.code is None  # sys.exit(None) == process exit status 0
