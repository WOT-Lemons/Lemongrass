import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

import lemongrass.cli as cli


class TestConfigErrorHandling:
    """A broken LEMONGRASS_CONFIG must surface as a one-line error, not a
    traceback — and must not take down `--help`, which needs no config. Runs in
    a subprocess because the failure happens at module import time."""

    def _run(self, args, tmp_path):
        env = dict(os.environ, LEMONGRASS_CONFIG=str(tmp_path / "nope.toml"))
        return subprocess.run(
            [sys.executable, "-c", "from lemongrass.cli import main; main()", *args],
            capture_output=True, text=True, env=env, timeout=60)

    def test_help_works_with_broken_config(self, tmp_path):
        res = self._run(["--help"], tmp_path)
        assert res.returncode == 0
        assert "Usage:" in res.stdout
        assert "Traceback" not in res.stderr

    def test_command_reports_config_error_without_traceback(self, tmp_path):
        res = self._run(["race-diagnose", "1", "2"], tmp_path)
        assert res.returncode == 1
        assert "Error:" in res.stderr
        assert "nope.toml" in res.stderr
        assert "Traceback" not in res.stderr


class TestDispatcher:
    def test_warns_about_dropped_env_vars_at_startup(self, monkeypatch, capsys):
        monkeypatch.setenv("OBD_PORT", "/dev/ttyUSB0")
        monkeypatch.setattr(cli.sys.stdin, 'isatty', lambda: False)
        with patch.object(sys, 'argv', ['lemongrass']):
            with pytest.raises(SystemExit):
                cli.main()
        assert "OBD_PORT" in capsys.readouterr().err

    @pytest.mark.parametrize("argv", [
        ['lemongrass'],                    # no command
        ['lemongrass', 'notacommand'],     # unknown command
    ])
    def test_missing_or_unknown_command_exits_nonzero(self, argv, monkeypatch):
        # A bare invocation only launches the home TUI on a TTY (see
        # test_bare_tty_launches_home); force non-TTY here so this exercises
        # the usage/exit-1 path regardless of how tests are run.
        monkeypatch.setattr(cli.sys.stdin, 'isatty', lambda: False)
        with patch.object(sys, 'argv', argv):
            with pytest.raises(SystemExit) as exc:
                cli.main()
        assert exc.value.code != 0

    def test_bare_tty_launches_home(self, monkeypatch):
        monkeypatch.setattr(cli.sys, 'argv', ['lemongrass'])
        monkeypatch.setattr(cli.sys.stdin, 'isatty', lambda: True)
        monkeypatch.setattr(cli.sys.stdout, 'isatty', lambda: True)
        with patch('lemongrass._env.resolve_tokens', return_value='tok'), \
             patch('lemongrass._home_tui.run_home_tui', return_value=0) as run, \
             patch('race_monitor.RaceMonitorClient'):
            with patch.object(cli.sys, 'exit', side_effect=SystemExit) as exit_:
                with pytest.raises(SystemExit):
                    cli.main()
        run.assert_called_once()
        exit_.assert_called_with(0)

    def test_bare_non_tty_prints_usage(self, monkeypatch, capsys):
        monkeypatch.setattr(cli.sys, 'argv', ['lemongrass'])
        monkeypatch.setattr(cli.sys.stdin, 'isatty', lambda: False)
        with patch.object(cli.sys, 'exit', side_effect=SystemExit) as exit_:
            with pytest.raises(SystemExit):
                cli.main()
        assert 'Usage:' in capsys.readouterr().out
        exit_.assert_called_with(1)

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


class TestRaceMonitorErrors:
    def test_429_exhaustion_exits_1_with_rate_limit_message(self, capsys):
        """race-monitor 0.7.0 raises RaceMonitorHTTPError(429) once retries are
        exhausted; the CLI must report it cleanly, not crash with a traceback."""
        from race_monitor import RaceMonitorHTTPError

        def raise_429():
            raise RaceMonitorHTTPError(429, "Too Many Requests")

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_429):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        err = capsys.readouterr().err
        assert 'rate limit' in err.lower()
        assert '429' in err

    def test_other_http_error_exits_1_with_status_and_body(self, capsys):
        """A non-429 HTTP error prints the status line and, below it, the
        response body so the operator sees the server's reason."""
        from race_monitor import RaceMonitorHTTPError

        def raise_500():
            raise RaceMonitorHTTPError(500, "upstream database unavailable")

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_500):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        err = capsys.readouterr().err
        assert '500' in err
        assert 'rate limit' not in err.lower()
        assert 'upstream database unavailable' in err

    def test_base_error_exits_1_without_traceback(self, capsys):
        from race_monitor import RaceMonitorError

        def raise_base():
            raise RaceMonitorError("no tokens configured")

        with patch.object(sys, 'argv', ['lemongrass', 'races', 'list']):
            with patch('lemongrass.races.main', raise_base):
                with pytest.raises(SystemExit) as wrapped:
                    cli.main()
        assert wrapped.value.code == 1
        assert 'no tokens configured' in capsys.readouterr().err


def test_db_is_a_registered_command():
    from lemongrass import cli
    assert cli._COMMANDS["db"] == "lemongrass.db"


def _run_cli_raising(monkeypatch, capsys, tmp_path, exc_to_raise):
    """Run cli.main() with a stubbed command that raises, returning (code, stderr).

    The [postgres] endpoint is pinned so the caller can assert the CLI reports the
    host and port it actually tried.
    """
    import sys as _sys

    import pytest as _pytest

    from lemongrass import cli

    cfg = tmp_path / "lemongrass.toml"
    cfg.write_text('[postgres]\nhost = "db.example"\nport = 6543\n')
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))

    class _Mod:
        @staticmethod
        def main():
            raise exc_to_raise

    monkeypatch.setattr(cli.importlib, "import_module", lambda name: _Mod)
    monkeypatch.setattr(_sys, "argv", ["lemongrass", "db", "upgrade"])
    with _pytest.raises(SystemExit) as exc:
        cli.main()
    return exc.value.code, capsys.readouterr().err


class _PgError(Exception):
    """Stand-in for a psycopg error, whose sqlstate is what the CLI branches on."""

    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.sqlstate = sqlstate


def test_operational_error_reports_cleanly(monkeypatch, capsys, tmp_path):
    from sqlalchemy.exc import OperationalError

    # A connection-phase failure: psycopg leaves sqlstate unset.
    orig = _PgError("connection refused", None)
    code, err = _run_cli_raising(
        monkeypatch, capsys, tmp_path, OperationalError("SELECT 1", {}, orig))
    assert code == 1
    assert "cannot reach PostgreSQL at db.example:6543" in err
    assert "connection refused" in err


def test_operational_error_after_connecting_is_not_reported_as_unreachable(
        monkeypatch, capsys, tmp_path):
    from sqlalchemy.exc import OperationalError

    # The server accepted the connection and then cancelled the statement
    # (SQLSTATE 57014). Calling that "cannot reach PostgreSQL" would be wrong.
    orig = _PgError("canceling statement due to statement timeout", "57014")
    code, err = _run_cli_raising(
        monkeypatch, capsys, tmp_path, OperationalError("SELECT 1", {}, orig))
    assert code == 1
    assert "cannot reach" not in err
    assert "PostgreSQL request failed at db.example:6543" in err
    assert "statement timeout" in err


def test_missing_schema_points_at_db_upgrade(monkeypatch, capsys, tmp_path):
    from sqlalchemy.exc import ProgrammingError

    # The first-run state of a fresh deploy: the server is up and the database
    # exists, but `db upgrade` has not been run. 42P01 is a ProgrammingError,
    # not an OperationalError, so without its own branch it is a traceback.
    orig = _PgError('relation "races" does not exist', "42P01")
    code, err = _run_cli_raising(
        monkeypatch, capsys, tmp_path,
        ProgrammingError("SELECT * FROM races", {}, orig))
    assert code == 1
    assert "lemongrass db upgrade" in err
    assert 'relation "races" does not exist' in err


def test_other_programming_errors_are_not_swallowed(monkeypatch, capsys, tmp_path):
    # A genuine SQL bug (42703 undefined_column) must keep its traceback
    # rather than be mislabelled as a missing schema.
    from sqlalchemy.exc import ProgrammingError

    orig = _PgError('column "nope" does not exist', "42703")
    with pytest.raises(ProgrammingError):
        _run_cli_raising(monkeypatch, capsys, tmp_path,
                         ProgrammingError("SELECT nope", {}, orig))


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
