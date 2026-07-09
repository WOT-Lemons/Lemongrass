import base64
import json
import logging
from contextlib import ExitStack
from unittest.mock import MagicMock, mock_open, patch
from urllib.error import HTTPError, URLError

import pytest

import lemongrass._influx as _influx
import lemongrass.pisugar_monitor as _mod

token_expiry = _mod.token_expiry
read_credentials = _mod.read_credentials


def _make_jwt(payload):
    header = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').rstrip(b'=').decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b'=').decode()
    return f"{header}.{body}.fakesig"


class TestTokenExpiry:
    def test_valid_jwt_returns_exp(self):
        assert token_expiry(_make_jwt({"sub": "admin", "exp": 1778339746})) == 1778339746

    def test_missing_exp_returns_none(self):
        assert token_expiry(_make_jwt({"sub": "admin"})) is None

    def test_invalid_token_returns_none(self):
        assert token_expiry("notajwt") is None

    def test_empty_string_returns_none(self):
        assert token_expiry("") is None

    @pytest.mark.parametrize("extra", ["", "x", "xy"])
    def test_padding_handled(self, extra):
        payload = {"exp": 9999, "pad": extra}
        assert token_expiry(_make_jwt(payload)) == 9999

    def test_base64url_payload_decoded(self):
        """JWT payloads are base64url; b64decode silently drops '-'/'_' and
        corrupts the payload, permanently disabling proactive refresh."""
        payload = {"exp": 4102444800, "k": ">>>"}
        body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        assert "-" in body or "_" in body  # guard: must exercise base64url chars
        assert token_expiry(f"header.{body}.sig") == 4102444800


class TestReadCredentials:
    def test_returns_credentials_from_config(self):
        data = json.dumps({"auth_user": "admin", "auth_password": "secret"})
        with patch("builtins.open", mock_open(read_data=data)):
            assert read_credentials() == ("admin", "secret")

    def test_missing_file_returns_none(self):
        with patch("builtins.open", side_effect=FileNotFoundError):
            assert read_credentials() == (None, None)

    def test_invalid_json_returns_none(self):
        with patch("builtins.open", mock_open(read_data="not json")):
            assert read_credentials() == (None, None)

    def test_missing_keys_returns_none(self):
        data = json.dumps({"other": "value"})
        with patch("builtins.open", mock_open(read_data=data)):
            assert read_credentials() == (None, None)


class TestHttpTimeouts:
    def test_login_passes_timeout(self):
        with patch.object(_mod.urllib.request, "urlopen") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = b"tok"
            _mod.login("u", "p")
        assert mock_open.call_args.kwargs["timeout"] == _mod.HTTP_TIMEOUT_S

    def test_exec_command_passes_timeout(self):
        with patch.object(_mod.urllib.request, "urlopen") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = b"battery: 84.5"
            _mod.exec_command("get battery")
        assert mock_open.call_args.kwargs["timeout"] == _mod.HTTP_TIMEOUT_S


class TestMain:
    def test_honors_configured_token_env_var(self, monkeypatch, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text('[influx]\ntoken_env = "MY_INFLUX_TOKEN"\n')
        monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
        monkeypatch.delenv("INFLUX_TELEMETRY_TOKEN", raising=False)
        monkeypatch.delenv("MY_INFLUX_TOKEN", raising=False)
        login = MagicMock()
        with patch.object(_mod, 'login', login), \
                patch.object(_mod, 'read_credentials', return_value=('admin', 'secret')):
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        assert exc.value.code == 1
        login.assert_not_called()


class TestExecCommandCoercion:
    def test_raw_mode_preserves_version_strings(self):
        """Firmware '1.10' must not become the float 1.1 in device tags."""
        with patch.object(_mod.urllib.request, "urlopen") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = \
                b"firmware_version: 1.10"
            assert _mod.exec_command("get firmware_version", coerce=False) == "1.10"

    def test_default_mode_still_coerces_floats(self):
        with patch.object(_mod.urllib.request, "urlopen") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = \
                b"battery: 84.5"
            assert _mod.exec_command("get battery") == 84.5

    @pytest.mark.parametrize("raw,expected", [
        (b"battery_charging: true", True),
        (b"battery_power_plugged: false", False),
    ])
    def test_coerces_booleans(self, raw, expected):
        with patch.object(_mod.urllib.request, "urlopen") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = raw
            assert _mod.exec_command("get battery_charging") is expected


class TestStartupConnect:
    def test_retries_until_server_reachable(self):
        """The monitor often boots before pisugar-server binds :8421; it must
        retry instead of dying with a traceback."""
        with patch.object(_mod, "login", side_effect=[URLError("refused"), "tok"]) as mock_login:
            with patch.object(_mod, "exec_command", return_value="PiSugar 3"):
                with patch.object(_mod, "sleep") as mock_sleep:
                    token, tags = _mod._startup_connect("u", "p")
        assert token == "tok"
        assert mock_login.call_count == 2
        mock_sleep.assert_called_once_with(_mod.STARTUP_RETRY_DELAY_S)
        # Device tags read raw (coerce=False) so "PiSugar 3" isn't numerically mangled.
        assert tags == {"server_version": "PiSugar 3", "model": "PiSugar 3",
                        "firmware_version": "PiSugar 3"}

    def test_no_credentials_skips_login(self):
        with patch.object(_mod, "login") as mock_login:
            with patch.object(_mod, "exec_command", return_value="PiSugar 3"):
                token, _tags = _mod._startup_connect(None, None)
        assert token is None
        mock_login.assert_not_called()


class TestResolveHost:
    def test_uses_configured_host_when_set(self, monkeypatch, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text('[pisugar]\nhost = "car-lemongrass-pi"\n')
        monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
        assert _mod._resolve_host() == "car-lemongrass-pi"

    def test_falls_back_to_gethostname(self, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        with patch.object(_mod.socket, "gethostname", return_value="fallback-host"):
            assert _mod._resolve_host() == "fallback-host"


class TestWritePointsBucket:
    def test_writes_to_pisugar_bucket(self):
        write_api = MagicMock()
        _mod.write_points(write_api, [_mod.build_point("pisugar-temperature", 42.0)])
        assert write_api.write.call_args.kwargs["bucket"] == _influx.BUCKET_PISUGAR

    def test_build_point_carries_host_tag_and_no_vin(self):
        point = _mod.build_point("pisugar-battery-level", 80, {"host": "pi-1"})
        assert point._tags == {"host": "pi-1"}
        assert "vin" not in point._tags

    def test_write_failure_is_logged_and_swallowed(self, caplog):
        """A rejected batch (e.g. a malformed reading) must be logged and
        swallowed, not propagated -- the next 0.5s tick re-reads fresh values."""
        caplog.set_level(logging.INFO)
        write_api = MagicMock()
        write_api.write.side_effect = RuntimeError("bucket rejected the write")
        _mod.write_points(write_api, [_mod.build_point("pisugar-temperature", 42.0)])
        assert "Failed to write" in caplog.text


class _StopLoopError(Exception):
    """Sentinel raised from a patched sleep() to break main()'s `while True`
    after exactly one tick."""


class TestMainLoop:
    def _drive_one_tick(self, monkeypatch, exec_command, *, credentials=(None, None),
                        token_expiry_ret=None, startup_token=None, login=None):
        """Run main() through a single loop iteration, then break via _StopLoopError.

        `exec_command` is passed straight to the mock's side_effect: a list feeds
        one value per read, an exception instance raises on the read path.
        """
        monkeypatch.setenv("INFLUX_TELEMETRY_TOKEN", "influx-tok")
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        connect_cm = MagicMock()
        write_api = connect_cm.__enter__.return_value.write_api.return_value
        login = login or MagicMock(return_value="new-token")
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(_mod, "read_credentials", return_value=credentials))
            stack.enter_context(
                patch.object(_mod, "_startup_connect",
                             return_value=(startup_token, {"model": "PiSugar 3"})))
            stack.enter_context(patch.object(_mod, "_resolve_host", return_value="pi-1"))
            stack.enter_context(
                patch.object(_mod, "token_expiry", return_value=token_expiry_ret))
            stack.enter_context(patch.object(_mod, "login", login))
            stack.enter_context(patch.object(_mod._influx, "connect",
                                             return_value=connect_cm))
            ec = stack.enter_context(
                patch.object(_mod, "exec_command", side_effect=exec_command))
            stack.enter_context(patch.object(_mod, "sleep", side_effect=_StopLoopError))
            with pytest.raises(_StopLoopError):
                _mod.main()
        return {"write_api": write_api, "login": login, "exec_command": ec}

    def test_one_tick_reads_six_and_writes(self, monkeypatch):
        r = self._drive_one_tick(
            monkeypatch,
            exec_command=[True, 0.1, 80.0, True, 4.2, 25.0],
        )
        assert r["exec_command"].call_count == 6
        assert r["write_api"].write.call_count == 1
        assert len(r["write_api"].write.call_args.kwargs["record"]) == 6

    def test_401_read_triggers_reauthentication(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        login = MagicMock(return_value="fresh-token")
        self._drive_one_tick(
            monkeypatch,
            exec_command=HTTPError("http://pi", 401, "unauthorized", {}, None),
            credentials=("admin", "secret"),
            startup_token="old-token",
            token_expiry_ret=None,  # skip proactive refresh; exercise the 401 path
            login=login,
        )
        login.assert_called_once_with("admin", "secret")
        assert "re-authenticating" in caplog.text

    def test_proactive_refresh_near_expiry(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        login = MagicMock(return_value="fresh-token")
        self._drive_one_tick(
            monkeypatch,
            exec_command=[True, 0.1, 80.0, True, 4.2, 25.0],
            credentials=("admin", "secret"),
            startup_token="old-token",
            token_expiry_ret=1,  # exp far in the past -> now() past the refresh margin
            login=login,
        )
        login.assert_called_once_with("admin", "secret")
        assert "nearing expiry" in caplog.text

    def test_generic_read_error_is_logged_and_swallowed(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        self._drive_one_tick(
            monkeypatch,
            exec_command=ValueError("garbled pisugar response"),
        )
        assert "Error reading from PiSugar" in caplog.text
