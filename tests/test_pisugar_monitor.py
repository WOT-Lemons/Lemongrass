import base64
import json
from unittest.mock import MagicMock, mock_open, patch
from urllib.error import URLError

import pytest

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

    def test_padding_handled(self):
        for extra in ["", "x", "xy"]:
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
    def test_missing_influx_token_exits_before_pisugar_login(self):
        """A missing INFLUX_TELEMETRY_TOKEN must fail fast: exit before attempting
        a pisugar login (a network call)."""
        login = MagicMock()
        with patch.dict('os.environ', {}, clear=True):
            with patch.object(_mod, 'login', login):
                with patch.object(_mod, 'read_credentials',
                                  return_value=('admin', 'secret')):
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
        assert tags == {"server_version": "PiSugar 3", "model": "PiSugar 3",
                        "firmware_version": "PiSugar 3"}

    def test_no_credentials_skips_login(self):
        with patch.object(_mod, "login") as mock_login:
            with patch.object(_mod, "exec_command", return_value="PiSugar 3"):
                token, _tags = _mod._startup_connect(None, None)
        assert token is None
        mock_login.assert_not_called()
