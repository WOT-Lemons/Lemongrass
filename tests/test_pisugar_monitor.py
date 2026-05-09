import base64
import importlib.util
import json
import pathlib
from unittest.mock import mock_open, patch

_spec = importlib.util.spec_from_file_location(
  "pisugar_monitor",
  pathlib.Path(__file__).parent.parent / "pisugar-monitor.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

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
    # Payload lengths that require 0, 1, and 2 padding chars
    for extra in ["", "x", "xy"]:
      payload = {"exp": 9999, "pad": extra}
      assert token_expiry(_make_jwt(payload)) == 9999


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
