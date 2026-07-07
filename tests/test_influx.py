import importlib
from unittest import mock

import pytest
from urllib3 import Retry

import lemongrass._influx as influx_mod


def test_defaults_match_prod(monkeypatch):
    monkeypatch.delenv('INFLUX_URL', raising=False)
    monkeypatch.delenv('INFLUX_ORG', raising=False)
    reloaded = importlib.reload(influx_mod)
    assert reloaded.INFLUX_URL == 'https://influxdb.focism.com'
    assert reloaded.INFLUX_ORG == 'focism'


def test_env_does_not_override_influx_constants(monkeypatch):
    monkeypatch.delenv('LEMONGRASS_CONFIG', raising=False)
    monkeypatch.setenv('INFLUX_URL', 'http://localhost:8086')
    monkeypatch.setenv('INFLUX_ORG', 'lemongrass')
    reloaded = importlib.reload(influx_mod)
    assert reloaded.INFLUX_URL == 'https://influxdb.focism.com'
    assert reloaded.INFLUX_ORG == 'focism'
    monkeypatch.delenv('INFLUX_URL', raising=False)
    monkeypatch.delenv('INFLUX_ORG', raising=False)
    importlib.reload(influx_mod)  # restore default module state for later tests


def test_bucket_names():
    assert influx_mod.BUCKET_LAPS == 'laps'
    assert influx_mod.BUCKET_RACES == 'races'
    assert influx_mod.BUCKET_SESSIONS == 'race_sessions'


def test_retry_policy_shape():
    assert isinstance(influx_mod.INFLUX_RETRIES, Retry)
    assert influx_mod.INFLUX_RETRIES.total == 3
    assert 530 in influx_mod.INFLUX_RETRIES.status_forcelist
    assert influx_mod.INFLUX_RETRIES.respect_retry_after_header is False
    assert influx_mod.INFLUX_RETRIES.allowed_methods is None
    assert influx_mod.INFLUX_RETRIES.backoff_max == 10


def test_connect_exits_when_token_missing(monkeypatch, caplog):
    monkeypatch.delenv('INFLUX_TELEMETRY_TOKEN', raising=False)
    with pytest.raises(SystemExit) as exc_info:
        influx_mod.connect()
    assert exc_info.value.code == 1
    assert 'INFLUX_TELEMETRY_TOKEN' in caplog.text


def test_connect_builds_client_with_shared_settings(monkeypatch):
    monkeypatch.setenv('INFLUX_TELEMETRY_TOKEN', 'secret-token')
    with mock.patch('influxdb_client.InfluxDBClient') as client_cls:
        result = influx_mod.connect()
    assert result is client_cls.return_value
    client_cls.assert_called_once_with(
        url=influx_mod.INFLUX_URL,
        token='secret-token',
        org=influx_mod.INFLUX_ORG,
        retries=influx_mod.INFLUX_RETRIES,
    )


def test_connect_omits_timeout_by_default(monkeypatch):
    """Batch callers keep the library default timeout; connect() must not pass a
    timeout kwarg unless one is explicitly requested."""
    monkeypatch.setenv('INFLUX_TELEMETRY_TOKEN', 'secret-token')
    with mock.patch('influxdb_client.InfluxDBClient') as client_cls:
        influx_mod.connect()
    assert 'timeout' not in client_cls.call_args.kwargs


def test_connect_forwards_timeout_and_retries(monkeypatch):
    monkeypatch.setenv('INFLUX_TELEMETRY_TOKEN', 'secret-token')
    retries = influx_mod.build_retries(1)
    with mock.patch('influxdb_client.InfluxDBClient') as client_cls:
        influx_mod.connect(timeout=3000, retries=retries)
    client_cls.assert_called_once_with(
        url=influx_mod.INFLUX_URL,
        token='secret-token',
        org=influx_mod.INFLUX_ORG,
        retries=retries,
        timeout=3000,
    )


def test_build_retries_shares_transient_policy_with_fewer_attempts():
    trimmed = influx_mod.build_retries(1)
    assert isinstance(trimmed, Retry)
    assert trimmed.total == 1
    # Same transient-error semantics as the shared default, only fewer attempts.
    assert trimmed.status_forcelist == influx_mod.INFLUX_RETRIES.status_forcelist
    assert trimmed.respect_retry_after_header is False
    assert trimmed.allowed_methods is None
    assert trimmed.backoff_max == 10


def test_connect_reads_token_from_configured_env_var(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[influx]\ntoken_env = "MY_TOKEN"\n')
    monkeypatch.setenv('LEMONGRASS_CONFIG', str(cfg))
    monkeypatch.delenv('INFLUX_TELEMETRY_TOKEN', raising=False)
    monkeypatch.setenv('MY_TOKEN', 'via-directive')
    reloaded = importlib.reload(influx_mod)
    with mock.patch('influxdb_client.InfluxDBClient') as client_cls:
        reloaded.connect()
    assert client_cls.call_args.kwargs['token'] == 'via-directive'
    monkeypatch.delenv('LEMONGRASS_CONFIG', raising=False)
    importlib.reload(influx_mod)  # restore default module state for later tests


class TestInvalidFluxIds:
    def test_clean_ids_pass(self):
        from lemongrass import _influx
        assert _influx.invalid_flux_ids(['144185', 'car_25-2']) == []

    def test_metacharacters_rejected(self):
        from lemongrass import _influx
        assert _influx.invalid_flux_ids(['ok', 'bad"id', 'a b']) == ['bad"id', 'a b']

    def test_non_strings_coerced(self):
        from lemongrass import _influx
        assert _influx.invalid_flux_ids([144185]) == []
