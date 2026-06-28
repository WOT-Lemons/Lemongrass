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


def test_env_overrides(monkeypatch):
    monkeypatch.setenv('INFLUX_URL', 'http://localhost:8086')
    monkeypatch.setenv('INFLUX_ORG', 'lemongrass')
    reloaded = importlib.reload(influx_mod)
    assert reloaded.INFLUX_URL == 'http://localhost:8086'
    assert reloaded.INFLUX_ORG == 'lemongrass'
    monkeypatch.delenv('INFLUX_URL', raising=False)
    monkeypatch.delenv('INFLUX_ORG', raising=False)
    importlib.reload(influx_mod)  # restore default module state for later tests


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
