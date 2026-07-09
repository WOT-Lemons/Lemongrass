import importlib
from unittest import mock

import pytest
from urllib3 import Retry

import lemongrass._influx as influx_mod


@pytest.fixture
def reload_influx(monkeypatch):
    """Reload _influx (which snapshots the config at import time) and restore the
    default module state on teardown, so an env-mutating reload can't leak into
    later tests regardless of ordering. Yields a callable that performs the reload
    and returns the reloaded module."""
    yield lambda: importlib.reload(influx_mod)
    for var in ("LEMONGRASS_CONFIG", "INFLUX_URL", "INFLUX_ORG"):
        monkeypatch.delenv(var, raising=False)
    importlib.reload(influx_mod)


def test_env_does_not_override_influx_constants(monkeypatch, reload_influx):
    monkeypatch.delenv('LEMONGRASS_CONFIG', raising=False)
    monkeypatch.setenv('INFLUX_URL', 'http://localhost:8086')
    monkeypatch.setenv('INFLUX_ORG', 'lemongrass')
    reloaded = reload_influx()
    assert reloaded.INFLUX_URL == 'https://influxdb.focism.com'
    assert reloaded.INFLUX_ORG == 'focism'


@pytest.mark.parametrize("retries,total", [
    (influx_mod.INFLUX_RETRIES, 3),      # the shared module-level default
    (influx_mod.build_retries(1), 1),    # a trimmed budget shares the same policy
])
def test_build_retries_shares_transient_policy(retries, total):
    assert isinstance(retries, Retry)
    assert retries.total == total
    assert retries.status_forcelist == [429, 502, 503, 504, 530]
    assert retries.respect_retry_after_header is False
    assert retries.allowed_methods is None
    assert retries.backoff_max == 10


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


def test_connect_reads_token_from_configured_env_var(monkeypatch, tmp_path, reload_influx):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[influx]\ntoken_env = "MY_TOKEN"\n')
    monkeypatch.setenv('LEMONGRASS_CONFIG', str(cfg))
    monkeypatch.delenv('INFLUX_TELEMETRY_TOKEN', raising=False)
    monkeypatch.setenv('MY_TOKEN', 'via-directive')
    reloaded = reload_influx()
    with mock.patch('influxdb_client.InfluxDBClient') as client_cls:
        reloaded.connect()
    assert client_cls.call_args.kwargs['token'] == 'via-directive'


class TestInvalidFluxIds:
    @pytest.mark.parametrize("values,expected", [
        (['144185', 'car_25-2'], []),                     # clean ids pass
        (['ok', 'bad"id', 'a b'], ['bad"id', 'a b']),     # metacharacters rejected
        ([144185], []),                                   # non-strings coerced
    ])
    def test_invalid_flux_ids(self, values, expected):
        assert influx_mod.invalid_flux_ids(values) == expected
