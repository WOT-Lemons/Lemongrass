import importlib

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
    importlib.reload(influx_mod)  # restore module-level defaults for other tests


def test_retry_policy_shape():
    assert isinstance(influx_mod.INFLUX_RETRIES, Retry)
    assert influx_mod.INFLUX_RETRIES.total == 3
    assert 530 in influx_mod.INFLUX_RETRIES.status_forcelist
    assert influx_mod.INFLUX_RETRIES.respect_retry_after_header is False
