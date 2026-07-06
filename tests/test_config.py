import textwrap
from pathlib import Path

import pytest

from lemongrass import _config


class TestParseSize:
    @pytest.mark.parametrize("value,expected", [
        (1073741824, 1073741824),      # int bytes passthrough
        ("1024", 1024),                # bare number = bytes
        ("512B", 512),                 # B suffix = bytes
        ("1GiB", 1073741824),          # binary
        ("1 gib", 1073741824),         # whitespace + lowercase
        ("500MiB", 524288000),         # binary
        ("1GB", 1000000000),           # decimal
        ("1.5GB", 1500000000),         # fraction
        (2.0, 2),                      # float floored to int
    ])
    def test_parses_valid(self, value, expected):
        assert _config.parse_size(value) == expected

    @pytest.mark.parametrize("value", ["1XB", "-1GiB", "GiB", "", 0, -5, "0GiB", True,
                                       1.5, 0.5])
    def test_rejects_invalid(self, value):
        # Non-integer floats are rejected rather than truncated: a TOML
        # `max_size = 1.5` (user meant "1.5GiB") must not become a 1-byte cap.
        with pytest.raises(ValueError):
            _config.parse_size(value)


class TestDefaults:
    def test_defaults_match_todays_literals(self, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        c = _config.load_config()
        assert c.influx.url == "https://influxdb.focism.com"
        assert c.influx.org == "focism"
        assert c.influx.token_env == "INFLUX_TELEMETRY_TOKEN"
        assert c.influx.buckets.laps == "laps"
        assert c.influx.buckets.races == "races"
        assert c.influx.buckets.sessions == "race_sessions"
        assert c.influx.buckets.telem == "telem"
        assert c.influx.buckets.pisugar == "pisugar"
        assert c.races.backfill.search_terms == (
            "Real Hoopties", "GP du Lac", "Halloween Hoop")
        assert c.races.backfill.default_car_number == "252"
        assert c.races.backfill.default_start_year == 2017
        assert c.racemonitor.tokens_env == "RACEMONITOR_TOKENS"
        assert c.telem.vin == ""
        assert c.telem.obd.port == "/dev/obd"
        assert c.telem.obd.baudrate == 0
        assert c.telem.obd.debug is False
        assert c.telem.spool.dir == "/data/telem-spool"
        assert c.telem.spool.max_size == 1073741824
        assert c.pisugar.host == ""
        assert c.pisugar.api_url == "http://localhost:8421"
        assert c.pisugar.config_path == "/etc/pisugar-server/config.json"


def _write_cfg(tmp_path, monkeypatch, body):
    p = tmp_path / "lemongrass.toml"
    p.write_text(textwrap.dedent(body))
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(p))
    return p


class TestFileOverlay:
    def test_partial_overlay_keeps_other_defaults(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [influx.buckets]
            laps = "my_laps"
        """)
        c = _config.load_config()
        assert c.influx.buckets.laps == "my_laps"       # overridden
        assert c.influx.buckets.races == "races"        # default kept
        assert c.influx.url == "https://influxdb.focism.com"

    def test_full_sections_overlay(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [influx]
            url = "http://localhost:8086"
            org = "team"
            [races.backfill]
            search_terms = ["Foo", "Bar"]
            default_car_number = "99"
            default_start_year = 2020
            [telem.spool]
            max_size = "2GiB"
        """)
        c = _config.load_config()
        assert c.influx.url == "http://localhost:8086"
        assert c.influx.org == "team"
        assert c.races.backfill.search_terms == ("Foo", "Bar")
        assert c.races.backfill.default_car_number == "99"
        assert c.races.backfill.default_start_year == 2020
        assert c.telem.spool.max_size == 2 * 1024 ** 3


class TestValidation:
    def test_missing_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LEMONGRASS_CONFIG", str(tmp_path / "nope.toml"))
        with pytest.raises(_config.ConfigError, match="could not be read"):
            _config.load_config()

    def test_malformed_toml_raises(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, "this is = = not toml")
        with pytest.raises(_config.ConfigError, match="not valid TOML"):
            _config.load_config()

    def test_unknown_key_raises(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [influx]
            bucket = "typo"
        """)
        with pytest.raises(_config.ConfigError, match="unknown key"):
            _config.load_config()

    def test_wrong_type_raises(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [races.backfill]
            default_start_year = "2017"
        """)
        with pytest.raises(_config.ConfigError, match="must be an integer"):
            _config.load_config()

    @pytest.mark.parametrize("body,section", [
        ("[telem]\nobd = 5", "telem.obd"),                     # scalar where table expected
        ("[[influx.buckets]]\nlaps = 'x'", "influx.buckets"),  # array-of-tables mistake
        ("[influx]\nbuckets = 'laps'", "influx.buckets"),      # string where table expected
        ("influx = 5", "influx"),                              # scalar top-level section
    ])
    def test_non_table_section_raises_config_error(self, tmp_path, monkeypatch,
                                                   body, section):
        _write_cfg(tmp_path, monkeypatch, body)
        with pytest.raises(_config.ConfigError, match=f"{section}.*must be a table"):
            _config.load_config()

    def test_bad_max_size_raises(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [telem.spool]
            max_size = "1XB"
        """)
        with pytest.raises(_config.ConfigError, match="max_size"):
            _config.load_config()

    def test_fractional_max_size_raises(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [telem.spool]
            max_size = 1.5
        """)
        with pytest.raises(_config.ConfigError, match="max_size"):
            _config.load_config()


class TestSecretsOnlyEnv:
    def test_non_secret_env_vars_are_ignored(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [influx]
            url = "http://from-file:8086"
        """)
        # Legacy non-secret env vars no longer affect the loaded config.
        for var, val in [
            ("INFLUX_URL", "http://from-env:9999"),
            ("INFLUX_ORG", "env-org"),
            ("OBD_PORT", "/dev/env-obd"),
            ("OBD_BAUDRATE", "38400"),
            ("OBD_DEBUG", "1"),
            ("CAR_VIN", "ENVVIN"),
            ("TELEM_SPOOL_DIR", "/env/spool"),
            ("TELEM_SPOOL_MAX_SIZE", "512MiB"),
            ("PISUGAR_HOST", "env-host"),
        ]:
            monkeypatch.setenv(var, val)
        c = _config.load_config()
        assert c.influx.url == "http://from-file:8086"   # file wins
        assert c.influx.org == "focism"                  # default kept; env ignored
        assert c.telem.obd.port == "/dev/obd"
        assert c.telem.obd.baudrate == 0
        assert c.telem.obd.debug is False
        assert c.telem.vin == ""
        assert c.telem.spool.dir == "/data/telem-spool"
        assert c.telem.spool.max_size == 1073741824
        assert c.pisugar.host == ""

    def test_buckets_have_no_env_override(self, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        monkeypatch.setenv("BUCKETS_LAPS", "ignored")
        assert _config.load_config().influx.buckets.laps == "laps"


class TestDroppedEnvVarWarning:
    """The secrets-only pivot stopped reading the old non-secret env vars; a
    deployment that still sets them silently falls back to defaults, so startup
    must call that out."""

    def _clear_all(self, monkeypatch):
        for var in _config._DROPPED_ENV_VARS:
            monkeypatch.delenv(var, raising=False)

    def test_warns_for_each_dropped_var_set(self, monkeypatch, capsys):
        self._clear_all(monkeypatch)
        monkeypatch.setenv("OBD_PORT", "/dev/ttyUSB0")
        monkeypatch.setenv("INFLUX_URL", "http://old:8086")
        _config.warn_dropped_env_vars()
        err = capsys.readouterr().err
        assert "OBD_PORT" in err and "telem.obd.port" in err
        assert "INFLUX_URL" in err and "influx.url" in err
        assert "no longer read" in err

    def test_silent_when_no_dropped_vars_set(self, monkeypatch, capsys):
        self._clear_all(monkeypatch)
        _config.warn_dropped_env_vars()
        assert capsys.readouterr().err == ""

    def test_never_released_vars_are_not_warned_about(self, monkeypatch, capsys):
        # CAR_VIN, HOST, and the spool vars were added after v4.0.0 and never
        # shipped in a release, so no deployment can have configured them via
        # env — they don't belong in a migration warning.
        self._clear_all(monkeypatch)
        for var in ("CAR_VIN", "HOST", "TELEM_SPOOL_DIR", "TELEM_SPOOL_MAX_BYTES"):
            monkeypatch.setenv(var, "x")
        _config.warn_dropped_env_vars()
        assert capsys.readouterr().err == ""


def test_sample_file_reproduces_defaults(monkeypatch):
    sample = Path(__file__).resolve().parents[1] / "lemongrass.toml.sample"
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(sample))
    assert _config.load_config() == _config.Config()


def test_sample_file_lists_every_key():
    """Equality with Config() can't catch a newly added field that's simply
    absent from the sample (both sides get the default); require the sample to
    spell out every key explicitly."""
    import dataclasses
    import tomllib

    sample = Path(__file__).resolve().parents[1] / "lemongrass.toml.sample"
    data = tomllib.loads(sample.read_text())

    def assert_covered(dc_value, section, where):
        for f in dataclasses.fields(dc_value):
            key = f"{where}{f.name}"
            assert f.name in section, f"lemongrass.toml.sample is missing {key}"
            child = getattr(dc_value, f.name)
            if dataclasses.is_dataclass(child):
                assert_covered(child, section[f.name], f"{key}.")

    assert_covered(_config.Config(), data, "")
