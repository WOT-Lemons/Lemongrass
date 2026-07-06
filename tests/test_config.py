import textwrap

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

    @pytest.mark.parametrize("value", ["1XB", "-1GiB", "GiB", "", 0, -5, "0GiB", True])
    def test_rejects_invalid(self, value):
        with pytest.raises(ValueError):
            _config.parse_size(value)


class TestDefaults:
    def test_defaults_match_todays_literals(self, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        for var in ("INFLUX_URL", "INFLUX_ORG", "OBD_PORT", "OBD_BAUDRATE",
                    "OBD_DEBUG", "CAR_VIN", "TELEM_SPOOL_DIR",
                    "TELEM_SPOOL_MAX_SIZE", "PISUGAR_HOST"):
            monkeypatch.delenv(var, raising=False)
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

    def test_bad_max_size_raises(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [telem.spool]
            max_size = "1XB"
        """)
        with pytest.raises(_config.ConfigError, match="max_size"):
            _config.load_config()


class TestEnvPrecedence:
    def test_env_overrides_file(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path, monkeypatch, """
            [influx]
            url = "http://from-file:8086"
        """)
        monkeypatch.setenv("INFLUX_URL", "http://from-env:9999")
        assert _config.load_config().influx.url == "http://from-env:9999"

    def test_env_size_and_int_coercion(self, tmp_path, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        monkeypatch.setenv("TELEM_SPOOL_MAX_SIZE", "512MiB")
        monkeypatch.setenv("OBD_BAUDRATE", "38400")
        c = _config.load_config()
        assert c.telem.spool.max_size == 512 * 1024 ** 2
        assert c.telem.obd.baudrate == 38400

    def test_buckets_have_no_env_override(self, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        monkeypatch.setenv("BUCKETS_LAPS", "ignored")
        assert _config.load_config().influx.buckets.laps == "laps"
