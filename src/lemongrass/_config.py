"""Layered configuration for lemongrass: dataclass defaults overlaid by an
optional TOML file (via LEMONGRASS_CONFIG). Environment variables are read only
for secrets, and only indirectly — a config value such as influx.token_env names
the env var that holds the secret; the secret itself is never read in this module.

This module is a leaf — it imports nothing else from lemongrass — so any command
module can source its settings here without an import cycle.
"""
import os
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

# Non-secret env vars read by released versions (<= v4.0.0) and dropped in the
# secrets-only model; a deployment still setting one silently runs on defaults.
# Vars introduced after v4.0.0 and replaced by config before ever shipping in a
# release (CAR_VIN, HOST, TELEM_SPOOL_DIR, TELEM_SPOOL_MAX_BYTES) are excluded:
# they were never a public interface, so there is nothing to migrate.
_DROPPED_ENV_VARS = {
    'INFLUX_URL': 'influx.url',
    'INFLUX_ORG': 'influx.org',
    'OBD_PORT': 'telem.obd.port',
    'OBD_BAUDRATE': 'telem.obd.baudrate',
    'OBD_DEBUG': 'telem.obd.debug',
}


def warn_dropped_env_vars():
    """Print a stderr warning for each legacy non-secret env var still set."""
    for var, key in sorted(_DROPPED_ENV_VARS.items()):
        if var in os.environ:
            print(f"Warning: {var} is set but no longer read; set {key} in the "
                  "TOML file named by LEMONGRASS_CONFIG instead "
                  "(see docs/CONFIGURATION.md)", file=sys.stderr)

_SIZE_RE = re.compile(r'^\s*([0-9]*\.?[0-9]+)\s*([KMGTP]I?B|B)?\s*$', re.IGNORECASE)
_SIZE_UNITS = {
    '': 1, 'B': 1,
    'KB': 1000, 'MB': 1000 ** 2, 'GB': 1000 ** 3, 'TB': 1000 ** 4, 'PB': 1000 ** 5,
    'KIB': 1024, 'MIB': 1024 ** 2, 'GIB': 1024 ** 3, 'TIB': 1024 ** 4, 'PIB': 1024 ** 5,
}


def parse_size(value):
    """Parse a byte size from an int/float (bytes) or a unit string like "1GiB".

    Binary units (KiB..PiB) are powers of 1024; decimal units (KB..PB) powers of
    1000; a bare number or a `B` suffix is bytes. Case-insensitive, whitespace and
    fractions allowed. Raises ValueError on an unparseable or non-positive value.
    """
    if isinstance(value, bool):
        raise ValueError(f"invalid size: {value!r}")
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(
                f"size must be a whole number of bytes: {value!r}"
                " (for fractional units use a string like \"1.5GiB\")")
        n = int(value)
    elif isinstance(value, str):
        m = _SIZE_RE.match(value)
        if not m:
            raise ValueError(f"invalid size: {value!r}")
        n = int(float(m.group(1)) * _SIZE_UNITS[(m.group(2) or '').upper()])
    else:
        raise ValueError(f"invalid size: {value!r}")
    if n <= 0:
        raise ValueError(f"size must be positive: {value!r}")
    return n


class ConfigError(Exception):
    """Raised for a malformed/unreadable config file or invalid config values."""


@dataclass(frozen=True)
class Buckets:
    laps: str = 'laps'
    races: str = 'races'
    sessions: str = 'race_sessions'
    telem: str = 'telem'
    pisugar: str = 'pisugar'


@dataclass(frozen=True)
class InfluxConfig:
    url: str = 'https://influxdb.focism.com'
    org: str = 'focism'
    token_env: str = 'INFLUX_TELEMETRY_TOKEN'
    buckets: Buckets = field(default_factory=Buckets)


@dataclass(frozen=True)
class BackfillConfig:
    search_terms: tuple = ('Real Hoopties', 'GP du Lac', 'Halloween Hoop')
    default_start_date: str = '2017-01-01'


@dataclass(frozen=True)
class RacesConfig:
    backfill: BackfillConfig = field(default_factory=BackfillConfig)


@dataclass(frozen=True)
class RaceMonitorConfig:
    tokens_env: str = 'RACEMONITOR_TOKENS'


@dataclass(frozen=True)
class ObdConfig:
    port: str = '/dev/obd'
    baudrate: int = 0
    debug: bool = False


@dataclass(frozen=True)
class SpoolConfig:
    dir: str = '/data/telem-spool'
    max_size: int = 1024 ** 3


@dataclass(frozen=True)
class TelemConfig:
    vin: str = ''
    obd: ObdConfig = field(default_factory=ObdConfig)
    spool: SpoolConfig = field(default_factory=SpoolConfig)


@dataclass(frozen=True)
class PisugarConfig:
    host: str = ''
    api_url: str = 'http://localhost:8421'
    config_path: str = '/etc/pisugar-server/config.json'


@dataclass(frozen=True)
class Config:
    influx: InfluxConfig = field(default_factory=InfluxConfig)
    races: RacesConfig = field(default_factory=RacesConfig)
    racemonitor: RaceMonitorConfig = field(default_factory=RaceMonitorConfig)
    telem: TelemConfig = field(default_factory=TelemConfig)
    pisugar: PisugarConfig = field(default_factory=PisugarConfig)


def load_config():
    """Build the config: dataclass defaults overlaid by the TOML file named in
    LEMONGRASS_CONFIG (if set). There is no environment-override layer — env is
    used only for secrets, which are referenced indirectly by the *_env config
    values and read at their point of use, never here.

    Not memoized — called at import/startup only, so reading the file fresh each
    call keeps behavior predictable under test monkeypatching.
    """
    return _build_config(_read_file())


def _read_file():
    path = os.environ.get('LEMONGRASS_CONFIG')
    if not path:
        return {}
    p = Path(path)
    try:
        raw = p.read_bytes()
    except OSError as e:
        raise ConfigError(f"LEMONGRASS_CONFIG={path!r} could not be read: {e}") from e
    try:
        return tomllib.loads(raw.decode('utf-8'))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as e:
        raise ConfigError(f"LEMONGRASS_CONFIG={path!r} is not valid TOML: {e}") from e


def _reject_unknown(d, allowed, where):
    if not isinstance(d, dict):
        raise ConfigError(f"[{where}] must be a table, not {type(d).__name__}")
    extra = set(d) - allowed
    if extra:
        raise ConfigError(f"unknown key(s) in [{where}]: {', '.join(sorted(extra))}")


def _typed(d, key, default, kind, where):
    if key not in d:
        return default
    v = d[key]
    if kind is str and not isinstance(v, str):
        raise ConfigError(f"{where}.{key} must be a string")
    if kind is int and (isinstance(v, bool) or not isinstance(v, int)):
        raise ConfigError(f"{where}.{key} must be an integer")
    if kind is bool and not isinstance(v, bool):
        raise ConfigError(f"{where}.{key} must be a boolean")
    if kind is tuple:
        if not (isinstance(v, list) and all(isinstance(x, str) for x in v)):
            raise ConfigError(f"{where}.{key} must be a list of strings")
        return tuple(v)
    return v


def _build_config(data):
    _reject_unknown(data, {'influx', 'races', 'racemonitor', 'telem', 'pisugar'},
                    'top level')
    return Config(
        influx=_build_influx(data.get('influx', {})),
        races=_build_races(data.get('races', {})),
        racemonitor=_build_racemonitor(data.get('racemonitor', {})),
        telem=_build_telem(data.get('telem', {})),
        pisugar=_build_pisugar(data.get('pisugar', {})),
    )


def _build_influx(d):
    _reject_unknown(d, {'url', 'org', 'token_env', 'buckets'}, 'influx')
    b = d.get('buckets', {})
    _reject_unknown(b, {'laps', 'races', 'sessions', 'telem', 'pisugar'},
                    'influx.buckets')
    dflt, bd = InfluxConfig(), Buckets()
    return InfluxConfig(
        url=_typed(d, 'url', dflt.url, str, 'influx'),
        org=_typed(d, 'org', dflt.org, str, 'influx'),
        token_env=_typed(d, 'token_env', dflt.token_env, str, 'influx'),
        buckets=Buckets(
            laps=_typed(b, 'laps', bd.laps, str, 'influx.buckets'),
            races=_typed(b, 'races', bd.races, str, 'influx.buckets'),
            sessions=_typed(b, 'sessions', bd.sessions, str, 'influx.buckets'),
            telem=_typed(b, 'telem', bd.telem, str, 'influx.buckets'),
            pisugar=_typed(b, 'pisugar', bd.pisugar, str, 'influx.buckets'),
        ),
    )


def _build_races(d):
    _reject_unknown(d, {'backfill'}, 'races')
    bf = d.get('backfill', {})
    _reject_unknown(bf, {'search_terms', 'default_start_date'}, 'races.backfill')
    dflt = BackfillConfig()
    return RacesConfig(backfill=BackfillConfig(
        search_terms=_typed(bf, 'search_terms', dflt.search_terms, tuple,
                            'races.backfill'),
        default_start_date=_typed(bf, 'default_start_date', dflt.default_start_date,
                                  str, 'races.backfill'),
    ))


def _build_racemonitor(d):
    _reject_unknown(d, {'tokens_env'}, 'racemonitor')
    dflt = RaceMonitorConfig()
    return RaceMonitorConfig(
        tokens_env=_typed(d, 'tokens_env', dflt.tokens_env, str, 'racemonitor'))


def _build_telem(d):
    _reject_unknown(d, {'vin', 'obd', 'spool'}, 'telem')
    obd_d, spool_d = d.get('obd', {}), d.get('spool', {})
    _reject_unknown(obd_d, {'port', 'baudrate', 'debug'}, 'telem.obd')
    _reject_unknown(spool_d, {'dir', 'max_size'}, 'telem.spool')
    dflt, od, sd = TelemConfig(), ObdConfig(), SpoolConfig()
    max_size = sd.max_size
    if 'max_size' in spool_d:
        try:
            max_size = parse_size(spool_d['max_size'])
        except ValueError as e:
            raise ConfigError(f"telem.spool.max_size: {e}") from e
    return TelemConfig(
        vin=_typed(d, 'vin', dflt.vin, str, 'telem'),
        obd=ObdConfig(
            port=_typed(obd_d, 'port', od.port, str, 'telem.obd'),
            baudrate=_typed(obd_d, 'baudrate', od.baudrate, int, 'telem.obd'),
            debug=_typed(obd_d, 'debug', od.debug, bool, 'telem.obd'),
        ),
        spool=SpoolConfig(
            dir=_typed(spool_d, 'dir', sd.dir, str, 'telem.spool'),
            max_size=max_size,
        ),
    )


def _build_pisugar(d):
    _reject_unknown(d, {'host', 'api_url', 'config_path'}, 'pisugar')
    dflt = PisugarConfig()
    return PisugarConfig(
        host=_typed(d, 'host', dflt.host, str, 'pisugar'),
        api_url=_typed(d, 'api_url', dflt.api_url, str, 'pisugar'),
        config_path=_typed(d, 'config_path', dflt.config_path, str, 'pisugar'),
    )
