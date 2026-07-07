# Configuration

lemongrass reads its non-secret settings from two layers, highest priority first:

1. **A TOML config file** — pointed to by the `LEMONGRASS_CONFIG` environment variable
   (an absolute path). Optional; if unset, no file is read.
2. **Built-in defaults** — the values shipped in the code.

Environment variables are used **only for secrets** (see below) — they do not override any
non-secret setting. With no config file, lemongrass runs on its built-in defaults. Copy
`lemongrass.toml.sample` (every key at its default) as a starting point and edit down.

## Secrets

Secrets are never stored in the config file. The file instead names the environment
variable that holds each secret (`*_env` directives):

| Secret | Directive (default) | Env var read |
|---|---|---|
| InfluxDB token | `influx.token_env` = `"INFLUX_TELEMETRY_TOKEN"` | value of that var |
| RaceMonitor token pool | `racemonitor.tokens_env` = `"RACEMONITOR_TOKENS"` | comma-separated; legacy singular `RACEMONITOR_TOKEN` also honored |

## Migrating from environment variables

Releases up to and including v4.0.0 configured non-secret settings via environment
variables. Those variables are **no longer read** — after upgrading, any setting still
supplied that way silently falls back to its built-in default (no error). Move each one
into the TOML file and point `LEMONGRASS_CONFIG` at it:

| Old env var | TOML key |
|---|---|
| `INFLUX_URL` | `influx.url` |
| `INFLUX_ORG` | `influx.org` |
| `OBD_PORT` | `telem.obd.port` |
| `OBD_BAUDRATE` | `telem.obd.baudrate` |
| `OBD_DEBUG` | `telem.obd.debug` |

Only the secrets stay in the environment: `INFLUX_TELEMETRY_TOKEN` and
`RACEMONITOR_TOKENS` / legacy `RACEMONITOR_TOKEN` (variable names configurable via the
`*_env` directives above).

The CLI prints a startup warning for each dropped variable it finds still set.

## Validation

Config is validated on load and fails loud rather than silently misbehaving:

- `LEMONGRASS_CONFIG` set but the file is missing/unreadable → error.
- Malformed TOML → error naming the file.
- Unknown keys or sections (e.g. `bucket` vs `buckets`) → error.
- Wrong value types (e.g. a numeric `default_start_date`) → error.
- An unparseable or non-positive `telem.spool.max_size` → error.

## Key reference

| TOML key | Type | Default | Description |
|---|---|---|---|
| `influx.url` | string | `https://influxdb.focism.com` | InfluxDB base URL |
| `influx.org` | string | `focism` | InfluxDB org |
| `influx.token_env` | string | `INFLUX_TELEMETRY_TOKEN` | env var holding the Influx token |
| `influx.buckets.laps` | string | `laps` | lap-data bucket |
| `influx.buckets.races` | string | `races` | race-metadata bucket |
| `influx.buckets.sessions` | string | `race_sessions` | session bucket |
| `influx.buckets.telem` | string | `telem` | OBD telemetry bucket |
| `influx.buckets.pisugar` | string | `pisugar` | PiSugar telemetry bucket |
| `races.backfill.search_terms` | list of strings | `["Real Hoopties", "GP du Lac", "Halloween Hoop"]` | RaceMonitor search terms |
| `races.backfill.default_start_date` | string | `2017-01-01` | earliest race date, `YYYY-MM-DD` (`--start-date` overrides) |
| `racemonitor.tokens_env` | string | `RACEMONITOR_TOKENS` | env var holding the token pool |
| `telem.vin` | string | `""` | VIN fallback when OBD does not report one |
| `telem.obd.port` | string | `/dev/obd` | OBD serial/socket port |
| `telem.obd.baudrate` | int | `0` | baud rate (`0` = auto-detect) |
| `telem.obd.debug` | bool | `false` | verbose python-obd logging |
| `telem.spool.dir` | string | `/data/telem-spool` | disk spool directory |
| `telem.spool.max_size` | size | `1GiB` | spool cap; accepts `1GiB`/`1GB`/bytes int |
| `pisugar.host` | string | `""` | host tag (`""` = system hostname) |
| `pisugar.api_url` | string | `http://localhost:8421` | pisugar-server base URL |
| `pisugar.config_path` | string | `/etc/pisugar-server/config.json` | pisugar credentials file |

## Examples

Override only the bucket names (e.g. a shared InfluxDB):

```toml
[influx.buckets]
laps = "team2_laps"
races = "team2_races"
sessions = "team2_sessions"
telem = "team2_telem"
pisugar = "team2_pisugar"
```

A fresh fork pointing at its own races and InfluxDB, with the token in a differently named
env var:

```toml
[influx]
url = "https://influx.example.org"
org = "team2"
token_env = "INFLUX_TOKEN"

[races.backfill]
search_terms = ["Team2 Endurance", "Winter Grand Prix"]
default_start_date = "2021-03-01"
```
