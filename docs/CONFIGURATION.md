# Configuration

lemongrass reads non-secret settings from two sources, in order of priority:

1. **A TOML file** named by the `LEMONGRASS_CONFIG` environment variable (absolute
   path). Optional.
2. **Built-in defaults.**

Environment variables hold only secrets. They do not override any other setting. To
start a config file, copy `lemongrass.toml.sample`, which lists every key at its
default.

## Secrets

Secrets are never stored in the config file. Instead, each `*_env` key names the
environment variable that holds the secret:

| Secret | Key (default) | Notes |
|---|---|---|
| InfluxDB token | `influx.token_env` = `"INFLUX_TELEMETRY_TOKEN"` | |
| RaceMonitor token pool | `racemonitor.tokens_env` = `"RACEMONITOR_TOKENS"` | Comma-separated. The legacy `RACEMONITOR_TOKEN` (one token) is also read. |
| PostgreSQL password | `postgres.password_env` = `"LEMONGRASS_DB_PASSWORD"` | |

## Migrating from environment variables

Up to v4.0.0, non-secret settings came from environment variables. These variables
are no longer read. A setting that is still supplied this way falls back to its
default without an error. The CLI prints a warning at startup for each one it finds.
Move each setting to the TOML file:

| Old env var | TOML key |
|---|---|
| `INFLUX_URL` | `influx.url` |
| `INFLUX_ORG` | `influx.org` |
| `OBD_PORT` | `telem.obd.port` |
| `OBD_BAUDRATE` | `telem.obd.baudrate` |
| `OBD_DEBUG` | `telem.obd.debug` |

## Validation

The config is validated on load. Each of these is an error:

- `LEMONGRASS_CONFIG` is set, but the file is missing or unreadable.
- The file is not valid TOML.
- The file has an unknown key or section, for example `bucket` instead of `buckets`.
- A value has the wrong type, for example a number for `default_start_date`.
- `telem.spool.max_size` cannot be parsed or is not positive.

## Key reference

| Key | Type | Default | Description |
|---|---|---|---|
| `influx.url` | string | `https://influxdb.focism.com` | InfluxDB base URL |
| `influx.org` | string | `focism` | InfluxDB org |
| `influx.token_env` | string | `INFLUX_TELEMETRY_TOKEN` | Env var that holds the InfluxDB token |
| `influx.buckets.laps` | string | `laps` | Lap and standings bucket |
| `influx.buckets.races` | string | `races` | Legacy race-metadata bucket, read by `db import-legacy` |
| `influx.buckets.sessions` | string | `race_sessions` | Legacy session bucket, read by `db import-legacy` |
| `influx.buckets.telem` | string | `telem` | OBD telemetry bucket |
| `influx.buckets.pisugar` | string | `pisugar` | PiSugar telemetry bucket |
| `races.backfill.search_terms` | list of strings | `["Real Hoopties", "GP du Lac", "Halloween Hoop"]` | RaceMonitor search terms |
| `races.backfill.default_start_date` | string | `2017-01-01` | Earliest race date, `YYYY-MM-DD`. `--start-date` overrides it. |
| `races.backfill.series_id` | int | `0` | RaceMonitor series to read races from. `0` disables it. When set, the search terms filter the series' races by name. |
| `racemonitor.tokens_env` | string | `RACEMONITOR_TOKENS` | Env var that holds the token pool |
| `team.id` | string | `""` | The `team_id` this install races as. When set, a live capture records its own `entries` row. Create the team first with `lemongrass teams add`. `""` disables it. |
| `telem.vin` | string | `""` | VIN to use when OBD does not report one |
| `telem.obd.port` | string | `/dev/obd` | OBD serial or socket port |
| `telem.obd.baudrate` | int | `0` | Baud rate. `0` detects it automatically. |
| `telem.obd.debug` | bool | `false` | Verbose python-obd logging |
| `telem.spool.dir` | string | `/data/telem-spool` | Disk spool directory |
| `telem.spool.max_size` | size | `1GiB` | Spool size limit. Accepts `1GiB`, `1GB`, or an integer number of bytes. |
| `pisugar.host` | string | `""` | `host` tag. `""` uses the system hostname. |
| `pisugar.api_url` | string | `http://localhost:8421` | pisugar-server base URL |
| `pisugar.config_path` | string | `/etc/pisugar-server/config.json` | pisugar-server credentials file |
| `postgres.host` | string | `localhost` | PostgreSQL host |
| `postgres.port` | int | `5432` | PostgreSQL port |
| `postgres.database` | string | `lemongrass` | Database name |
| `postgres.user` | string | `lemongrass` | Database user |
| `postgres.password_env` | string | `LEMONGRASS_DB_PASSWORD` | Env var that holds the database password |

## Changing search terms interactively

In a terminal, `race-backfill` opens a checklist of the matched races. Search terms
that you add or remove there apply to the current run. When you confirm, and the
terms changed, it offers to save them to `races.backfill.search_terms` in the file
named by `LEMONGRASS_CONFIG`. The rest of the file, including comments, is kept. If
`LEMONGRASS_CONFIG` is not set or the save fails, it prints the TOML for you to paste
in.

Press `s` in the checklist to find a series and save it as `races.backfill.series_id`.

## Examples

Change only the bucket names, for example on a shared InfluxDB:

```toml
[influx.buckets]
laps = "team2_laps"
telem = "team2_telem"
pisugar = "team2_pisugar"
```

A fork with its own InfluxDB, races, and token variable name:

```toml
[influx]
url = "https://influx.example.org"
org = "team2"
token_env = "INFLUX_TOKEN"

[races.backfill]
search_terms = ["Team2 Endurance", "Winter Grand Prix"]
default_start_date = "2021-03-01"
```
