# Configuration

lemongrass reads non-secret settings from two sources. The first source has
priority:

1. **A TOML file.** The `LEMONGRASS_CONFIG` environment variable gives the absolute
   path to the file. The file is optional.
2. **Built-in defaults.**

lemongrass reads only secrets from environment variables. Environment variables do
not change other settings. To make a config file, copy `lemongrass.toml.sample`. This
file contains all keys with their default values.

## Secrets

Do not put secrets in the config file. For each secret, a `*_env` key gives the name
of the environment variable that contains the secret:

| Secret | Key (default) | Notes |
|---|---|---|
| InfluxDB token | `influx.token_env` = `"INFLUX_TELEMETRY_TOKEN"` | |
| RaceMonitor tokens | `racemonitor.tokens_env` = `"RACEMONITOR_TOKENS"` | Separate tokens with commas. lemongrass also reads the legacy variable `RACEMONITOR_TOKEN`, which contains one token. |
| PostgreSQL password | `postgres.password_env` = `"LEMONGRASS_DB_PASSWORD"` | |

## Migrating from environment variables

Version 4.0.0 and earlier read non-secret settings from environment variables.
Later versions do not read these variables. If you still set one of them,
lemongrass uses the default value and does not show an error. The CLI shows a
warning at startup for each of these variables that it finds.

Move each setting to the TOML file:

| Old environment variable | TOML key |
|---|---|
| `INFLUX_URL` | `influx.url` |
| `INFLUX_ORG` | `influx.org` |
| `OBD_PORT` | `telem.obd.port` |
| `OBD_BAUDRATE` | `telem.obd.baudrate` |
| `OBD_DEBUG` | `telem.obd.debug` |

## Validation

lemongrass validates the config when it loads it. It stops with an error in these
conditions:

- `LEMONGRASS_CONFIG` is set, but lemongrass cannot find or read the file.
- The file is not valid TOML.
- The file has an unknown key or section, for example `bucket` instead of `buckets`.
- A value has the wrong type, for example a number for `default_start_date`.
- The value of `telem.spool.max_size` is not a valid size, or is not more than 0.

## Key reference

| Key | Type | Default | Description |
|---|---|---|---|
| `influx.url` | string | `https://influxdb.focism.com` | InfluxDB base URL |
| `influx.org` | string | `focism` | InfluxDB organization |
| `influx.token_env` | string | `INFLUX_TELEMETRY_TOKEN` | Environment variable that contains the InfluxDB token |
| `influx.buckets.laps` | string | `laps` | Bucket for laps and standings |
| `influx.buckets.races` | string | `races` | Legacy bucket for race metadata. Only `db import-legacy` and `races prune` use it. |
| `influx.buckets.sessions` | string | `race_sessions` | Legacy bucket for sessions. Only `db import-legacy` and `races prune` use it. |
| `influx.buckets.telem` | string | `telem` | Bucket for OBD telemetry |
| `influx.buckets.pisugar` | string | `pisugar` | Bucket for PiSugar telemetry |
| `races.backfill.search_terms` | list of strings | `["Real Hoopties", "GP du Lac", "Halloween Hoop"]` | Search terms for RaceMonitor |
| `races.backfill.default_start_date` | string | `2017-01-01` | Earliest race date, as `YYYY-MM-DD`. `--start-date` overrides this value. |
| `races.backfill.series_id` | int | `0` | RaceMonitor series to read races from. If set, the search terms filter the races in the series by name. `0` disables this setting. |
| `racemonitor.tokens_env` | string | `RACEMONITOR_TOKENS` | Environment variable that contains the RaceMonitor tokens |
| `team.id` | string | `""` | The `team_id` of your team. If set, a live capture records its own `entries` row. Create the team first with `lemongrass teams add`. `""` disables this setting. |
| `telem.vin` | string | `""` | VIN to use if OBD does not report a VIN |
| `telem.obd.port` | string | `/dev/obd` | OBD serial port or socket |
| `telem.obd.baudrate` | int | `0` | Baud rate. `0` detects the baud rate automatically. |
| `telem.obd.debug` | bool | `false` | Show debug log messages from python-obd |
| `telem.spool.dir` | string | `/data/telem-spool` | Directory for the disk spool |
| `telem.spool.max_size` | size | `1GiB` | Maximum size of the spool. Use `1GiB`, `1GB`, or an integer number of bytes. |
| `pisugar.host` | string | `""` | Value of the `host` tag. `""` uses the system hostname. |
| `pisugar.api_url` | string | `http://localhost:8421` | Base URL of pisugar-server |
| `pisugar.config_path` | string | `/etc/pisugar-server/config.json` | pisugar-server file that contains the credentials |
| `postgres.host` | string | `localhost` | PostgreSQL host |
| `postgres.port` | int | `5432` | PostgreSQL port |
| `postgres.database` | string | `lemongrass` | Database name |
| `postgres.user` | string | `lemongrass` | Database user |
| `postgres.password_env` | string | `LEMONGRASS_DB_PASSWORD` | Environment variable that contains the database password |

## Changing search terms interactively

In a terminal, `race-backfill` shows a list of the matched races. You can add or
remove search terms in this list. The changes apply to the current run.

If you changed the terms, lemongrass offers to save them when you confirm. It writes
them to `races.backfill.search_terms` in the file that `LEMONGRASS_CONFIG` gives.
lemongrass does not change the other content of the file, including comments. If
`LEMONGRASS_CONFIG` is not set, or if the save fails, lemongrass shows the TOML
text. Copy this text into your config file.

To find a series and save it as `races.backfill.series_id`, press `s` in the list.

## Examples

Change only the bucket names, for example on a shared InfluxDB:

```toml
[influx.buckets]
laps = "team2_laps"
telem = "team2_telem"
pisugar = "team2_pisugar"
```

Use a different InfluxDB, different races, and a different name for the token
variable:

```toml
[influx]
url = "https://influx.example.org"
org = "team2"
token_env = "INFLUX_TOKEN"

[races.backfill]
search_terms = ["Team2 Endurance", "Winter Grand Prix"]
default_start_date = "2021-03-01"
```
