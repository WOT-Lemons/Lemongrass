# Lemongrass

Open source car telemetry for 24 Hours of Lemons.

## Requirements

- Raspberry Pi running Raspberry Pi OS
- PiSugar 3 UPS
- USB OBD-II adapter
- InfluxDB 2.x
- PostgreSQL 14 or later
- Grafana

## Services

Two long-running services run on the Pi:

| Service | Description |
| --- | --- |
| `telem` | Reads car data from the OBD-II adapter |
| `pisugar-monitor` | Reads the status of the PiSugar 3 UPS |

Deployment files are not part of this repository. Keep Docker Compose files,
Telegraf configuration, and other deployment files in a separate infrastructure
repository.

## Installation

Install lemongrass with one of these commands:

```shell
docker pull ghcr.io/wot-lemons/lemongrass:latest   # Docker
pip install lemongrass                             # pip
uv tool install lemongrass                         # uv
uvx lemongrass <command>                           # uv, without installing
```

To pin a lemongrass release, replace the image tag `latest` with the release
version (`X.Y.Z`). For the list of tags, see `ghcr.io/wot-lemons/lemongrass`.

The examples in this document use the installed `lemongrass` command. To run a
command with Docker, run it in a container:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest lemongrass <command>
```

## Configuration

lemongrass reads secrets from environment variables:

| Variable | Used for |
| --- | --- |
| `INFLUX_TELEMETRY_TOKEN` | InfluxDB |
| `LEMONGRASS_DB_PASSWORD` | PostgreSQL |
| `RACEMONITOR_TOKENS` | RaceMonitor API. Separate tokens with commas. To get a token, go to <https://www.race-monitor.com/Home/API>. |

To set the secrets:

1. Copy `.env.sample` to `.env`.
2. Add the values to `.env`.
3. Load the file into your shell:

   ```shell
   set -a && source .env && set +a
   ```

lemongrass reads all other settings from an optional TOML file. The
`LEMONGRASS_CONFIG` environment variable gives the path to this file. If
`LEMONGRASS_CONFIG` is not set, lemongrass uses built-in defaults. To make a config
file, copy `lemongrass.toml.sample`. With Docker, mount the file in the container.
Then set `LEMONGRASS_CONFIG` to the path of the file in the container. For the list
of all keys, see
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Database

lemongrass stores race and session metadata in PostgreSQL. It stores laps,
standings, telemetry, and PiSugar data in InfluxDB.

Before you use lemongrass for the first time, apply the database schema. Apply it
again after each upgrade that adds a migration.

```shell
lemongrass db upgrade
lemongrass db current   # show the applied revision
```

| Subcommand | Description |
| --- | --- |
| `upgrade` | Apply all pending migrations, then load the track data |
| `current` | Show the applied revision |
| `import-legacy [--dry-run] [--only-missing]` | Copy races and sessions from the legacy InfluxDB buckets to PostgreSQL |
| `export-legacy [--output FILE]` | Export races and sessions as InfluxDB line protocol, for rollback |

To move an existing installation from the legacy InfluxDB buckets to PostgreSQL,
follow the runbook in
[local-testing/migrations/README.md](local-testing/migrations/README.md).

## Lap data

`lemongrass laps` reads lap data from RaceMonitor. It can watch a live race or
import a completed race.

To find a race ID, open <https://www.race-monitor.com/Live/Race> while the race is
live. The ID is in the URL:

![Race ID in the URL bar](https://i.imgur.com/1FQNvSb.png)

### Interactive mode

To start interactive mode, run `lemongrass laps` with no arguments in a terminal.
Search for a race by name, or enter its ID. Then select one of these options:

- **Watch a live race.** lemongrass shows your car's laps, the field leaderboard,
  and a log. When the race ends, lemongrass offers to run the final import.
- **Import a completed race.** lemongrass imports the full field into InfluxDB.
- **Wait for a race to start.** Enter your car number. lemongrass checks the race
  every 10 seconds. When the race is live and your car is in the timing feed,
  lemongrass starts to watch the race. To select a different car after the race
  starts, press `c`.

You can also start interactive mode from the Home menu. To open the Home menu, run
`lemongrass` with no arguments.

### Command-line mode

```shell
lemongrass laps RACE_ID [CAR_NUMBER] [options]
```

Use this form in scripts and cron jobs. If the race is live, you must give
`CAR_NUMBER`.

| Option | Description |
| --- | --- |
| `-m`, `--monitor` | Watch a live race and show new laps when they arrive. Requires `CAR_NUMBER`. |
| `-n`, `--network` | Write laps to InfluxDB |
| `--wait-for-live` | Wait until the race is live, then watch it. Implies `-m`. |
| `--interval SECONDS` | Time between checks in monitor mode. Default: 30. |
| `-o`, `--out` | Write the car's laps to a CSV file. Live races only. |
| `-c`, `--class A/B/C` | Group or filter by class |
| `--dry-run` | Show what lemongrass would write, but do not write it. Implies `-n`. Completed races only. |
| `--skip-if-complete` | Do not import the race if it already has all laps at the current schema version. Completed races with `-n` only. |
| `-v`, `--verbose` | Show debug log messages |

To stop, press Ctrl-C. The command exits with code 130. Monitor mode also stops
when the race ends.

#### Live race

```shell
lemongrass laps RACE_ID CAR_NUMBER -m -n
```

To start before the green flag, use `--wait-for-live`. lemongrass checks the race
every 10 seconds, with no time limit. If the race ends before your car is in the
timing feed, the command exits with a non-zero code.

```shell
lemongrass laps RACE_ID CAR_NUMBER -n --wait-for-live
```

Example output:

```plain
--------------------------------------------------------------------------------
Race 166811
Fast Friday Started: 2026-06-19 16:00:00
Seekonk Speedway   Ends: 2026-06-20 01:00:00
--------------------------------------------------------------------------------
2026-06-19 20:52:41,152 - INFO - Race 166811 is currently live.
...
Team: Jacob Burns Car Number: 13   Class: Legends Transponder: 13775638
Best Position: 2
Final Position: 1
Final Class Position: 1
Total Laps: 16
Best Lap: 6
Best Lap Time: 00:00:14.165
Total Time: 00:05:43.405
--------------------------------------------------------------------------------
2026-06-19 20:52:41,369 - INFO - Monitoring car 13...
--------------------------------------------------------------------------------
Lap Position      LapTime FlagStatus    TotalTime
  1        8 00:00:14.572      Green 00:00:23.097
  2        5 00:00:14.458      Green 00:00:37.555
  3        4 00:00:14.389      Green 00:00:51.944
...
 17        1 00:00:14.291      Green 00:05:57.696
```

#### Completed race

```shell
lemongrass laps RACE_ID      # show the final standings
lemongrass laps RACE_ID -n   # import the full field into InfluxDB
```

For a completed race, lemongrass ignores `CAR_NUMBER`. The RaceMonitor API accepts
approximately 6 requests per minute, so an import can take several minutes.

Example output:

```plain
--------------------------------------------------------------------------------
Race 166429
The B.F.E. GP 2026 Started: 2026-06-12 10:00:00
High Plains Raceway   Ends: 2026-06-14 19:30:00
--------------------------------------------------------------------------------
2026-06-19 20:46:17,488 - INFO - Race 166429 is not live. Monitor mode disabled.
2026-06-19 20:46:17,948 - INFO - Rate limited: sleeping 59.34s [6/6 slots used over 60s window; oldest request 0.66s ago]
--------------------------------------------------------------------------------
2026-06-19 20:47:18,913 - INFO - Current overall rankings.
--------------------------------------------------------------------------------
Pos.   # Class  Class Pos.                             Name Laps Transponder
   1 852     A           1              Rusty Bottom Racing  351        D-34
   2   4     A           2         Sew So Fast - GC edition  349         C-2
   3 380     A           3                 Vice City Racing  349        D-20
...
```

#### CSV output in Docker

`-o` writes the CSV file to `/data`, the working directory of the container. It
works only for a live race. The container runs as a non-root user. To keep the
file, mount a writable host directory at `/data`. Run the container as your host
user:

```shell
mkdir -p out
docker run --rm -it --env-file .env \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/out:/data" \
  ghcr.io/wot-lemons/lemongrass:latest \
  lemongrass laps RACE_ID CAR_NUMBER -o
```

## Race management

```shell
lemongrass races <subcommand> [args]
```

| Subcommand | Description |
| --- | --- |
| `list` | List stored races, with lap counts and schema version status |
| `prune RACE_ID... [--yes]` | Delete races. lemongrass deletes the laps and standings from InfluxDB, then deletes the race, its sessions, and its entries from PostgreSQL. To skip the confirmation prompt, use `--yes`. |
| `backfill [options]` | Import historical races. Same as `lemongrass race-backfill`. |
| `diagnose RACE_ID CAR_NUMBER` | Compare the RaceMonitor and InfluxDB lap counts for one car |
| `identify [RACE_ID...] [--dry-run]` | Set the venue, layout, and event IDs from `tracks.toml`. See [Track identity](#track-identity). |

Examples:

```shell
lemongrass races list
lemongrass races prune 144185
lemongrass races prune 144185 120037 --yes
lemongrass races diagnose 144185 252
```

### Races browser

To open a list of stored races, run `lemongrass races` with no subcommand in a
terminal.

| Key | Action |
| --- | --- |
| `a` | Select all races |
| `i` | Invert the selection |
| `p` | Delete the selected races |
| `d` | Diagnose the highlighted race |
| `r` | Import the highlighted race again |
| `b` | Search RaceMonitor and import a race |
| `esc` | Go back |

Interactive mode needs a terminal on stdin and stdout. If stdin or stdout is not a
terminal, the commands run in command-line mode. `telem` and `pisugar-monitor` do not have an
interactive mode.

### Backfill

`lemongrass races backfill` finds past races on RaceMonitor and imports them. It
searches with the terms in `races.backfill.search_terms`. If
`races.backfill.series_id` is set, it reads the races in that series and uses the
terms as a filter. In a terminal, it shows the matched races. You can change the
selection before the import starts.

| Option | Description |
| --- | --- |
| `--dry-run` | List the races that lemongrass would import, but do not import them |
| `--force` | Import all races again, including complete races |
| `--upgrade-stored` | Import stored races again if their laps or standings have an older `schema_version`. lemongrass uses the stored race list and does not search RaceMonitor, so this option is faster than `--force`. |
| `--validate` | Make sure that each expected race has metadata and at least one lap |
| `--start-date YYYY-MM-DD` | Include only races that start on or after this date. Default: `races.backfill.default_start_date`. |

You cannot use `--upgrade-stored` with `--start-date` or `--validate`. To also
import races that are already at the current schema version, add `--force`.

### Sessions

Each lap point in InfluxDB has a `session_id` tag. This tag contains the RaceMonitor
session ID. To separate the parts of a race, such as day 1 and day 2, filter on this
tag in Flux. lemongrass stores session metadata in PostgreSQL. It no longer writes
to the legacy `race_sessions` bucket.

### Track identity

RaceMonitor's track name is free text, and its spelling changes between years. To
group races across seasons, lemongrass maps track names to venue, layout, and event
IDs. The mapping is in `src/lemongrass/data/tracks.toml`. To change the mapping,
edit that file in a pull request.

```shell
lemongrass races identify --dry-run   # show the changes and the unmatched names
# add the unmatched names to src/lemongrass/data/tracks.toml
lemongrass races identify             # write the IDs
```

`races identify` uses only stored data. It does not send requests to RaceMonitor. If
a race has an unmatched track name, its IDs stay NULL, and lemongrass shows the raw
`track_name`. If you give a race ID that is not stored, the command shows an error
on stderr and exits with a non-zero code.

`lemongrass tracks sync [--dry-run]` loads `tracks.toml` into the database.
`lemongrass db upgrade` also does this.

### Teams

lemongrass stores team data only in PostgreSQL.

```shell
lemongrass teams <subcommand> [args]
```

| Subcommand | Description |
| --- | --- |
| `add TEAM_ID NAME` | Create a team. If the team exists, change its name. |
| `list` | List the teams and their aliases |
| `alias TEAM_ID ALIAS` | Add a different spelling of a team's name |
| `merge FROM_ID INTO_ID` | Move the entries and aliases of one team to a different team, then delete the first team |

Create a team before you use its ID in `team.id` or `entries set --team`.

### Entries

An entry records which team drove which car number in a race. Car numbers change
between events. Thus, an entry is the only reliable link between a team and its
laps.

```shell
lemongrass entries <subcommand> [args]
```

| Subcommand | Description |
| --- | --- |
| `set RACE_ID CAR_NUMBER [--team TEAM_ID]` | Record an entry. The default for `--team` is `team.id`. |
| `list [--team TEAM_ID] [--race RACE_ID]` | List entries. To show only one team or one race, use the options. |
| `propose --term TERM [--term TERM ...] [--team TEAM_ID]` | Find stored competitor names that match the terms, and select the entries to record. You can also save a matched name as an alias. |

If `team.id` is set, a live capture records its own entry. To record entries for
past races, use `lemongrass entries`.

## Contributing

To set up a development environment, run the tests, or run the local stack, see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Upgrading

To upgrade to a new major version, follow the steps in
[docs/UPGRADING.md](docs/UPGRADING.md).
