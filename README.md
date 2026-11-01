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
| `pisugar-monitor` | Reads PiSugar 3 UPS status |

Deployment (Docker Compose, Telegraf, and so on) belongs in a separate infrastructure
repository, not this one.

## Installation

Use one of these:

```shell
docker pull ghcr.io/wot-lemons/lemongrass:latest   # Docker
pip install lemongrass                             # pip
uv tool install lemongrass                         # uv
uvx lemongrass <command>                           # uv, without installing
```

To pin a lemongrass release, replace the image tag `latest` with the release
version (`X.Y.Z`). Tags are listed at `ghcr.io/wot-lemons/lemongrass`.

The examples below use the installed `lemongrass` command. With Docker, run the
same command in a container:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest lemongrass <command>
```

## Configuration

Secrets come from environment variables. Copy `.env.sample` to `.env`, fill it in,
and load it into your shell:

```shell
set -a && source .env && set +a
```

| Variable | Used for |
| --- | --- |
| `INFLUX_TELEMETRY_TOKEN` | InfluxDB |
| `LEMONGRASS_DB_PASSWORD` | PostgreSQL |
| `RACEMONITOR_TOKENS` | RaceMonitor API. Comma-separated. Get a token at <https://www.race-monitor.com/Home/API>. |

All other settings are in an optional TOML file named by `LEMONGRASS_CONFIG`. Without
it, lemongrass uses built-in defaults. Copy `lemongrass.toml.sample` to start. In
Docker, mount the file and set `LEMONGRASS_CONFIG` to its path in the container. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md) for every key.

## Database

Race and session metadata are stored in PostgreSQL. Laps, standings, telemetry, and
PiSugar data are stored in InfluxDB.

Apply the schema before first use, and again after each upgrade that adds a
migration:

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

To move an existing install from the legacy InfluxDB buckets to PostgreSQL, follow
the runbook in [local-testing/migrations/README.md](local-testing/migrations/README.md).

## Lap data

`lemongrass laps` reads lap data from RaceMonitor. It can watch a live race or import
a completed one.

To find a race ID, open <https://www.race-monitor.com/Live/Race> while the race is
live. The ID is in the URL:

![Race ID in the URL bar](https://i.imgur.com/1FQNvSb.png)

### Interactive mode

Run `lemongrass laps` with no arguments in a terminal. Search for a race by name or
enter its ID, then do one of these:

- **Watch a live race.** Shows your car's laps, the field leaderboard, and a log.
  When the race ends, lemongrass offers to run the final import.
- **Import a completed race.** Imports the full field into InfluxDB.
- **Wait for a race to start.** Enter your car number. lemongrass checks every 10
  seconds and starts watching when the race is live and your car is in the timing
  feed. Once the race is live, press `c` to pick a different car.

You can also open this from the Home menu: run `lemongrass` with no arguments.

### Command-line mode

```shell
lemongrass laps RACE_ID [CAR_NUMBER] [options]
```

Scripts and cron jobs use this form. `CAR_NUMBER` is required while a race is live.

| Option | Description |
| --- | --- |
| `-m`, `--monitor` | Watch a live race and update as laps arrive. Requires `CAR_NUMBER`. |
| `-n`, `--network` | Write laps to InfluxDB |
| `--wait-for-live` | Wait for the race to go live, then monitor. Implies `-m`. |
| `--interval SECONDS` | Poll interval in monitor mode. Default: 30. |
| `-o`, `--out` | Write the car's laps to a CSV file. Live races only. |
| `-c`, `--class A/B/C` | Group or filter by class |
| `--dry-run` | Show what would be written, without writing. Implies `-n`. Completed races only. |
| `--skip-if-complete` | Skip the import if the race already has all laps at the current schema version. Completed races with `-n` only. |
| `-v`, `--verbose` | Debug logging |

Press Ctrl-C to stop. The command exits with code 130. Monitor mode also stops
when the race ends.

#### Live race

```shell
lemongrass laps RACE_ID CAR_NUMBER -m -n
```

To start before the green flag, use `--wait-for-live`. It checks the race every 10
seconds and has no timeout. If the race ends before your car appears in the timing
feed, it exits with a non-zero code.

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
lemongrass laps RACE_ID      # print the final standings
lemongrass laps RACE_ID -n   # import the full field into InfluxDB
```

For a completed race, `CAR_NUMBER` is ignored. The RaceMonitor API allows about 6
requests per minute, so an import can take a few minutes.

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

`-o` writes the CSV to the container's working directory, `/data`. It works only
for a live race. The container
runs as a non-root user, so mount a writable host directory at `/data` and run as
your host user:

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
| `list` | List stored races with lap counts and schema version status |
| `prune RACE_ID... [--yes]` | Delete races: laps and standings from InfluxDB, then the race and its sessions and entries from PostgreSQL. Asks for confirmation unless `--yes` is given. |
| `backfill [options]` | Import historical races. Same as `lemongrass race-backfill`. |
| `diagnose RACE_ID CAR_NUMBER` | Compare RaceMonitor and InfluxDB lap counts for one car |
| `identify [RACE_ID...] [--dry-run]` | Set venue, layout, and event IDs from `tracks.toml`. See [Track identity](#track-identity). |

Examples:

```shell
lemongrass races list
lemongrass races prune 144185
lemongrass races prune 144185 120037 --yes
lemongrass races diagnose 144185 252
```

### Races browser

Run `lemongrass races` with no subcommand in a terminal to open a list of stored
races:

| Key | Action |
| --- | --- |
| `a` | Select all |
| `i` | Invert selection |
| `p` | Delete the selected races |
| `d` | Diagnose the highlighted race |
| `r` | Re-import the highlighted race |
| `b` | Search RaceMonitor and import a race |
| `esc` | Back |

The interactive modes need a terminal on both stdin and stdout. Otherwise, the
commands run in command-line mode. `telem` and `pisugar-monitor` have no
interactive mode.

### Backfill

`lemongrass races backfill` finds past races on RaceMonitor and imports them. It
searches by `races.backfill.search_terms`. If `races.backfill.series_id` is set, it
reads that series instead and uses the terms to filter it. In a terminal, it shows
the matched races so you can edit the selection first.

| Option | Description |
| --- | --- |
| `--dry-run` | List what would be imported, without writing |
| `--force` | Import every race again, including complete ones |
| `--upgrade-stored` | Re-import stored races whose laps or standings have an older `schema_version`. Uses the stored race list instead of searching RaceMonitor, so it is faster than `--force`. |
| `--validate` | Check that each expected race has metadata and at least one lap |
| `--start-date YYYY-MM-DD` | Include only races that start on or after this date. Default: `races.backfill.default_start_date`. |

`--upgrade-stored` cannot be combined with `--start-date` or `--validate`. Add
`--force` to also fetch races that are already at the current schema version.

### Sessions

Each lap point in InfluxDB has a `session_id` tag with the RaceMonitor session ID.
Filter on it in Flux to separate parts of a race, such as day 1 and day 2. Session
metadata is in PostgreSQL. The legacy `race_sessions` bucket is no longer written to.

### Track identity

RaceMonitor's track name is free text, and its spelling changes between years. To
group races across seasons, lemongrass maps track names to venue, layout, and event
IDs from `src/lemongrass/data/tracks.toml`. To change the mapping, edit that file in
a pull request.

```shell
lemongrass races identify --dry-run   # show changes and unmatched names
# add the unmatched names to src/lemongrass/data/tracks.toml
lemongrass races identify             # write the IDs
```

`races identify` uses only stored data, so it makes no RaceMonitor requests. A race
with an unmatched track name keeps NULL IDs and shows its raw `track_name`. If you
name a race ID that is not stored, the command reports it on stderr and exits with a
non-zero code.

`lemongrass tracks sync [--dry-run]` loads `tracks.toml` into the database. `lemongrass
db upgrade` also does this.

### Teams

Team data is stored only in PostgreSQL.

```shell
lemongrass teams <subcommand> [args]
```

| Subcommand | Description |
| --- | --- |
| `add TEAM_ID NAME` | Create a team, or rename it if it exists |
| `list` | List teams and their aliases |
| `alias TEAM_ID ALIAS` | Add another spelling of a team's name |
| `merge FROM_ID INTO_ID` | Merge one team into another, with its entries and aliases |

Create a team before you reference it in `team.id` or `entries set --team`.

### Entries

An entry records which team ran which car number in a race. Car numbers change
between events, so an entry is the only reliable link between a team and its laps.

```shell
lemongrass entries <subcommand> [args]
```

| Subcommand | Description |
| --- | --- |
| `set RACE_ID CAR_NUMBER [--team TEAM_ID]` | Record an entry. `--team` defaults to `team.id`. |
| `list [--team TEAM_ID] [--race RACE_ID]` | List entries, optionally for one team or race |
| `propose --term TERM [--term TERM ...] [--team TEAM_ID]` | Find stored competitor names that match, confirm which to record as entries, and optionally save a name as an alias |

When `team.id` is set, a live capture records its own entry. Use `entries` for past
races.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, tests, and the local
stack.

## Upgrading

To upgrade to a new major version, follow the steps in
[docs/UPGRADING.md](docs/UPGRADING.md).
