# Lemongrass

Open source car telemetry for 24 Hours of Lemons.

## Requirements

- Raspberry Pi running Raspberry Pi OS
- PiSugar 3 UPS
- USB OBD-II adapter
- An InfluxDB instance running v2.x
- Grafana to visualize the data

## Services

This repo provides two long-running services that run on the pi:

| Service | Description |
| --- | --- |
| telem | Monitors car data via OBD-II USB adapter |
| pisugar-monitor | Monitors PiSugar 3 UPS |

Deployment and orchestration (docker-compose, telegraf for OS metrics, etc.) are managed in the deployment (IaC) repository, not here.

## Lap Data

### Setup

1. Get a Race Monitor API token <https://www.race-monitor.com/Home/API>

2. Add your token to a `.env` file (see `.env.sample`) and source it:

```shell
source .env
```

3. Get your race ID

We need a Race ID to get information for. Head to <https://www.race-monitor.com/Live/Race> while your race is live to get this easily from the URL.

![Image of Race ID in URL bar](https://i.imgur.com/1FQNvSb.png)

4. Run the tool

Pull the latest image:

```shell
docker pull ghcr.io/wot-lemons/lemongrass:latest
```

### Live Race

**Docker** — pass your credentials via an env file (see `.env.sample`). To pin to a specific version instead of `latest`, replace the tag (e.g. `1.2.3`). Available tags are listed at `ghcr.io/wot-lemons/lemongrass`.

> **Note:** `CAR_NUMBER` is required for live/monitor mode. Omit it for completed races to write laps for all competitors (fieldwide backfill).

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest lemongrass laps RACE_ID CAR_NUMBER -m -n
```

**pip** — install from PyPI and source your `.env` first (step 2 above):

```shell
pip install lemongrass
lemongrass laps RACE_ID CAR_NUMBER -m -n
```

**uv** — install from PyPI as a tool and source your `.env` first (step 2 above):

```shell
uv tool install lemongrass
lemongrass laps RACE_ID CAR_NUMBER -m -n
```

Or run ephemerally without installing:

```shell
uvx lemongrass laps RACE_ID CAR_NUMBER -m -n
```

> **Graceful exit:** Press Ctrl-C at any time to stop monitoring cleanly (exits 130). The monitor also exits automatically when the race ends.

Real example:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest lemongrass laps 166811 13 -m -n
```

```plain
2026-06-19 20:52:41,057 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/RaceDetails "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
Race 166811
Fast Friday Started: 2026-06-19 16:00:00
Seekonk Speedway   Ends: 2026-06-20 01:00:00
--------------------------------------------------------------------------------
2026-06-19 20:52:41,151 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/IsLive "HTTP/1.1 200 OK"
2026-06-19 20:52:41,152 - INFO - Race 166811 is currently live.
2026-06-19 20:52:41,251 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Live/GetSession "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
2026-06-19 20:52:41,252 - INFO - Current overall rankings.
--------------------------------------------------------------------------------
Empty DataFrame
Columns: [Pos., #, Class, Class Pos., Name, Laps, Transponder]
Index: []
--------------------------------------------------------------------------------
2026-06-19 20:52:41,365 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Live/GetRacer "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
Team: Jacob Burns Car Number: 13   Class: Legends Transponder: 13775638
Best Position: 2
Final Position: 1
Final Class Position: 1
Total Laps: 16
Best Lap: 6
Best Lap Time: 00:00:14.165
Total Time: 00:05:43.405
--------------------------------------------------------------------------------
Lap Position      LapTime FlagStatus    TotalTime
  1        8 00:00:14.572      Green 00:00:23.097
  2        5 00:00:14.458      Green 00:00:37.555
  3        4 00:00:14.389      Green 00:00:51.944
  4        3 00:00:14.258      Green 00:01:06.202
  5        2 00:00:14.313      Green 00:01:20.515
  6        2 00:00:14.165      Green 00:01:34.680
  7        2 00:00:14.258      Green 00:01:48.938
  8        1 00:00:14.391      Green 00:02:03.329
  9        1 00:00:14.437      Green 00:02:17.766
 10        1 00:00:14.299      Green 00:02:32.065
 11        1 00:00:14.248      Green 00:02:46.313
 12        1 00:00:14.260      Green 00:04:46.328
 13        1 00:00:14.194      Green 00:05:00.522
 14        1 00:00:14.251      Green 00:05:14.773
 15        1 00:00:14.322      Green 00:05:29.095
 16        1 00:00:14.310      Green 00:05:43.405
 17        1 00:00:14.291      Green 00:05:57.696
--------------------------------------------------------------------------------
2026-06-19 20:52:41,369 - INFO - Monitoring car 13...
--------------------------------------------------------------------------------
Lap Position      LapTime FlagStatus    TotalTime
  1        8 00:00:14.572      Green 00:00:23.097
  2        5 00:00:14.458      Green 00:00:37.555
  3        4 00:00:14.389      Green 00:00:51.944
  4        3 00:00:14.258      Green 00:01:06.202
  5        2 00:00:14.313      Green 00:01:20.515
  6        2 00:00:14.165      Green 00:01:34.680
  7        2 00:00:14.258      Green 00:01:48.938
  8        1 00:00:14.391      Green 00:02:03.329
  9        1 00:00:14.437      Green 00:02:17.766
 10        1 00:00:14.299      Green 00:02:32.065
 11        1 00:00:14.248      Green 00:02:46.313
 12        1 00:00:14.260      Green 00:04:46.328
 13        1 00:00:14.194      Green 00:05:00.522
 14        1 00:00:14.251      Green 00:05:14.773
 15        1 00:00:14.322      Green 00:05:29.095
 16        1 00:00:14.310      Green 00:05:43.405
 17        1 00:00:14.291      Green 00:05:57.696
```

### Completed Race

You can retrieve info for a completed race too. Omit `CAR_NUMBER` to write laps for all competitors in the field (fieldwide backfill mode).

**Docker:**

```shell
# Single car
docker run --rm -it \
  --env-file .env \
  ghcr.io/wot-lemons/lemongrass:latest \
  lemongrass laps RACE_ID CAR_NUMBER

# Full field
docker run --rm -it \
  --env-file .env \
  ghcr.io/wot-lemons/lemongrass:latest \
  lemongrass laps RACE_ID
```

**pip / uv:**

```shell
lemongrass laps RACE_ID CAR_NUMBER   # single car
lemongrass laps RACE_ID              # full field
```

> **Persisting CSV output:** `lemongrass laps -o` writes a `.csv` to the container's
> working directory (`/data`). The container runs as a non-root user, so that write
> stays inside the container and is lost on exit unless you mount a writable directory
> at `/data`. Bind mounts keep their host ownership, so pass `--user` to run as your
> host user — the CSV then lands in `./out` owned by you:
>
> ```shell
> mkdir -p out
> docker run --rm -it --env-file .env \
>   --user "$(id -u):$(id -g)" \
>   -v "$(pwd)/out:/data" \
>   ghcr.io/wot-lemons/lemongrass:latest \
>   lemongrass laps RACE_ID CAR_NUMBER -o
> ```

Real example:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest lemongrass laps 166429 852
```

```plain
2026-06-19 20:46:17,391 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/RaceDetails "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
Race 166429
The B.F.E. GP 2026 Started: 2026-06-12 10:00:00
High Plains Raceway   Ends: 2026-06-14 19:30:00
--------------------------------------------------------------------------------
2026-06-19 20:46:17,487 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/IsLive "HTTP/1.1 200 OK"
2026-06-19 20:46:17,488 - INFO - Race 166429 is not live. Monitor mode disabled.
2026-06-19 20:46:17,587 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionsForRace "HTTP/1.1 200 OK"
2026-06-19 20:46:17,692 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:46:17,797 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:46:17,945 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:46:17,948 - INFO - Rate limited: sleeping 59.34s [6/6 slots used over 60s window; oldest request 0.66s ago]
2026-06-19 20:47:17,422 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:47:17,570 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:47:17,794 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:47:18,090 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:47:18,314 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-06-19 20:47:18,626 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
2026-06-19 20:47:18,913 - INFO - Current overall rankings.
--------------------------------------------------------------------------------
Pos.   # Class  Class Pos.                             Name Laps Transponder
   1 852     A           1              Rusty Bottom Racing  351        D-34
   2   4     A           2         Sew So Fast - GC edition  349         C-2
   3 380     A           3                 Vice City Racing  349        D-20
   4   6     A           4               Stay Classy Racing  337         C-3
   5 177     B           1                     DadBodCarMod  336        D-11
   6  49     B           2             Broken Spokes Racing  334        C-20
   7 101     A           5            Smokey and the Bandit  333         D-1
   8  11     A           6          Whiskey + Doughnuts =É.  333      165002
   9 300     A           7                     World War Zx  331        D-14
  10 501     A           8            Vistabeam Racing Team  330        D-26
...
  75 779     B          33     Poorsche Club of America (R)   59        D-31
  76 333     B          34           Green Beret RacingÉ.II   53        D-19
  77  73     B          35                      Team HonDuh   23     5632016
  78  28     A          27             Liquid Mechanics (R)   11     6201404
  79 150     A          28                       NTD Racing    9         D-8
  80  35     C          17                         Passhark    8        C-13
  81  17     C          18                     Haiku Racing              C-6
  82  72                 1      Enforcement Motorsports (R)             C-25
--------------------------------------------------------------------------------
Team:        Car Number: 852  Class: A Transponder: D-34
Best Position: 7
Final Position: 1
Final Class Position: 1
Total Laps: 351
Best Lap: 214
Best Lap Time: 00:02:12.483
Total Time: 14:36:02.587
--------------------------------------------------------------------------------
Lap      LapTime Position FlagStatus    TotalTime
  1 00:02:25.450        6      Green 06:28:04.193
  2 00:02:24.449        5      Green 06:30:28.642
  3 00:02:24.332        5      Green 06:32:52.974
  4 00:02:26.745        5      Green 06:35:19.719
  5 00:02:23.508        5      Green 06:37:43.227
  6 00:02:23.444        4      Green 06:40:06.671
  7 00:02:24.004        3      Green 06:42:30.675
  8 00:02:43.345        3      Green 06:45:14.020
  9 00:36:18.582        7      Green 07:21:32.602
 10 00:02:23.425        7      Green 07:23:56.027
...
340 00:02:21.490        1      Green 14:09:37.216
341 00:02:26.332        1      Green 14:12:03.478
342 00:02:26.332        1      Green 14:14:29.810
343 00:02:24.485        1      Green 14:16:54.224
344 00:02:24.068        1      Green 14:19:18.222
345 00:02:21.403        1      Green 14:21:39.553
346 00:02:27.229        1      Green 14:24:06.782
347 00:02:28.195        1      Green 14:26:34.906
348 00:02:24.841        1      Green 14:28:59.674
349 00:02:20.867        1      Green 14:31:20.472
350 00:02:21.190        1      Green 14:33:41.592
351 00:02:21.065        1     Finish 14:36:02.587
--------------------------------------------------------------------------------
```

## Race Management

The `races` subcommand provides tools for inspecting and managing race data stored in InfluxDB.

```shell
lemongrass races <subcommand> [args]
```

| Subcommand | Description |
| ------------ | ------------- |
| `list` | Show all stored races with lap counts and schema status |
| `prune RACE_ID...` | Delete all data for one or more races from InfluxDB |
| `backfill` | Run historical backfill for all tracked races (delegates to `lemongrass race-backfill`; use `--help` for all options) |
| `diagnose RACE_ID CAR_NUMBER` | Compare RaceMonitor vs InfluxDB lap counts for a specific car |

### Examples

```shell
# List all stored races and their schema version status
lemongrass races list

# Delete a race (prompts for confirmation)
lemongrass races prune 144185

# Delete multiple races at once, skipping confirmation
lemongrass races prune 144185 120037 --yes

# Diagnose a lap count mismatch for car 252 in race 144185
lemongrass races diagnose 144185 252
```

### Backfill Options

The `backfill` subcommand delegates to `lemongrass race-backfill` and supports these flags:

| Flag | Description |
| ------ | ------------- |
| `--dry-run` | Print what would be backfilled without writing anything |
| `--force` | Re-backfill every race, even those already complete and current |
| `--upgrade-stored` | Re-process laps already in InfluxDB whose `schema_version` is older than current — faster than `--force` because it skips re-fetching from RaceMonitor |
| `--validate` | Check that every expected race has metadata and at least one lap in InfluxDB |
| `--start-date YYYY-MM-DD` | Only include races starting on/after this date (default: 2017-01-01) |

> **Note:** `--upgrade-stored` is mutually exclusive with `--start-date` and `--validate`; combine it with `--force` to also re-fetch races already at the current schema.

### Session Tracking

All lap points written to InfluxDB include a `session_id` tag corresponding to the RaceMonitor session ID. In Flux queries you can filter by `session_id` to isolate specific race segments (e.g. Day 1 vs. Day 2). Session metadata is stored in the `race_sessions` bucket.

## Configuration

lemongrass is configured by an optional TOML file named by the `LEMONGRASS_CONFIG` environment
variable, falling back to built-in defaults. Secrets (`INFLUX_TELEMETRY_TOKEN`,
`RACEMONITOR_TOKENS`) are supplied via the environment and referenced by `*_env` keys in the
file — environment variables are not used for any non-secret setting. Copy
`lemongrass.toml.sample` to get started. See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for
the full key reference.

## Contributing

For development setup, running the test suite, and testing against a local
InfluxDB stack instead of prod, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Upgrading from v1.x

As of v2.0.0, the individual entry points (`laps`, `telem`, `race-backfill`, `pisugar-monitor`, `race-diagnose`) were replaced by a single `lemongrass` command. If you have the old package installed, update and prefix commands with `lemongrass`:

| Before | After |
| -------- | ------- |
| `laps RACE_ID CAR_NUMBER` | `lemongrass laps RACE_ID CAR_NUMBER` |
| `telem` | `lemongrass telem` |
| `race-backfill` | `lemongrass race-backfill` or `lemongrass races backfill` |
| `pisugar-monitor` | `lemongrass pisugar-monitor` |
| `race-diagnose` | `lemongrass race-diagnose` or `lemongrass races diagnose` |
