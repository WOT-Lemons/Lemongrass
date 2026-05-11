# Lemongrass

Open source car telemetry for 24 Hours of Lemons.

## Requirements

- Raspberry Pi running Raspberry Pi OS
- PiSugar 3 UPS
- USB OBD-II adapter
- An InfluxDB instance running v2.x
- Grafana to visualize the data

## Raspberry Pi Services

The services on the pi are managed via [docker-compose.yml](docker-compose.yml).

| Service | Description |
| --- | --- |
| telem | Monitors car data via OBD-II USB adapter |
| pisugar-monitor | Monitors PiSugar 3 UPS |
| telegraf | Monitors Raspberry Pi OS |

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
docker pull ghcr.io/wot-lemons/lemongrass-laps:latest
```

### Live Race

Run it, passing your credentials via an env file (see `.env.sample`).

To pin to a specific version instead of `latest`, replace the tag (e.g. `1.2.3`). Available tags are listed at `ghcr.io/wot-lemons/lemongrass-laps`.

Generic example:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass-laps:latest RACE_ID CAR_NUMBER -m -n
```

Real example:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass-laps:latest 164732 372 -m -n
```

```plain
2026-05-10 17:31:26,577 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/RaceDetails "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
Race 164732
The Sausage Fest 2026 Started: 2026-05-08 14:00:00
Road America   Ends: 2026-05-10 23:00:00
--------------------------------------------------------------------------------
2026-05-10 17:31:26,628 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/IsLive "HTTP/1.1 200 OK"
2026-05-10 17:31:26,650 - INFO - Race 164732 is currently live.
2026-05-10 17:31:26,695 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Live/GetSession "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
2026-05-10 17:31:26,700 - INFO - Current overall rankings.
--------------------------------------------------------------------------------
Empty DataFrame
Columns: [Pos., #, Name, Laps, Transponder]
Index: []
--------------------------------------------------------------------------------
2026-05-10 17:31:26,896 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Live/GetRacer "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
Team: Lemonaid Car Number: 372  Transponder: 7150207
Best Position: 2
Final Position: 1
Total Laps: 192
Best Lap: 22
Best Lap Time: 00:02:45.024
Total Time: 10:43:15.928
--------------------------------------------------------------------------------
Lap Position      LapTime FlagStatus    TotalTime
 -2       94 00:02:56.397      Green 00:03:58.988
 -1       94 00:03:06.094      Green 00:07:05.082
  1       81 00:02:49.854      Green 00:12:50.376
  2       80 00:02:56.454      Green 00:15:46.830
  3       77 00:02:52.749      Green 00:18:39.579
  4       74 00:02:56.466      Green 00:21:36.045
  5       70 00:03:02.744      Green 00:24:38.789
 ...
190        1 00:02:52.168      Green 10:37:24.996
191        1 00:02:50.178      Green 10:40:15.174
192        1 00:03:00.754      Green 10:43:15.928
--------------------------------------------------------------------------------
2026-05-10 17:31:26,915 - INFO - Writing laps to influx...
2026-05-10 17:31:32,287 - INFO - All lap data written successfully
--------------------------------------------------------------------------------
2026-05-10 17:31:32,288 - INFO - Monitoring car 372...
--------------------------------------------------------------------------------
2026-05-10 17:32:02,695 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Live/GetRacer "HTTP/1.1 200 OK"
```

### Completed Race

You can retrieve info for a completed race too.

Generic example:

```shell
docker run --rm -it \
  --env-file .env \
  ghcr.io/wot-lemons/lemongrass-laps:latest \
  RACE_ID CAR_NUMBER
```

Real example:

```shell
docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass-laps:latest 164732 121
```

```plain
2026-05-11 18:45:01,901 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/RaceDetails "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
Race 164732
The Sausage Fest 2026	Started: 2026-05-08 10:00:00
Road America			Ends: 2026-05-10 19:00:00
--------------------------------------------------------------------------------
2026-05-11 18:45:01,944 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Race/IsLive "HTTP/1.1 200 OK"
2026-05-11 18:45:01,948 - INFO - Race 164732 is not live. Monitor mode disabled.
2026-05-11 18:45:01,994 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionsForRace "HTTP/1.1 200 OK"
2026-05-11 18:45:02,056 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-05-11 18:45:02,108 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-05-11 18:45:02,161 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-05-11 18:45:02,164 - INFO - Rate limited: sleeping 59.65s [6/6 slots used over 60s window; oldest request 0.35s ago]
2026-05-11 18:46:01,913 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
2026-05-11 18:46:02,053 - INFO - HTTP Request: POST https://api.race-monitor.com/v2/Results/SessionDetails "HTTP/1.1 200 OK"
--------------------------------------------------------------------------------
2026-05-11 18:46:02,222 - INFO - Current overall rankings.
--------------------------------------------------------------------------------
Pos.   #                         Name Laps Transponder
   1 121        The Patriot race team  259         B-3
   2  36                        TATAS  257     1070156
   3   3                Badger Racing  254     4510781
   4 546           It’s Around Here….  251        B-24
   5  53            Team Fiery Death!  249        A-17
   6 223        Adventurers Anonymous  246        B-13
   7 307       Gutless Cutlass Racing  246        B-15
   8 770                  Team Mayhem  244        B-26
   9 123                  Polka Kings  244         B-4
  10 949         Scheißmeister Racing  243        B-32
...
  95 248                   3 Gena + 2    7        B-14
  96 502                     SlowZuki    3        B-20
  97  83 Dunning-Kruger Effect Racing         13030906
  98 392              GWOT VET 49 (R)             B-31
  99 178                  Four Idiots  -22    12947397
--------------------------------------------------------------------------------
Team:       	Car Number: 121 	Transponder: B-3
Best Position:	6
Final Position:	1
Total Laps:	259
Best Lap:	154
Best Lap Time:	00:02:48.657
Total Time:	14:11:40.349
--------------------------------------------------------------------------------
Lap      LapTime Position FlagStatus    TotalTime
  1 00:03:06.447       12      Green 00:04:13.066
  2 00:03:04.366       11      Green 00:07:17.432
  3 00:03:12.643        8      Green 00:10:30.075
...
258 00:03:00.218        1      Green 14:08:27.761
259 00:03:12.588        1     Finish 14:11:40.349
--------------------------------------------------------------------------------
2026-05-11 18:46:02,229 - INFO - Writing laps to influx...
2026-05-11 18:46:11,310 - INFO - All lap data written successfully
--------------------------------------------------------------------------------
```
