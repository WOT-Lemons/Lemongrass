# Contributing

This project uses [`uv`](https://docs.astral.sh/uv/). Run everything through
`uv run`. Do not call `.venv` paths directly.

## Running the tests

```bash
uv run pytest
uv run ruff check
```

CI runs lint and tests as separate jobs. Run both before you push.

## Running the PostgreSQL tests

Tests that use PostgreSQL are skipped unless `LEMONGRASS_TEST_DATABASE_URL` is set.
Point it at a throwaway database on a non-default port. Do not point it at the
local stack's Postgres on port 5432. It uses the same credentials and database name
as the test server, and these tests delete its data.

Start a throwaway server and wait until it is healthy:

```bash
docker run --rm -d --name lg-test-pg -p 127.0.0.1:55432:5432 \
  --health-cmd='pg_isready -U lemongrass -d lemongrass' \
  --health-interval=1s --health-timeout=5s --health-retries=30 \
  -e POSTGRES_USER=lemongrass -e POSTGRES_PASSWORD=local-dev-password \
  -e POSTGRES_DB=lemongrass postgres:18

until [ "$(docker inspect -f '{{.State.Health.Status}}' lg-test-pg)" = healthy ]; do sleep 1; done
```

Run the tests:

```bash
export LEMONGRASS_TEST_DATABASE_URL=postgresql+psycopg://lemongrass:local-dev-password@localhost:55432/lemongrass
export LEMONGRASS_TEST_DB_ALLOW_WIPE=1
uv run pytest
```

`LEMONGRASS_TEST_DB_ALLOW_WIPE=1` is a separate, required opt-in. Before each
database test, the `clean_db` fixture drops and recreates the `public` schema. It
refuses to run without this variable.

Remove the container when you are done:

```bash
docker rm -f lg-test-pg
```

## Running the local stack

The local stack runs the full pipeline without prod. All credentials in it are
non-secret and local-only.

| Service | Purpose |
| --- | --- |
| `influxdb` | InfluxDB 2 on `localhost:8086` |
| `postgres` | PostgreSQL 18 on `localhost:5432` |
| `db-schema` | One-shot: applies the database schema (`lemongrass db upgrade`) |
| `grafana` | Grafana on `localhost:3000`, with datasources and dashboards provisioned |
| `grafana-library-panels` | One-shot: loads library panels into Grafana |
| `elm327` | ELM327 OBD-II emulator |
| `telem` | `lemongrass telem`, reading from the emulator |

1. Start the stack:

   ```bash
   docker compose -f local-testing/docker-compose.yml up --build -d
   ```

   `--build` builds the local images on first start and after code changes.

   On a new InfluxDB volume, the init scripts create org `lemongrass`, token
   `local-dev-token`, and these buckets: `laps`, `telem`, `pisugar`,
   `system_metrics/autogen`, and the legacy `races` and `race_sessions`. The legacy
   buckets are not written to. They exist only for `lemongrass db import-legacy`.

   `db-schema` runs every migration and loads the track data on each `up`, then
   exits with code 0. Grafana waits for it to finish. If it fails with
   `Can't locate revision`, the database was migrated by a newer checkout. Run the
   stack from that checkout, or reset all data with `down -v`.

   `grafana-library-panels` also exits with code 0 when done. Grafana cannot
   provision library panels from files, so this container loads them through the
   HTTP API. The panels in `local-testing/grafana/library_panels/` are copies of
   the prod library panels. When you change one side, change the other.

2. Configure your shell for the local stack:

   ```bash
   set -a && source local-testing/.env.local && set +a
   ```

   This sets `LEMONGRASS_CONFIG` to `local-testing/lemongrass.toml` (local InfluxDB
   and Postgres), plus `INFLUX_TELEMETRY_TOKEN` and `LEMONGRASS_DB_PASSWORD`. Without
   a config file, the CLI uses its prod defaults.

3. Load a race from RaceMonitor. You need a RaceMonitor API token
   (<https://www.race-monitor.com/Home/API>). The API allows about 6 requests per
   minute, so a full race takes a few minutes:

   ```bash
   RACEMONITOR_TOKENS=<token> uv run lemongrass laps 161198 -n
   ```

4. Check the result:

   ```bash
   uv run lemongrass races list
   ```

   Or open Grafana at <http://localhost:3000> (user `admin`, password
   `local-dev-password`). The provisioned dashboards are **laps**, **race-control**,
   **standings**, **year-over-year**, **car252-pisugar-ups**, and **telegraf**. The
   OBD panels in **race-control** show data from the emulator. **car252-pisugar-ups**
   stays empty because it needs PiSugar hardware. **telegraf** stays empty because
   the stack has no Telegraf agent.

5. Stop the stack:

   ```bash
   docker compose -f local-testing/docker-compose.yml down
   ```

   Data is kept in named volumes. Add `-v` to delete all InfluxDB, Postgres, and
   Grafana data and start fresh next time.

### Emulated OBD telemetry

`telem` normally reads a physical ELM327 adapter. In the local stack, it connects to
the [`ELM327-emulator`](https://pypi.org/project/ELM327-emulator/) over TCP at
`socket://elm327:35000`. It writes OBD data to the `telem` bucket, tagged with the
car VIN.

The `telem` and `pisugar` buckets are created only on a new InfluxDB volume. If your
volume is older than these buckets, recreate it:

```bash
docker compose -f local-testing/docker-compose.yml down -v
docker compose -f local-testing/docker-compose.yml up --build -d
```

`pisugar-monitor` is not part of the local stack because it needs PiSugar hardware.

### Running the OBD integration tests

`uv run pytest` runs only the fast, mocked tests. The integration tests run telem's
real OBD code against the emulator and use the `integration` marker. The emulator
is installed separately so that it stays out of `uv.lock`:

```bash
uv sync
bash local-testing/install-emulator.sh
uv run --no-sync pytest -m integration tests/
```

`--no-sync` stops `uv run` from removing the emulator. CI runs the same commands in
the `integration` job.

## Troubleshooting

**InfluxDB unreachable:** commands retry a few times, then exit non-zero with
`cannot reach InfluxDB at …`.
