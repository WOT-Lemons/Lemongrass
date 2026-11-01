# Contributing

This project uses [`uv`](https://docs.astral.sh/uv/). Run all commands with
`uv run`. Do not run programs from `.venv` directly.

## Running the tests

```bash
uv run pytest
uv run ruff check
```

CI runs the tests and the linter in separate jobs. Before you push, run both
commands.

## Running the PostgreSQL tests

The PostgreSQL tests run only if `LEMONGRASS_TEST_DATABASE_URL` is set. Set it to a
temporary database on a non-default port.

> **Warning:** Do not use the local stack's PostgreSQL on port 5432. It has the same
> credentials and database name as the test database, and these tests delete its
> data.

1. Start a temporary PostgreSQL server. Wait until it is healthy:

   ```bash
   docker run --rm -d --name lg-test-pg -p 127.0.0.1:55432:5432 \
     --health-cmd='pg_isready -U lemongrass -d lemongrass' \
     --health-interval=1s --health-timeout=5s --health-retries=30 \
     -e POSTGRES_USER=lemongrass -e POSTGRES_PASSWORD=local-dev-password \
     -e POSTGRES_DB=lemongrass postgres:18

   until [ "$(docker inspect -f '{{.State.Health.Status}}' lg-test-pg)" = healthy ]; do sleep 1; done
   ```

2. Run the tests:

   ```bash
   export LEMONGRASS_TEST_DATABASE_URL=postgresql+psycopg://lemongrass:local-dev-password@localhost:55432/lemongrass
   export LEMONGRASS_TEST_DB_ALLOW_WIPE=1
   uv run pytest
   ```

   Before each database test, the `clean_db` fixture deletes and creates the
   `public` schema again. You must set `LEMONGRASS_TEST_DB_ALLOW_WIPE=1`. If it is
   not set, the database tests fail.

3. Remove the container:

   ```bash
   docker rm -f lg-test-pg
   ```

## Running the local stack

The local stack runs the full pipeline on your computer, without production
services. All credentials in the stack are for local use only and are not secret.

| Service | Purpose |
| --- | --- |
| `influxdb` | InfluxDB 2 on `localhost:8086` |
| `postgres` | PostgreSQL 18 on `localhost:5432` |
| `db-schema` | Applies the database schema (`lemongrass db upgrade`), then stops |
| `grafana` | Grafana on `localhost:3000`, with the datasources and dashboards |
| `grafana-library-panels` | Loads the library panels into Grafana, then stops |
| `elm327` | ELM327 OBD-II emulator |
| `telem` | `lemongrass telem`, connected to the emulator |

1. Start the stack:

   ```bash
   docker compose -f local-testing/docker-compose.yml up --build -d
   ```

   `--build` builds the local images. On the first start, and after you change the
   code, use `--build`.

   On a new InfluxDB volume, the init scripts create these items:

   - Organization `lemongrass`
   - Token `local-dev-token`
   - Buckets `laps`, `telem`, `pisugar`, and `system_metrics/autogen`
   - Legacy buckets `races` and `race_sessions`. lemongrass does not write new data
     to these buckets. Only `lemongrass db import-legacy` and
     `lemongrass races prune` use them.

   Each time the stack starts, `db-schema` applies all migrations, loads the track
   data, and stops with exit code 0. Grafana starts only if `db-schema` completes
   successfully. If `db-schema` fails with `Can't locate revision`, a newer
   checkout migrated the database. To fix this, start the stack from that checkout. Alternatively, delete
   all data with `down -v`.

   `grafana-library-panels` also stops with exit code 0. Grafana cannot load library
   panels from files, so this container loads them through the Grafana HTTP API. The
   panels in `local-testing/grafana/library_panels/` are copies of the production
   library panels. If you change one copy, make the same change to the other copy.

2. Configure your shell for the local stack:

   ```bash
   set -a && source local-testing/.env.local && set +a
   ```

   This command sets these variables:

   - `LEMONGRASS_CONFIG`, to `local-testing/lemongrass.toml`. This file points the
     CLI to the local InfluxDB and PostgreSQL. If you do not set
     `LEMONGRASS_CONFIG`, the CLI uses the production defaults.
   - `INFLUX_TELEMETRY_TOKEN`
   - `LEMONGRASS_DB_PASSWORD`

3. Import a race from RaceMonitor. You must have a RaceMonitor API token. To get a
   token, go to <https://www.race-monitor.com/Home/API>.

   ```bash
   RACEMONITOR_TOKENS=<token> uv run lemongrass laps 161198 -n
   ```

   The API accepts approximately 6 requests per minute, so the import can take
   several minutes.

4. Make sure that the import is correct:

   ```bash
   uv run lemongrass races list
   ```

   You can also open Grafana at <http://localhost:3000>. The user name is `admin`,
   and the password is `local-dev-password`. The stack includes these dashboards:

   | Dashboard | Data |
   | --- | --- |
   | **laps** | Lap data |
   | **race-control** | Race data. The OBD panels show data from the emulator. |
   | **standings** | Standings |
   | **year-over-year** | Results for one team at one venue, by year |
   | **car252-pisugar-ups** | Empty. This dashboard needs PiSugar hardware. |
   | **telegraf** | Empty. The stack does not include a Telegraf agent. |

5. Stop the stack:

   ```bash
   docker compose -f local-testing/docker-compose.yml down
   ```

   Docker keeps the data in named volumes. To delete all InfluxDB, PostgreSQL, and
   Grafana data, add `-v`.

### Emulated OBD telemetry

Usually, `telem` reads a physical ELM327 adapter. In the local stack, it connects to
the [`ELM327-emulator`](https://pypi.org/project/ELM327-emulator/) over TCP at
`socket://elm327:35000`. It writes the OBD data to the `telem` bucket, with the car
VIN as a tag.

The init scripts create the `telem` and `pisugar` buckets only on a new InfluxDB
volume. If your volume is older than these buckets, delete the volume and start the
stack again:

```bash
docker compose -f local-testing/docker-compose.yml down -v
docker compose -f local-testing/docker-compose.yml up --build -d
```

The local stack does not include `pisugar-monitor`, because it needs PiSugar
hardware.

### Running the OBD integration tests

`uv run pytest` runs only the mocked tests. The integration tests have the
`integration` marker. They run the real OBD code in `telem` against the emulator.
The install script installs the emulator outside `uv.lock`.

```bash
uv sync
bash local-testing/install-emulator.sh
uv run --no-sync pytest -m integration tests/
```

`--no-sync` prevents `uv run` from removing the emulator. The `integration` job in
CI runs the same commands.

## Troubleshooting

**InfluxDB is not available:** The commands try again a few times. Then they exit
with a non-zero code and the message `cannot reach InfluxDB at …`.
