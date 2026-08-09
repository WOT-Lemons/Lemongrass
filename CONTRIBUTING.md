# Contributing

This project uses [`uv`](https://docs.astral.sh/uv/) for dependency management and
running. Use `uv run ...` rather than invoking `.venv` paths directly.

## Running the tests

```bash
uv run pytest
uv run ruff check
```

CI runs lint separately from the test suite, so a green `pytest` does not imply a
green lint — run both before pushing.

## Running the PostgreSQL-backed tests

Tests that touch PostgreSQL are skipped unless `LEMONGRASS_TEST_DATABASE_URL` is
set. Point it at a **throwaway** database on a non-default port — never at the
local-testing stack's Postgres (below), which uses the same port, credentials,
and database name that CI's URL does, and whose data these tests would destroy:

```bash
docker run --rm -d --name lg-test-pg -p 127.0.0.1:55432:5432 \
  --health-cmd='pg_isready -U lemongrass -d lemongrass' \
  --health-interval=1s --health-timeout=5s --health-retries=30 \
  -e POSTGRES_USER=lemongrass -e POSTGRES_PASSWORD=local-dev-password \
  -e POSTGRES_DB=lemongrass postgres:18

until [ "$(docker inspect -f '{{.State.Health.Status}}' lg-test-pg)" = healthy ]; do sleep 1; done
```

`docker run -d` returns before the server finishes initializing, so wait for the
health check rather than starting the tests straight away.

```bash
export LEMONGRASS_TEST_DATABASE_URL=postgresql+psycopg://lemongrass:local-dev-password@localhost:55432/lemongrass
export LEMONGRASS_TEST_DB_ALLOW_WIPE=1
uv run pytest
```

`LEMONGRASS_TEST_DB_ALLOW_WIPE=1` is a required, separate opt-in: the `clean_db`
fixture drops and recreates the target database's `public` schema before every
test that uses it, and refuses to do so without this confirmation. Tear the
container down when you're done:

```bash
docker rm -f lg-test-pg
```

## Testing against a local InfluxDB stack

When prod is unavailable, run the full pipeline against a local InfluxDB.

1. Start the stack (InfluxDB on `:8086`, Grafana on `:3000`, plus the emulated
   OBD car — an ELM327 emulator + `telem`):

   ```bash
   docker compose -f local-testing/docker-compose.yml up --build -d
   ```

   `--build` builds the `elm327` and `telem` images on first start (and after
   changes). First start creates org `lemongrass`, a pinned operator token
   (`local-dev-token`), and the `laps`, `races`, `race_sessions`, `telem`,
   `pisugar`, and legacy `stats_252/autogen` buckets. These are non-secret,
   local-only values.

   A one-shot `grafana-library-panels` container also runs on every `up`. Grafana
   provisions dashboards and datasources from files but has no file provisioning
   for library panels, so this container pushes them into Grafana over its HTTP
   API instead; it exits with code 0 once done, which is normal. The models it
   pushes, under `local-testing/grafana/library_panels/`, are copies of the
   prod library panels — update both together whenever either side changes, or
   the local stack stops reproducing prod.

2. Point the CLI at the local stack by sourcing the committed app env:

   ```bash
   set -a && source local-testing/.env.local && set +a
   ```

   This sets `LEMONGRASS_CONFIG` to `local-testing/lemongrass.toml` — which points the CLI at
   the local InfluxDB (`url = http://localhost:8086`, `org = lemongrass`) — and the secret
   `INFLUX_TELEMETRY_TOKEN=local-dev-token`. (Without a config file the CLI uses its built-in
   defaults, `https://influxdb.focism.com` / `focism`.)

3. Seed data by running a one-shot pull from RaceMonitor. This calls the RaceMonitor API, so it
   needs your own RaceMonitor API token — this requires a Race Monitor account
   with API access; get a token at <https://www.race-monitor.com/Home/API>. The
   `-n`/`--network` flag does a one-shot pull of a completed race and writes it to the local
   buckets. The API is rate-limited to ~6 req/min, so pulling a full race takes a few minutes:

   ```bash
   RACEMONITOR_TOKEN=<token> uv run lemongrass laps 161198 -n
   ```

4. Inspect the result:

   ```bash
   uv run lemongrass races list
   ```

   or open Grafana at http://localhost:3000 (login `admin` / `local-dev-password`)
   — the InfluxDB datasource and lemongrass dashboards (**laps**, **race-control**, **standings**)
   are pre-provisioned. The **telegraf** dashboard and the OBD panels in **race-control** render
   emulated telemetry from the `telem` service, which starts with the default stack (see
   "Emulated OBD telemetry" below). **car252-pisugar-ups** stays empty — it needs PiSugar UPS
   hardware.

5. Tear down (add `-v` to wipe data and re-trigger bucket init next start):

   ```bash
   docker compose -f local-testing/docker-compose.yml down
   ```

   Storage is persistent named Docker volumes, so a plain `down` keeps your seeded
   data across restarts; pass `-v` only when you want a clean reset.

### Emulated OBD telemetry

The `telem` service normally reads a physical ELM327 adapter. For local testing, a
virtual ELM327 ([`ELM327-emulator`](https://pypi.org/project/ELM327-emulator/)) runs as
the `elm327` service and telem connects to it over TCP (`socket://elm327:35000`). Both
start as part of the default stack (step 1), so telem streams emulated OBD data with no
extra flags.

telem writes emulated OBD data to the `telem` bucket (tagged with the car VIN);
PiSugar host metrics go to the `pisugar` bucket (tagged with `host`). Both are
created by the InfluxDB init scripts. The `telem` and `pisugar` buckets and
their DBRPs are created only on a **fresh** InfluxDB volume, so if you had a
stack running before they were added, recreate the volume:

```bash
docker compose -f local-testing/docker-compose.yml down -v
docker compose -f local-testing/docker-compose.yml up --build -d
```

(`pisugar-monitor` remains out of scope for local testing — it needs PiSugar UPS hardware.)

### Running the OBD integration tests

The default `uv run pytest` run is mock-only and fast. The integration tests drive telem's
real OBD path against the emulator and are excluded by the `integration` marker. The
emulator is installed imperatively (kept out of `uv.lock`):

```bash
uv sync
bash local-testing/install-emulator.sh
uv run --no-sync pytest -m integration tests/
```

`--no-sync` prevents `uv run` from pruning the imperatively-installed emulator. CI runs
this same sequence in the `integration` job.

**Unreachable InfluxDB:** if the server is down or unreachable, commands retry a few
times, then exit non-zero with a one-line `cannot reach InfluxDB at …` message
rather than a traceback.
