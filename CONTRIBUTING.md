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

## Testing against a local InfluxDB stack

When prod is unavailable, run the full pipeline against a local InfluxDB.

1. Start the stack (InfluxDB on `:8086`, Grafana on `:3000`):

   ```bash
   docker compose -f local-testing/docker-compose.yml up -d
   ```

   First start creates org `lemongrass`, a pinned operator token
   (`local-dev-token`), and the `laps`, `races`, and `race_sessions` buckets.
   These are non-secret, local-only values.

2. Point the CLI at the local stack by sourcing the committed app env:

   ```bash
   set -a && source local-testing/.env.local && set +a
   ```

   This sets `INFLUX_URL=http://localhost:8086`, `INFLUX_ORG=lemongrass`, and
   `INFLUX_TELEMETRY_TOKEN=local-dev-token`. (In prod these are unset and the CLI
   falls back to `https://influxdb.focism.com` / `focism`.)

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
   are pre-provisioned. The OBD dashboards (**telegraf**, **car252-pisugar-ups**) and OBD panels
   in **race-control** show no data locally because OBD/`stats_252` collection is not configured
   for local testing.

5. Tear down (add `-v` to wipe data and re-trigger bucket init next start):

   ```bash
   docker compose -f local-testing/docker-compose.yml down
   ```

   Storage is persistent named Docker volumes, so a plain `down` keeps your seeded
   data across restarts; pass `-v` only when you want a clean reset.

**Note:** telemetry/OBD commands (`telem`, `pisugar-monitor`) are out of scope for
local testing — they write to a `stats_252` bucket that this stack does not create.

**Unreachable InfluxDB:** if the server is down or unreachable, commands retry a few
times, then exit non-zero with a one-line `cannot reach InfluxDB at …` message
rather than a traceback.
