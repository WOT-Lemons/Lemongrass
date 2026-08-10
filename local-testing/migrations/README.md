# stats_252 → telem / pisugar migration (one-time)

Splits the legacy `stats_252/autogen` bucket into `telem` (OBD, tagged `vin`)
and `pisugar` (PiSugar, tagged `host`). Run once, manually, against the target
InfluxDB after the `telem` and `pisugar` buckets exist.

## Steps

1. Edit `2026-07-05-stats252-split.flux`, replacing `CURRENT_CAR_VIN` with the
   car's VIN and `CURRENT_PI_HOSTNAME` with the Pi's hostname.
2. Dry-run against the local stack first (see Verify).
3. Run against the target (paths are relative to the repo root):
   `influx query --org <org> --token <token> --file local-testing/migrations/2026-07-05-stats252-split.flux`
4. Verify row counts in `telem` / `pisugar` match the source, then (later,
   once confident) delete `stats_252/autogen`. It is left intact as rollback.

## Verify locally

    cd local-testing
    docker compose down -v && docker compose up -d --build influxdb
    # seed a couple of legacy points:
    docker compose exec influxdb influx write --org lemongrass --token local-dev-token \
      --bucket 'stats_252/autogen' \
      'rpm value=1234
    pisugar-battery-level value=80'
    # run the migration (placeholders substituted):
    docker compose exec influxdb sh -c \
      "influx query --org lemongrass --token local-dev-token '
        from(bucket: \"stats_252/autogen\") |> range(start: 0)
          |> filter(fn: (r) => not r._measurement =~ /^pisugar-/)
          |> set(key: \"vin\", value: \"TESTVIN\") |> to(bucket: \"telem\")'"
    # confirm the point landed vin-tagged in telem:
    docker compose exec influxdb influx query --org lemongrass --token local-dev-token \
      'from(bucket: "telem") |> range(start: 0) |> filter(fn: (r) => r._measurement == "rpm")'
    # run the pisugar half of the split:
    docker compose exec influxdb sh -c \
      "influx query --org lemongrass --token local-dev-token '
        from(bucket: \"stats_252/autogen\") |> range(start: 0)
          |> filter(fn: (r) => r._measurement =~ /^pisugar-/)
          |> set(key: \"host\", value: \"TESTHOST\") |> to(bucket: \"pisugar\")'"
    # confirm the point landed host-tagged in pisugar:
    docker compose exec influxdb influx query --org lemongrass --token local-dev-token \
      'from(bucket: "pisugar") |> range(start: 0) |> filter(fn: (r) => r._measurement == "pisugar-battery-level")'

Expected: the `rpm` point appears in `telem` carrying `vin=TESTVIN`, and the
`pisugar-battery-level` point appears in `pisugar` carrying `host=TESTHOST`.

# Races and sessions → PostgreSQL cutover (one-time)

Moves race and session metadata out of the legacy `races` / `race_sessions` InfluxDB
buckets into PostgreSQL. Laps, standings, telemetry, and PiSugar data are unaffected —
they stay in InfluxDB. Run once, per environment, when deploying the build that includes
the `lemongrass db import-legacy` / `export-legacy` commands.

> **Note:** the `local-testing/` Grafana dashboard variables are not repointed at
> Postgres by this migration — that is a separate, not-yet-implemented change. Until it
> lands, the local dashboards will keep resolving races and sessions from the legacy
> Influx buckets and will not show races written after cutover. Production dashboards
> are managed elsewhere and are a separate sub-project's responsibility.

## Prerequisite

- A PostgreSQL instance (v14+) is provisioned and reachable from wherever you run
  `lemongrass db`.
- `LEMONGRASS_DB_PASSWORD` (or whatever env var `postgres.password_env` names) is set.
- `[postgres]` is configured in the TOML config (host, port, database, user — see
  `lemongrass.toml.sample`).

## Steps

1. **Pull the new image. Do not restart the running services yet.** The
   `import-legacy` / `export-legacy` / `upgrade` / `current` subcommands only exist in
   this new build. Restarting the live services first would point them at a Postgres
   database with no schema applied, before you've had a chance to check it.

   ```shell
   docker pull ghcr.io/wot-lemons/lemongrass:latest
   ```

2. Apply the schema, from the new image, against the provisioned Postgres:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db upgrade
   ```

3. Preview the copy without writing anything:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy --dry-run
   ```

   The command prints read/would-write/skip counts and any orphan sessions (a session
   whose race has no matching race point — these are reported, never repaired by
   inventing a stub race). Shape of the output:

   ```
   === --dry-run ===
   races:    read   184  written     0  would-write   184
   sessions: read   210  written     0  skipped     3  would-write   207
   orphan sessions belong to race id(s): 64202
   now stored: 0 race(s), 0 session(s)
   ```

   (The `=== --dry-run ===` / `=== --only-missing ===` headers above are this
   document's labels for the two runs, not something the command prints itself —
   they're here to keep the two examples straight.)

   A healthy dry run has `races read == races would-write` (nothing written yet, so
   `written` is `0`) and a small, explained `sessions skipped` count matching the
   orphan race id(s) printed. **These example figures are illustrative only** — they
   have not been produced against real captured data; a rehearsal run (see Task 14)
   should record the actual observed counts here once it happens.

4. Run the import for real:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy
   ```

   A healthy run satisfies `races read == races written` and
   `sessions read == sessions written + sessions skipped` (the skipped ones are the
   same orphans reported in step 3).

5. **Deploy the new code and restart the services.** From this point on they read and
   write race and session metadata in Postgres instead of InfluxDB.

6. Catch up anything written to the legacy Influx buckets between steps 4 and 5 —
   **this step must come after step 5, and must pass `--only-missing`**:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy --only-missing
   ```

   `--only-missing` inserts rows absent from Postgres and never touches a row that's
   already there. Without it, this step would replay stale Influx values over rows the
   newly-deployed writer has already written or corrected — and the importer, running
   after, would win, silently overwriting the fresher Postgres data. Expect the shape:

   ```
   === --only-missing ===
   races:    read   190  written     6  skipped-existing   184
   sessions: read   215  written     5  skipped     3  skipped-existing   207
   orphan sessions belong to race id(s): 64202
   now stored: 190 race(s), 212 session(s)
   ```

   A healthy `--only-missing` run satisfies `races read == races written +
   skipped-existing` and `sessions read == sessions written + skipped +
   skipped-existing`. **These figures are also illustrative, not observed** — a
   rehearsal has not yet been run against captured data.

## Rollback

If the new code needs to be reverted after cutover, export what's in Postgres back out
as Influx line protocol and re-seed the legacy buckets so the reverted code (which only
knows how to read races and sessions from Influx) has data to work with:

```shell
docker run --rm --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
  lemongrass db export-legacy > races-and-sessions.lp
```

(No `-it` here — this run is non-interactive and its stdout is being redirected to a
file; allocating a pseudo-TTY on a piped command can corrupt the output.)

(`--output` writes inside the container's own filesystem and is lost when the
container exits unless you bind-mount a writable `/data` — same footgun as
`lemongrass laps -o`, see the main README. Redirecting the default stdout output to a
host file, as above, sidesteps that entirely.)

Races are written first, then sessions, so the file can be split at the first
`session,` line:

```shell
awk '/^session,/{s=1} {print > (s ? "sessions.lp" : "races.lp")}' races-and-sessions.lp
```

and each half fed to `influx write` against its own bucket:

```shell
influx write --bucket races --precision ns @races.lp
influx write --bucket race_sessions --precision ns @sessions.lp
```

Then revert the code and restart the services.

**Caveat:** the legacy Influx schema has no `series_id` tag, so `export-legacy` does not
emit one — a rollback silently drops `series_id` from every exported row, even though
it's part of the Postgres schema. Reverting is otherwise lossless for rows already
migrated, but revert *without* running `export-legacy` first loses every race and
session written directly to Postgres since cutover (they have no Influx counterpart).
