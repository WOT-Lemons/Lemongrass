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

> **Note:** the `local-testing/` Grafana dashboards resolve races and sessions from
> Postgres via the `wotl-postgres` datasource, so they need that datasource provisioned
> (`docker compose up` does it) and the schema applied before their race pickers
> populate. The legacy Influx datasources stay provisioned for the pre-cutover buckets.
> Production dashboards are managed elsewhere and are a separate sub-project's
> responsibility.

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
   orphan race id(s) printed. The counts above are shaped like a full production
   deployment; the numbers themselves are made up. For counts actually observed, see
   [Observed rehearsal](#observed-rehearsal) below.

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
   skipped-existing`. These figures are illustrative too — see
   [Observed rehearsal](#observed-rehearsal).

## Observed rehearsal

Run twice from a clean stack (`docker compose down -v`, restore the three legacy
buckets by name, run steps 2-4 and 6) against four real races captured by the
pre-cutover writer. Both runs produced identical output:

    # step 3, --dry-run
    races:    read     4  written     0  would-write     4
    sessions: read    12  written     0  skipped     0  would-write    12
    now stored: 0 race(s), 0 session(s)

    # step 4, real import
    races:    read     4  written     4
    sessions: read    12  written    12  skipped     0
    now stored: 4 race(s), 12 session(s)

    # step 6, --only-missing
    races:    read     4  written     0  skipped-existing     4
    sessions: read    12  written     0  skipped     0  skipped-existing    12
    now stored: 4 race(s), 12 session(s)

**No orphan sessions appeared**, so the orphan line was never printed and
`sessions skipped` stayed `0` in every run. A deployment whose Influx history has
sessions whose race point was deleted or never written will see a non-zero count
there; this fixture has none, so that path is exercised only by the unit tests.

What the rehearsal confirmed beyond the counts:

- `session_count` on each imported race row equals its actual session count (3, 4,
  2, 3).
- `series_id` is NULL on every row and `series_name` is populated — the legacy
  points carry no series id, so it cannot be backfilled and is plumbed at capture
  time instead.
- `race_time` comes from the race point's *timestamp*; there is no `race_time`
  field to read.
- Three distinct spellings of one track and two spellings of one race name survive
  as written — `name` and `track_name` are raw passthrough.
- A session with an empty name imports and re-exports intact.
- `races list`, the races browser, and the dashboards all show actual lap counts
  from Influx joined to the Postgres attributes; three of the four races have fewer
  actual laps than `expected_lap_count` (3, 2, and 11 short).
- `races prune` deletes the Influx data first and the Postgres race row last, whose
  cascade removes that race's sessions; a second prune of the same id reports
  not-found.
- The rollback path round-trips: `export-legacy`, split with the `awk` above, and
  written back into scratch buckets reproduces all 4 races and all 12 sessions with
  identical identity, timestamps, and field values.

One defect surfaced and was fixed: pointing the races browser at an InfluxDB that
rejected the token crashed the TUI instead of showing `load failed: …`, because
Textual parses a status Label's string as console markup and the 401 body contains
a bracketed segment that reads as a malformed tag.

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
influx write --org <org> --token <token> --bucket races --precision ns @races.lp
if [ -s sessions.lp ]; then
  influx write --org <org> --token <token> --bucket race_sessions --precision ns @sessions.lp
fi
```

(If a deployment has zero sessions, `awk` never creates `sessions.lp` and this second
`influx write` would fail on a missing file — the `[ -s sessions.lp ]` guard skips it in
that case.)

Then revert the code and restart the services.

**Caveat:** `export-legacy` emits `series_id` as a field when the row has one, and `import-legacy`
reads it back, so a round trip preserves it. What a rollback *cannot* preserve is a `series_id` that
was never in Influx to begin with: rows created by `import-legacy` from legacy points have it NULL
permanently, because the legacy race point carried no series id to read. Reverting is otherwise
lossless for rows already migrated, but a revert performed *without* running `export-legacy` first
loses every race and session written directly to PostgreSQL since cutover — they have no Influx
counterpart at all.
