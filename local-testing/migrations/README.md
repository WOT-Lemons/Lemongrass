# Moving races and sessions to PostgreSQL

This one-time procedure copies race and session metadata from the legacy `races` and
`race_sessions` InfluxDB buckets to PostgreSQL. Laps, standings, telemetry, and
PiSugar data stay in InfluxDB. Run it once per environment, when you deploy the
first version that includes `lemongrass db import-legacy`.

The local stack does not need this procedure. `docker compose up` applies the schema
(the `db-schema` service) and provisions the `wotl-postgres` Grafana datasource.

## Prerequisites

- PostgreSQL 14 or later, reachable from where you run `lemongrass db`.
- `[postgres]` set in the config file (host, port, database, user). See
  `lemongrass.toml.sample`.
- `LEMONGRASS_DB_PASSWORD` set, or the variable named by `postgres.password_env`.

## Procedure

The commands below use Docker with an `.env` file. Run the steps in order.

1. **Pull the new image. Do not restart the services yet.** The `db` commands exist
   only in the new image. If you restart first, the services connect to a database
   that has no schema.

   ```shell
   docker pull ghcr.io/wot-lemons/lemongrass:latest
   ```

2. Apply the schema:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db upgrade
   ```

3. Do a dry run. Nothing is written.

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy --dry-run
   ```

   Example output (illustrative numbers):

   ```plain
   races:    read   184  written     0  would-write   184
   sessions: read   210  written     0  skipped     3  would-write   207
   orphan sessions belong to race id(s): 64202
   now stored: 0 race(s), 0 session(s)
   ```

   Check that `races read` equals `races would-write`. Each skipped session logs a
   warning with the reason. The two reasons are:

   - **Orphan:** the session's race is not in the `races` bucket. Its race ID is
     listed on the `orphan sessions` line.
   - **Duplicate ID:** another race already owns the session ID.

   Use the warnings to account for every skipped session. Skipped sessions are not
   repaired.

4. Run the import:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy
   ```

   Check that `races read` equals `races written`, and that `sessions read` equals
   `sessions written` plus `sessions skipped`.

5. **Deploy the new version and restart the services.** From now on, they read and
   write race and session metadata in PostgreSQL.

6. Import anything that was written to the legacy buckets between steps 4 and 5. Run
   this after step 5, and always with `--only-missing`:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy --only-missing
   ```

   `--only-missing` inserts only rows that are not already in PostgreSQL. Without
   it, old InfluxDB values overwrite rows that the new version already wrote.

   Example output (illustrative numbers):

   ```plain
   races:    read   190  written     6  skipped-existing   184
   sessions: read   215  written     5  skipped     3  skipped-existing   207
   orphan sessions belong to race id(s): 64202
   now stored: 190 race(s), 212 session(s)
   ```

   Check that `races read` equals `written` plus `skipped-existing`, and that
   `sessions read` equals `written` plus `skipped` plus `skipped-existing`.

## What the import does not set

- `series_id` is NULL on imported rows. Legacy race points do not have a series ID.
- `venue_id`, `layout_id`, and `event_id` are NULL. Run `lemongrass races identify`
  after the import to set them.
- `name` and `track_name` are copied as written, including different spellings of
  the same race or track.
- `race_time` comes from the race point's timestamp.

## Rollback

To go back to the previous version, export PostgreSQL to InfluxDB line protocol and
write it back to the legacy buckets. Do this before you revert. If you revert
without exporting, all races and sessions created since the cutover are lost.

1. Export. Do not use `-it`, because the output goes to a file:

   ```shell
   docker run --rm --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db export-legacy > races-and-sessions.lp
   ```

   Use stdout, as shown. `--output` writes inside the container, and the file is
   lost when the container exits.

2. Split the file. Races come first, then sessions:

   ```shell
   awk '/^session,/{s=1} {print > (s ? "sessions.lp" : "races.lp")}' races-and-sessions.lp
   ```

3. Write each file to its bucket. `sessions.lp` does not exist if there are no
   sessions:

   ```shell
   influx write --org <org> --token <token> --bucket races --precision ns @races.lp
   if [ -s sessions.lp ]; then
     influx write --org <org> --token <token> --bucket race_sessions --precision ns @sessions.lp
   fi
   ```

4. Revert the code and restart the services.

A round trip keeps `series_id` where a row has one. It does not keep `venue_id`,
`layout_id`, or `event_id`. Run `lemongrass races identify` again after you move
forward to PostgreSQL a second time.
