# Moving races and sessions to PostgreSQL

This procedure copies race and session metadata from the legacy `races` and
`race_sessions` InfluxDB buckets to PostgreSQL. Laps, standings, telemetry, and
PiSugar data stay in InfluxDB. Do this procedure one time for each environment.
Do it when you deploy the first version that includes `lemongrass db import-legacy`.

The local stack does not need this procedure. `docker compose up` applies the
schema with the `db-schema` service. It also adds the `wotl-postgres` datasource to
Grafana.

## Prerequisites

- PostgreSQL 14 or later. The computer where you run `lemongrass db` must be able to
  connect to it.
- A `[postgres]` section in the config file, with the host, port, database, and
  user. See `lemongrass.toml.sample`.
- The database password in `LEMONGRASS_DB_PASSWORD`, or in the variable that
  `postgres.password_env` gives.

## Procedure

The commands in this procedure use Docker and an `.env` file. Do the steps in this
sequence.

1. Pull the new image:

   ```shell
   docker pull ghcr.io/wot-lemons/lemongrass:latest
   ```

   > **Caution:** Do not restart the services yet. Only the new image has the `db`
   > commands. If you restart the services first, they connect to a database that
   > has no schema.

2. Apply the schema:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db upgrade
   ```

3. Do a dry run. The dry run does not write data.

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy --dry-run
   ```

   Example output. The numbers are only examples.

   ```plain
   races:    read   184  written     0  would-write   184
   sessions: read   210  written     0  skipped     3  would-write   207
   orphan sessions belong to race id(s): 64202
   now stored: 0 race(s), 0 session(s)
   ```

   Make sure that `races read` is equal to `races would-write`.

   For each skipped session, the command logs a warning with one of these reasons:

   - **Orphan:** The race of the session is not in the `races` bucket. The
     `orphan sessions` line shows the race ID.
   - **Duplicate ID:** A different race already has the session ID.

   Use the warnings to find the reason for each skipped session. The import does not
   repair skipped sessions.

4. Run the import:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy
   ```

   Make sure that these values are correct:

   - `races read` is equal to `races written`.
   - `sessions read` is equal to `sessions written` plus `sessions skipped`.

5. Deploy the new version. Restart the services. After this step, the services
   read and write race and session metadata in PostgreSQL.

6. Import the data that the old version wrote to the legacy buckets between steps 4
   and 5:

   ```shell
   docker run --rm -it --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db import-legacy --only-missing
   ```

   > **Caution:** Do this step after step 5. Always use `--only-missing`. This option
   > adds only the rows that are not in PostgreSQL. Without this option, old
   > InfluxDB values replace the rows that the new version wrote.

   Example output. The numbers are only examples.

   ```plain
   races:    read   190  written     6  skipped-existing   184
   sessions: read   215  written     5  skipped     3  skipped-existing   207
   orphan sessions belong to race id(s): 64202
   now stored: 190 race(s), 212 session(s)
   ```

   Make sure that these values are correct:

   - `races read` is equal to `written` plus `skipped-existing`.
   - `sessions read` is equal to `written` plus `skipped` plus `skipped-existing`.

## Imported values

- `series_id` is NULL on imported rows, because legacy race points do not have a
  series ID.
- `venue_id`, `layout_id`, and `event_id` are NULL. To set them, run
  `lemongrass races identify` after the import.
- The import copies `name` and `track_name` without changes. Different spellings of
  the same race or track stay different.
- The import sets `race_time` from the timestamp of the race point.

## Rollback

To go back to the previous version, export the data from PostgreSQL to InfluxDB
line protocol. Then write the data to the legacy buckets.

> **Caution:** Export the data before you revert. If you revert without an export,
> you lose all races and sessions that lemongrass created after the move to
> PostgreSQL.

1. Export the data:

   ```shell
   docker run --rm --env-file .env ghcr.io/wot-lemons/lemongrass:latest \
     lemongrass db export-legacy > races-and-sessions.lp
   ```

   Do not use `-it`, because the output goes to a file. Use stdout, as shown. Do not
   use `--output`, because it writes the file inside the container. Docker deletes
   the file when the container stops.

2. Split the file. The races come first, then the sessions:

   ```shell
   awk '/^session,/{s=1} {print > (s ? "sessions.lp" : "races.lp")}' races-and-sessions.lp
   ```

3. Write each file to its bucket. If there are no sessions, `sessions.lp` does not
   exist, and the `if` statement skips it.

   ```shell
   influx write --org <org> --token <token> --bucket races --precision ns @races.lp
   if [ -s sessions.lp ]; then
     influx write --org <org> --token <token> --bucket race_sessions --precision ns @sessions.lp
   fi
   ```

4. Revert the code. Restart the services.

The export and import keep `series_id` if a row has one. They do not keep
`venue_id`, `layout_id`, or `event_id`. If you move to PostgreSQL again later, run
`lemongrass races identify` again.
