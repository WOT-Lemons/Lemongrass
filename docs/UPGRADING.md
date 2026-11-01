# Upgrading

This document gives the steps that you must do when you upgrade to a new major
version. For all breaking changes, see [CHANGELOG.md](../CHANGELOG.md).

If you upgrade across more than one major version, do the steps for each version.
Start with the oldest version. Run `lemongrass races backfill --upgrade-stored` only
one time, after all other steps.

## To 7.x

lemongrass stores race and session metadata in PostgreSQL, not InfluxDB. Before you
deploy version 7:

1. Set up PostgreSQL 14 or later.
2. Add a `[postgres]` section to the config file.
3. Set `LEMONGRASS_DB_PASSWORD`.
4. Follow the runbook in
   [local-testing/migrations/README.md](../local-testing/migrations/README.md).

## To 6.x

The lap schema changed. Import the stored races again:

```shell
lemongrass races backfill --upgrade-stored
```

## To 5.x

Non-secret settings moved from environment variables to a TOML file. See
[Migrating from environment variables](CONFIGURATION.md#migrating-from-environment-variables).

## To 3.x

On the `lap` and `standings` measurements, `competitor_name` and `car_info` changed
from tags to fields.

1. Import the stored races again:

   ```shell
   lemongrass races backfill --upgrade-stored
   ```

2. Update the Grafana queries that group or pivot on these tags.

## To 2.x

One `lemongrass` command replaces the separate commands. Add `lemongrass` before
each command:

| Before | After |
| --- | --- |
| `laps RACE_ID CAR_NUMBER` | `lemongrass laps RACE_ID CAR_NUMBER` |
| `telem` | `lemongrass telem` |
| `race-backfill` | `lemongrass race-backfill` or `lemongrass races backfill` |
| `pisugar-monitor` | `lemongrass pisugar-monitor` |
| `race-diagnose` | `lemongrass race-diagnose` or `lemongrass races diagnose` |
