# Upgrading

These major versions require action when you upgrade. For all breaking changes, see
[CHANGELOG.md](../CHANGELOG.md).

If you upgrade across several major versions, do the steps for each one, oldest
first. Run `lemongrass races backfill --upgrade-stored` only once, after all other
steps.

## To 7.x

Race and session metadata moved from InfluxDB to PostgreSQL. Before you deploy:

1. Set up PostgreSQL 14 or later.
2. Add `[postgres]` to the config file and set `LEMONGRASS_DB_PASSWORD`.
3. Follow the runbook in
   [local-testing/migrations/README.md](../local-testing/migrations/README.md).

## To 6.x

The lap schema changed. Reprocess stored races:

```shell
lemongrass races backfill --upgrade-stored
```

## To 5.x

Non-secret settings moved from environment variables to a TOML file. See
[Migrating from environment variables](CONFIGURATION.md#migrating-from-environment-variables).

## To 3.x

`competitor_name` and `car_info` changed from tags to fields on the `lap` and
`standings` measurements. Reprocess stored races:

```shell
lemongrass races backfill --upgrade-stored
```

Update Grafana queries that group or pivot on these tags.

## To 2.x

The separate commands were replaced by one `lemongrass` command. Add the
`lemongrass` prefix:

| Before | After |
| --- | --- |
| `laps RACE_ID CAR_NUMBER` | `lemongrass laps RACE_ID CAR_NUMBER` |
| `telem` | `lemongrass telem` |
| `race-backfill` | `lemongrass race-backfill` or `lemongrass races backfill` |
| `pisugar-monitor` | `lemongrass pisugar-monitor` |
| `race-diagnose` | `lemongrass race-diagnose` or `lemongrass races diagnose` |
