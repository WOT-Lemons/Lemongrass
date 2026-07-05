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
