#!/bin/bash
# Runs once, inside the influxdb container, after DOCKER_INFLUXDB_INIT setup on
# first init (empty volume). Creates the race buckets the app writes by name.
# `laps` already exists via DOCKER_INFLUXDB_INIT_BUCKET.
set -euo pipefail

# During first-init the entrypoint runs influxd on INFLUXD_INIT_PORT (9999),
# not 8086 — 8086 only binds after init completes.
for bucket in races race_sessions; do
  influx bucket create \
    --name "$bucket" \
    --org "$DOCKER_INFLUXDB_INIT_ORG" \
    --host "http://localhost:${INFLUXD_INIT_PORT:-9999}" \
    --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
done

# stats_252 telemetry migrated from InfluxDB v1, so its bucket is NAMED
# `stats_252/autogen` (v1 db/rp). telem + pisugar_monitor write to that exact
# name via the v2 API, which matches on bucket name and ignores DBRP mappings —
# so a bare `stats_252` bucket would 404 those writes.
influx bucket create \
  --name 'stats_252/autogen' \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --host "http://localhost:${INFLUXD_INIT_PORT:-9999}" \
  --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
