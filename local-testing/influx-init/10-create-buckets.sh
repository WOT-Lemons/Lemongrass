#!/bin/bash
# Runs once, inside the influxdb container, after DOCKER_INFLUXDB_INIT setup on
# first init (empty volume). Creates the race buckets the app writes by name.
# `laps` already exists via DOCKER_INFLUXDB_INIT_BUCKET.
set -euo pipefail

# During first-init the entrypoint runs influxd on INFLUXD_INIT_PORT (9999),
# not 8086 — 8086 only binds after init completes.
for bucket in races race_sessions telem pisugar; do
  influx bucket create \
    --name "$bucket" \
    --org "$DOCKER_INFLUXDB_INIT_ORG" \
    --host "http://localhost:${INFLUXD_INIT_PORT:-9999}" \
    --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
done

# Telegraf writes host metrics to a bucket NAMED `system_metrics/autogen` (v1
# db/rp, kept from its InfluxDB v1 origin). The v2 write API matches on bucket
# name and ignores DBRP mappings, so the bucket needs that literal name.
influx bucket create \
  --name 'system_metrics/autogen' \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --host "http://localhost:${INFLUXD_INIT_PORT:-9999}" \
  --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
