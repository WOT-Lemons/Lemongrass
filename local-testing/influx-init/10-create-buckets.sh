#!/bin/bash
# Runs once, inside the influxdb container, after DOCKER_INFLUXDB_INIT setup on
# first init (empty volume). Creates the race buckets the app writes by name.
# `laps` already exists via DOCKER_INFLUXDB_INIT_BUCKET.
set -euo pipefail

# During first-init the entrypoint runs influxd on INFLUXD_INIT_PORT (9999),
# not 8086 — 8086 only binds after init completes.
for bucket in races race_sessions stats_252; do
  influx bucket create \
    --name "$bucket" \
    --org "$DOCKER_INFLUXDB_INIT_ORG" \
    --host "http://localhost:${INFLUXD_INIT_PORT:-9999}" \
    --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
done
