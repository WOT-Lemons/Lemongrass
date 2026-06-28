#!/bin/bash
# Runs once, inside the influxdb container, after DOCKER_INFLUXDB_INIT setup on
# first init (empty volume). Creates the race buckets the app writes by name.
# `laps` already exists via DOCKER_INFLUXDB_INIT_BUCKET.
set -euo pipefail

for bucket in races race_sessions; do
  influx bucket create \
    --name "$bucket" \
    --org "$DOCKER_INFLUXDB_INIT_ORG" \
    --host "http://localhost:${INFLUXD_INIT_PORT:-9999}" \
    --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
done
