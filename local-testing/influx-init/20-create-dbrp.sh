#!/bin/bash
# Runs once, inside the influxdb container, after 10-create-buckets.sh on first
# init (empty volume). Creates v1 DBRP mappings so the InfluxQL Grafana
# datasources (wotl-laps-ql / wotl-races-ql) can query the 2.x buckets via the
# v1 API. The race_sessions bucket is queried only via Flux, so it needs no DBRP.
set -euo pipefail

# During first-init the entrypoint runs influxd on INFLUXD_INIT_PORT (9999),
# not 8086 — 8086 only binds after init completes.
HOST="http://localhost:${INFLUXD_INIT_PORT:-9999}"

for db in laps races stats_252; do
  bucket_id=$(influx bucket list \
    --name "$db" \
    --org "$DOCKER_INFLUXDB_INIT_ORG" \
    --host "$HOST" \
    --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN" \
    --hide-headers | awk '{print $1}')

  influx v1 dbrp create \
    --db "$db" \
    --rp autogen \
    --bucket-id "$bucket_id" \
    --default \
    --org "$DOCKER_INFLUXDB_INIT_ORG" \
    --host "$HOST" \
    --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
done
