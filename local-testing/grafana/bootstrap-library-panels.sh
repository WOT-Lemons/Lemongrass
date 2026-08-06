#!/bin/sh
# Creates (or updates) the Grafana library panels the provisioned dashboards
# reference. Grafana provisions dashboards and datasources from files but has no
# file provisioning for library panels — they live only in the database and must
# be pushed over the HTTP API, which is what this container does on every
# `docker compose up`.
#
# The uids below are pinned to the same values the dashboards-as-code repo pins,
# because the dashboard JSON references panels by uid. Change one and the local
# tiles render as "library panel not found".
set -eu

GRAFANA_URL="${GRAFANA_URL:-http://grafana:3000}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:local-dev-password}"
PANEL_DIR="${PANEL_DIR:-/library_panels}"

# file:uid:name — name must match the dashboards' libraryPanel.name.
PANELS="
last_lap_time:aetsk34lvq22of:Last Lap Time
pisugar_battery_level:aek81rxxuthj4f:PiSugar Battery Level
pisugar_battery_voltage:cek81s6bt1m9sa:PiSugar Battery Voltage
pisugar_power_status:aek7q7arua7swe:PiSugar Power Status
"

echo "waiting for grafana at $GRAFANA_URL ..."
i=0
until curl -sf "$GRAFANA_URL/api/health" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "grafana did not become healthy within 60s" >&2
    exit 1
  fi
  sleep 1
done

echo "$PANELS" | while IFS=: read -r file uid name; do
  [ -n "$file" ] || continue
  model=$(cat "$PANEL_DIR/$file.json")

  if ! code=$(curl -s -o /tmp/get.json -w '%{http_code}' -u "$GRAFANA_AUTH" \
    "$GRAFANA_URL/api/library-elements/$uid"); then
    echo "request to fetch $name ($uid) failed before a response arrived" >&2
    exit 1
  fi

  if [ "$code" = "404" ]; then
    printf '{"uid":"%s","folderUid":"","name":"%s","kind":1,"model":%s}' "$uid" "$name" "$model" \
      | curl -sf -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' \
             -X POST --data-binary @- "$GRAFANA_URL/api/library-elements" >/dev/null
    echo "created  $name ($uid)"
  elif [ "$code" = "200" ]; then
    # PATCH requires the current version. The element's own "version" is the last
    # one in the response body, so take the last match.
    version=$(grep -o '"version":[0-9]*' /tmp/get.json | tail -1 | cut -d: -f2)
    case "$version" in
      ''|*[!0-9]*)
        echo "could not parse a numeric version for $name ($uid) from the GET response; refusing to send a malformed PATCH" >&2
        exit 1
        ;;
    esac
    printf '{"uid":"%s","folderUid":"","name":"%s","kind":1,"version":%s,"model":%s}' \
      "$uid" "$name" "$version" "$model" \
      | curl -sf -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' \
             -X PATCH --data-binary @- "$GRAFANA_URL/api/library-elements/$uid" >/dev/null
    echo "updated  $name ($uid) from version $version"
  else
    echo "unexpected status $code fetching $name ($uid):" >&2
    cat /tmp/get.json >&2
    exit 1
  fi
done

echo "library panels ready"
