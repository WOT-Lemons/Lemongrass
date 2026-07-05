// One-time split of the legacy stats_252/autogen bucket into the car-agnostic
// telem bucket (OBD, vin-tagged) and the pisugar bucket (host-tagged).
//
// Replace the two placeholders before running:
//   CURRENT_CAR_VIN   -- the VIN of the single car whose history this is
//   CURRENT_PI_HOSTNAME -- the hostname that car's Pi reports (telegraf host)
//
// Idempotent: ns timestamps + Influx upsert mean a re-run overwrites, not
// duplicates. Leaves stats_252/autogen intact for rollback.

// OBD measurements -> telem
from(bucket: "stats_252/autogen")
  |> range(start: 0)
  |> filter(fn: (r) => not r._measurement =~ /^pisugar-/)
  |> set(key: "vin", value: "CURRENT_CAR_VIN")
  |> to(bucket: "telem")

// PiSugar measurements -> pisugar
from(bucket: "stats_252/autogen")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement =~ /^pisugar-/)
  |> set(key: "host", value: "CURRENT_PI_HOSTNAME")
  |> to(bucket: "pisugar")
