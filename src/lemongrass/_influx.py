"""Shared InfluxDB connection settings for lemongrass commands.

Centralizes the URL/org (env-overridable, defaulting to prod), the retry policy,
and the token-reading client factory so every command constructs its
InfluxDBClient the same way.
"""
import logging
import os
import re
import sys

from urllib3 import Retry

INFLUX_URL = os.environ.get('INFLUX_URL', 'https://influxdb.focism.com')
INFLUX_ORG = os.environ.get('INFLUX_ORG', 'focism')

# Bucket names, shared across the write paths and Flux queries (and created by
# local-testing/influx-init). Single source of truth so a rename touches one place.
BUCKET_LAPS = 'laps'
BUCKET_RACES = 'races'
BUCKET_SESSIONS = 'race_sessions'

# Bounded retry for transient failures (connection blips, retryable 5xx). We do
# NOT honor Retry-After: a downed Cloudflare tunnel returns 530 with
# Retry-After: 120, which would otherwise hang the CLI for minutes. allowed_methods
# is None so POST writes are retried too — the points written are idempotent.
def build_retries(total):
    """Build the shared transient-error retry policy with a given attempt budget.

    Batch commands use the 3-retry default; the telemetry hot loop trims this
    (it has a durable spool as its real retry path) but keeps the same
    forcelist/back-off semantics, so the policy lives in one place.
    """
    return Retry(
        total=total,
        backoff_factor=1,
        backoff_max=10,
        status_forcelist=[429, 502, 503, 504, 530],
        respect_retry_after_header=False,
        allowed_methods=None,
    )


INFLUX_RETRIES = build_retries(3)


def connect(timeout=None, retries=INFLUX_RETRIES):
    """Return an InfluxDBClient wired with the shared URL/org/retries.

    Reads the token from INFLUX_TELEMETRY_TOKEN; logs an error and exits with
    status 1 if it is unset. The InfluxDBClient import is deferred so importing
    this module (which every command does) stays cheap.

    `timeout` (ms) and `retries` let the telemetry hot loop fail fast to its
    spool; when timeout is None the influxdb-client library default (10s) applies
    so batch callers are unaffected.
    """
    token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
    if not token:
        logging.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
        sys.exit(1)
    from influxdb_client import InfluxDBClient
    kwargs = {'url': INFLUX_URL, 'token': token, 'org': INFLUX_ORG,
              'retries': retries}
    if timeout is not None:
        kwargs['timeout'] = timeout
    return InfluxDBClient(**kwargs)


# Lap timestamps are session-anchored and Flux `stop` is exclusive, so laps can
# legitimately fall outside the nominal race bounds; pad the window instead of
# trusting Start/EndDateEpoc exactly. The race_id tag filter stays the exact selector.
# Shared by race_backfill and race_diagnose so the two padding windows can't drift.
WINDOW_PAD_S = 86400


_FLUX_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')


def invalid_flux_ids(values):
    """Return the values unsafe to interpolate into a Flux string literal.

    Race/car identifiers are interpolated into Flux queries and delete
    predicates; restricting them to [A-Za-z0-9_-] rules out quotes and other
    metacharacters that would break (or alter) the query.
    """
    return [v for v in values if not _FLUX_ID_RE.fullmatch(str(v))]
