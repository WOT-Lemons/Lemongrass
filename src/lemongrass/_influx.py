"""Shared InfluxDB connection settings for lemongrass commands.

Centralizes the URL/org (env-overridable, defaulting to prod), the retry policy,
and the token-reading client factory so every command constructs its
InfluxDBClient the same way.
"""
import logging
import os
import sys

from urllib3 import Retry

INFLUX_URL = os.environ.get('INFLUX_URL', 'https://influxdb.focism.com')
INFLUX_ORG = os.environ.get('INFLUX_ORG', 'focism')

# Bounded retry for transient failures (connection blips, retryable 5xx). We do
# NOT honor Retry-After: a downed Cloudflare tunnel returns 530 with
# Retry-After: 120, which would otherwise hang the CLI for minutes. allowed_methods
# is None so POST writes are retried too — the points written are idempotent.
INFLUX_RETRIES = Retry(
    total=3,
    backoff_factor=1,
    backoff_max=10,
    status_forcelist=[429, 502, 503, 504, 530],
    respect_retry_after_header=False,
    allowed_methods=None,
)


def connect():
    """Return an InfluxDBClient wired with the shared URL/org/retries.

    Reads the token from INFLUX_TELEMETRY_TOKEN; logs an error and exits with
    status 1 if it is unset. The InfluxDBClient import is deferred so importing
    this module (which every command does) stays cheap.
    """
    token = os.environ.get('INFLUX_TELEMETRY_TOKEN')
    if not token:
        logging.error("INFLUX_TELEMETRY_TOKEN environment variable not set")
        sys.exit(1)
    from influxdb_client import InfluxDBClient
    return InfluxDBClient(
        url=INFLUX_URL, token=token, org=INFLUX_ORG, retries=INFLUX_RETRIES
    )
