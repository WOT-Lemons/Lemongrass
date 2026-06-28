"""Shared InfluxDB connection settings for lemongrass commands.

Centralizes the URL/org (env-overridable, defaulting to prod) and the retry
policy so every command constructs its InfluxDBClient the same way.
"""
import os

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
