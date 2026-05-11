# pylint: disable=missing-module-docstring
import sys
from unittest.mock import MagicMock

for _mod in [
    'influxdb_client',
    'influxdb_client.client',
    'influxdb_client.client.write_api',
    'obd',
    'pandas',
    'race_monitor',
]:
    sys.modules.setdefault(_mod, MagicMock())
