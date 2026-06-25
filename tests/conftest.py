import sys
from unittest.mock import MagicMock

for _mod in [
    'obd',
    'pandas',
    'race_monitor',
]:
    sys.modules.setdefault(_mod, MagicMock())

# The real get_streaming_command is needed for _describe_bad_value tests.
# Temporarily remove the mock, import the real module, copy the function, restore.
_race_monitor_mock = sys.modules.pop('race_monitor')
# NOTE: if `import race_monitor` raises here, sys.modules['race_monitor'] is not restored;
# safe because race-monitor is a hard pyproject.toml dependency.
import race_monitor as _real_race_monitor  # noqa: E402
_race_monitor_mock.get_streaming_command = _real_race_monitor.get_streaming_command
sys.modules['race_monitor'] = _race_monitor_mock
del _real_race_monitor
