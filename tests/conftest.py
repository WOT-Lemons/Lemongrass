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
try:
    import race_monitor as _real_race_monitor

    _race_monitor_mock.get_streaming_command = _real_race_monitor.get_streaming_command
    del _real_race_monitor
finally:
    sys.modules['race_monitor'] = _race_monitor_mock
