import sys
from unittest.mock import MagicMock

for _mod in [
    'obd',
    'race_monitor',
]:
    sys.modules.setdefault(_mod, MagicMock())

# The real get_streaming_command is needed for _describe_bad_value tests, and the
# real exception classes are needed so cli.py can `except RaceMonitorError` (a
# MagicMock attribute is not a valid exception type). Temporarily remove the mock,
# import the real module, copy the needed symbols onto the mock, restore.
_race_monitor_mock = sys.modules.pop('race_monitor')
try:
    import race_monitor as _real_race_monitor

    _race_monitor_mock.get_streaming_command = _real_race_monitor.get_streaming_command
    _race_monitor_mock.RaceMonitorError = _real_race_monitor.RaceMonitorError
    _race_monitor_mock.RaceMonitorHTTPError = _real_race_monitor.RaceMonitorHTTPError
    del _real_race_monitor
except ImportError:
    sys.modules['race_monitor'] = _race_monitor_mock
    raise
finally:
    sys.modules['race_monitor'] = _race_monitor_mock
