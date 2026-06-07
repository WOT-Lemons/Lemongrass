# pylint: disable=missing-module-docstring
import sys
from unittest.mock import MagicMock

for _mod in [
    'obd',
    'pandas',
    'race_monitor',
]:
    sys.modules.setdefault(_mod, MagicMock())
