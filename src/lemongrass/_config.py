"""Layered configuration for lemongrass: dataclass defaults, an optional TOML
file (via LEMONGRASS_CONFIG), and environment-variable overrides.

This module is a leaf — it imports nothing else from lemongrass — so any command
module can source its settings here without an import cycle.
"""
import re

_SIZE_RE = re.compile(r'^\s*([0-9]*\.?[0-9]+)\s*([KMGTP]I?B|B)?\s*$', re.IGNORECASE)
_SIZE_UNITS = {
    '': 1, 'B': 1,
    'KB': 1000, 'MB': 1000 ** 2, 'GB': 1000 ** 3, 'TB': 1000 ** 4, 'PB': 1000 ** 5,
    'KIB': 1024, 'MIB': 1024 ** 2, 'GIB': 1024 ** 3, 'TIB': 1024 ** 4, 'PIB': 1024 ** 5,
}


def parse_size(value):
    """Parse a byte size from an int/float (bytes) or a unit string like "1GiB".

    Binary units (KiB..PiB) are powers of 1024; decimal units (KB..PB) powers of
    1000; a bare number or a `B` suffix is bytes. Case-insensitive, whitespace and
    fractions allowed. Raises ValueError on an unparseable or non-positive value.
    """
    if isinstance(value, bool):
        raise ValueError(f"invalid size: {value!r}")
    if isinstance(value, (int, float)):
        n = int(value)
    elif isinstance(value, str):
        m = _SIZE_RE.match(value)
        if not m:
            raise ValueError(f"invalid size: {value!r}")
        n = int(float(m.group(1)) * _SIZE_UNITS[(m.group(2) or '').upper()])
    else:
        raise ValueError(f"invalid size: {value!r}")
    if n <= 0:
        raise ValueError(f"size must be positive: {value!r}")
    return n
