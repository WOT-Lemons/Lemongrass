"""Durable on-disk spool for telemetry points during an InfluxDB outage.

telem.py's hot tier is an in-memory queue flushed to InfluxDB every 0.5s. When a
flush fails (Influx unreachable) the unwritten batch is serialized to InfluxDB
line protocol and appended here, on disk, so it survives the watchdog restart a
coincident OBD dropout triggers. On recovery the oldest file is replayed through
the same write_api and deleted. Replay is idempotent: every point carries an
explicit nanosecond timestamp and Influx upserts by measurement+tags+time.
"""
import logging
import os
from pathlib import Path

logger = logging.getLogger('telem')

DEFAULT_SPOOL_DIR = '/data/telem-spool'
DEFAULT_MAX_BYTES = 1024 ** 3        # 1 GiB
ROTATE_BYTES = 8 * 1024 * 1024       # 8 MiB per file
_SUFFIX = '.lp'


class Spool:
    """A directory of rotating line-protocol files buffering points on disk."""

    def __init__(
        self, directory, max_bytes=DEFAULT_MAX_BYTES, rotate_bytes=ROTATE_BYTES
    ):
        self.dir = Path(directory)
        self.max_bytes = max_bytes
        self.rotate_bytes = rotate_bytes
        self.enabled = self._ensure_dir()

    @classmethod
    def from_env(cls):
        return cls(
            os.environ.get('TELEM_SPOOL_DIR', DEFAULT_SPOOL_DIR),
            max_bytes=int(
                os.environ.get('TELEM_SPOOL_MAX_BYTES', DEFAULT_MAX_BYTES)
            ),
        )

    def _ensure_dir(self):
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            return True
        except OSError as e:
            logger.error(
                "Spool dir %s unusable (%s); telemetry durability disabled",
                self.dir,
                e,
            )
            return False

    def _files(self):
        if not self.enabled:
            return []
        return sorted(self.dir.glob(f'*{_SUFFIX}'))

    def _append_path(self):
        files = self._files()
        if files and files[-1].stat().st_size < self.rotate_bytes:
            return files[-1]
        next_seq = (int(files[-1].stem) + 1) if files else 1
        return self.dir / f'{next_seq:012d}{_SUFFIX}'

    def append(self, points):
        """Serialize points to line protocol and fsync-append to the newest file."""
        if not self.enabled or not points:
            return
        blob = ''.join(p.to_line_protocol() + '\n' for p in points).encode()
        path = self._append_path()
        try:
            with open(path, 'ab') as f:
                f.write(blob)
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logger.error("Spool append to %s failed: %s", path, e)
            return
        self._enforce_cap()

    def _enforce_cap(self):
        files = self._files()
        total = sum(f.stat().st_size for f in files)
        dropped = 0
        while total > self.max_bytes and len(files) > 1:
            victim = files.pop(0)
            total -= victim.stat().st_size
            try:
                victim.unlink()
                dropped += 1
            except OSError:
                pass
        if dropped:
            logger.warning(
                "Spool exceeded %d bytes; dropped %d oldest file(s)",
                self.max_bytes,
                dropped,
            )
