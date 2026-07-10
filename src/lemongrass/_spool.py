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

from influxdb_client.rest import ApiException

logger = logging.getLogger('telem')

DEFAULT_SPOOL_DIR = '/data/telem-spool'
DEFAULT_MAX_BYTES = 1024 ** 3        # 1 GiB
ROTATE_BYTES = 8 * 1024 * 1024       # 8 MiB per file
_SUFFIX = '.lp'                      # live, replayable spool files
_BAD_SUFFIX = '.bad'                 # quarantined files (unwritable / unreadable)


class Spool:
    """A directory of rotating line-protocol files buffering points on disk."""

    def __init__(
        self, directory, max_bytes=DEFAULT_MAX_BYTES, rotate_bytes=ROTATE_BYTES
    ):
        """Open a spool at ``directory``, capped at ``max_bytes`` total and
        rotating to a new file every ``rotate_bytes``. Durability is disabled
        (``self.enabled`` False) if the directory cannot be created."""
        self.dir = Path(directory)
        self.max_bytes = max_bytes
        self.rotate_bytes = rotate_bytes
        self.enabled = self._ensure_dir()

    @classmethod
    def from_config(cls):
        """Build a Spool from the telem.spool section of the loaded config."""
        from lemongrass import _config
        spool = _config.load_config().telem.spool
        return cls(spool.dir, max_bytes=spool.max_size)

    def _ensure_dir(self):
        """Create the spool directory; return True if usable, False (and log) if not."""
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
        """Return the live (.lp) spool files, oldest first (empty if disabled)."""
        if not self.enabled:
            return []
        return sorted(self.dir.glob(f'*{_SUFFIX}'))

    def _next_seq(self):
        """Return the next file sequence number, one past the highest existing.

        Considers BOTH live (.lp) and quarantined (.bad) files so the counter
        never resets and collides with a lingering quarantined file.
        """
        # Derive the next sequence from BOTH live (.lp) and quarantined (.bad)
        # files: once every .lp drains, a lingering .bad must not let the
        # counter reset to 1 and collide with (rename would clobber) or
        # mis-order against the quarantined file.
        seqs = [
            int(p.stem)
            for p in (*self.dir.glob(f'*{_SUFFIX}'), *self.dir.glob(f'*{_BAD_SUFFIX}'))
        ]
        return (max(seqs) + 1) if seqs else 1

    def _append_path(self):
        """Path to append to: the newest file if under the rotate threshold,
        otherwise a freshly sequenced file."""
        files = self._files()
        if files and files[-1].stat().st_size < self.rotate_bytes:
            return files[-1]
        return self.dir / f'{self._next_seq():012d}{_SUFFIX}'

    def _fsync_dir(self):
        """fsync the spool directory so a newly-created file's directory entry
        is durable across a hard fault (the per-file data fsync alone does not
        persist the parent-dir metadata that makes the new file discoverable)."""
        try:
            dir_fd = os.open(self.dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as e:
            logger.warning("Could not fsync spool dir %s: %s", self.dir, e)

    def append(self, points):
        """Serialize points to line protocol and fsync-append to the newest file.

        Returns True when the points were durably written to disk (or there
        was nothing to do), False when they could not be durably stored --
        callers must fall back to an in-memory backlog in that case.
        """
        if not self.enabled:
            return False
        if not points:
            return True
        blob = ''.join(p.to_line_protocol() + '\n' for p in points).encode()
        path = self._append_path()
        is_new = not path.exists()
        try:
            with open(path, 'ab') as f:
                f.write(blob)
                f.flush()
                os.fsync(f.fileno())
            if is_new:
                self._fsync_dir()
        except OSError as e:
            logger.error("Spool append to %s failed: %s", path, e)
            return False
        self._enforce_cap()
        return True

    def _enforce_cap(self):
        """Evict the oldest files until total size is within ``max_bytes``.

        Counts both live (.lp) and quarantined (.bad) files toward the cap and
        always keeps at least one file. Eviction counts are logged.
        """
        # Count both live (.lp) and quarantined (.bad) files toward the cap: a
        # recurring stream of poison/unreadable files must not accumulate .bad
        # files past telem.spool.max_size on a constrained device. Both suffixes
        # are zero-padded-sequence names, so a plain name sort is oldest-first.
        files = sorted(
            [*self.dir.glob(f'*{_SUFFIX}'), *self.dir.glob(f'*{_BAD_SUFFIX}')],
            key=lambda p: p.name,
        )
        total = sum(f.stat().st_size for f in files)
        dropped = 0
        while total > self.max_bytes and len(files) > 1:
            victim = files.pop(0)
            total -= victim.stat().st_size
            try:
                victim.unlink()
                dropped += 1
            except OSError as e:
                logger.warning("Could not evict spool file %s: %s", victim.name, e)
        if dropped:
            logger.warning(
                "Spool exceeded %d bytes; dropped %d oldest file(s)",
                self.max_bytes,
                dropped,
            )

    def replay_oldest(self, write_api, bucket):
        """Replay the oldest spool file through write_api; delete it on success.

        Returns True if the spool is empty or one file was drained (or a corrupt
        file was quarantined — progress either way). Returns False if the write
        failed for a retryable/connectivity reason (Influx still down): the file
        is kept for the next attempt.
        """
        if not self.enabled:
            return True
        files = self._files()
        if not files:
            return True
        path = files[0]
        try:
            text = path.read_text()
        except OSError as e:
            logger.error("Cannot read spool file %s: %s; quarantining", path, e)
            quarantine = path.with_suffix(_BAD_SUFFIX)
            try:
                path.rename(quarantine)
            except OSError as e2:
                logger.warning(
                    "Could not quarantine unreadable spool file %s: %s", path.name, e2)
            return True
        try:
            write_api.write(bucket=bucket, record=text)
        except ApiException as e:
            if e.status and 400 <= e.status < 500 and e.status != 429:
                return self._handle_corrupt(write_api, bucket, path, text)
            return False  # 5xx / 429 / unknown status: retryable, keep the file
        except Exception:
            return False  # connectivity failure, keep the file
        try:
            path.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("Could not remove replayed spool file %s: %s", path.name, e)
        logger.info("Replayed spool file %s", path.name)
        return True

    def _handle_corrupt(self, write_api, bucket, path, text):
        """Salvage a 4xx-rejected file by dropping its (possibly torn) last line;
        quarantine to <name>.bad if it still will not write.

        A *retryable* failure of the salvage write (5xx / 429 / unknown status /
        connectivity) keeps the file and returns False, so its good lines are not
        thrown away over a transient hiccup between the two writes; only a genuine
        4xx rejection (or an unsalvageable single-line file) is quarantined.
        """
        lines = text.splitlines()
        if len(lines) > 1:
            salvaged = '\n'.join(lines[:-1]) + '\n'
            try:
                write_api.write(bucket=bucket, record=salvaged)
            except ApiException as e:
                if not (e.status and 400 <= e.status < 500 and e.status != 429):
                    return False  # retryable — keep the file, try again later
                # genuine 4xx on the salvaged data too — fall through to quarantine
            except Exception:
                return False  # connectivity failure — keep the file
            else:
                try:
                    path.unlink(missing_ok=True)
                except OSError as e:
                    logger.warning(
                        "Could not remove salvaged spool file %s: %s", path.name, e)
                logger.warning(
                    "Dropped 1 unwritable line from spool file %s", path.name
                )
                return True
        quarantine = path.with_suffix(_BAD_SUFFIX)
        try:
            path.rename(quarantine)
        except OSError as e:
            logger.warning(
                "Could not quarantine spool file %s -> %s: %s",
                path.name,
                quarantine.name,
                e,
            )
        logger.error(
            "Quarantined unwritable spool file %s -> %s",
            path.name,
            quarantine.name,
        )
        return True
