"""Durable on-disk spool for telemetry points during an InfluxDB outage.

telem.py's hot tier is an in-memory queue flushed to InfluxDB every 0.5s. When a
flush fails (Influx unreachable) the unwritten batch is serialized to InfluxDB
line protocol and appended here, on disk, so it survives the watchdog restart a
coincident OBD dropout triggers.

A separate drain thread replays the oldest file in bounded chunks and deletes it,
independently of the OBD link — recovery must not wait for the car to be running.
Replay is idempotent: every point carries an explicit nanosecond timestamp and
Influx upserts by measurement+tags+time. The appending and draining threads are
serialized by Spool's lock and its rename-to-claim protocol.
"""
import logging
import os
import threading
from pathlib import Path

from influxdb_client.rest import ApiException

logger = logging.getLogger('telem')

DEFAULT_SPOOL_DIR = '/data/telem-spool'
DEFAULT_MAX_BYTES = 1024 ** 3        # 1 GiB
ROTATE_BYTES = 8 * 1024 * 1024       # 8 MiB per file
_SUFFIX = '.lp'                      # live, replayable spool files
_BAD_SUFFIX = '.bad'                 # quarantined files (unwritable / unreadable)
_CLAIM_SUFFIX = '.replaying'  # a file the drain thread has taken ownership of

# Lines per replay request. A whole file (up to ROTATE_BYTES) in one POST cannot
# finish inside any sane timeout on the car's uplink, which is why a 4.4h outage
# never drained. Chunking makes request size independent of the rotation size.
REPLAY_CHUNK_LINES = 5000

# replay_oldest outcomes. A bool cannot distinguish "drained a file" from
# "nothing to do", and the drain loop needs different sleep intervals for each.
DRAINED = 'drained'   # one file replayed (or quarantined) — progress was made
EMPTY = 'empty'       # nothing to replay
RETRY = 'retry'       # retryable failure; the file was kept for another attempt


class Spool:
    """A directory of rotating line-protocol files buffering points on disk."""

    def __init__(
        self, directory, max_bytes=DEFAULT_MAX_BYTES, rotate_bytes=ROTATE_BYTES
    ):
        """Open a spool at ``directory``, capped at ``max_bytes`` total and
        rotating to a new file every ``rotate_bytes``. Durability is disabled
        (``self.enabled`` False) if the directory cannot be created.

        Safe for one appending thread plus one draining thread: ``_lock`` guards
        every directory read/mutate, and the drain claims a file by renaming it
        out of the ``.lp`` namespace before reading it, so the appender can never
        select a file that is being replayed.
        """
        self.dir = Path(directory)
        self.max_bytes = max_bytes
        self.rotate_bytes = rotate_bytes
        # Never held across a network write — only across fast metadata calls.
        # Not reentrant: no locked method may call another locked method.
        self._lock = threading.Lock()
        self.enabled = self._ensure_dir()
        if self.enabled:
            self._reclaim_orphans()

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

    def _reclaim_orphans(self):
        """Return files claimed by a drain that died mid-replay to the live set.

        A claim is a rename, so a crash (or the watchdog's sys.exit) between
        claiming and finishing would otherwise strand the file outside every
        glob, invisible forever.
        """
        # Materialize before renaming: Path.glob is lazy over os.scandir, and
        # mutating directory entries mid-iteration can skip or repeat entries on
        # a hash-ordered directory (ext4 dir_index) -- a skipped claim would stay
        # stranded until the next restart.
        for path in list(self.dir.glob(f'*{_CLAIM_SUFFIX}')):
            try:
                path.rename(path.with_suffix(_SUFFIX))
                logger.warning("Reclaimed interrupted spool file %s", path.name)
            except OSError as e:
                logger.warning("Could not reclaim spool file %s: %s", path.name, e)

    @staticmethod
    def _size(path):
        """st_size, or 0 if the file vanished (the other thread unlinked it)."""
        try:
            return path.stat().st_size
        except OSError:
            return 0

    def pending(self):
        """(file count, total bytes) still awaiting replay. For drain logging."""
        if not self.enabled:
            return (0, 0)
        with self._lock:
            files = self._files()
            return (len(files), sum(self._size(f) for f in files))

    def _files(self):
        """Return the live (.lp) spool files, oldest first (empty if disabled)."""
        if not self.enabled:
            return []
        return sorted(self.dir.glob(f'*{_SUFFIX}'))

    def _next_seq(self):
        """Return the next file sequence number, one past the highest existing.

        Considers live (.lp), quarantined (.bad) AND in-flight (.replaying)
        files so the counter never resets and collides with a lingering
        quarantined file or one the drain thread is mid-replay on.
        """
        # Derive the next sequence from live (.lp), quarantined (.bad) and
        # claimed (.replaying) files: once every .lp drains, a lingering .bad or
        # a claimed file must not let the counter reset to 1 and collide with
        # (rename would clobber) or mis-order against it.
        seqs = [
            int(p.stem)
            for p in (
                *self.dir.glob(f'*{_SUFFIX}'),
                *self.dir.glob(f'*{_BAD_SUFFIX}'),
                *self.dir.glob(f'*{_CLAIM_SUFFIX}'),
            )
        ]
        return (max(seqs) + 1) if seqs else 1

    def _append_path(self):
        """Path to append to: the newest file if under the rotate threshold,
        otherwise a freshly sequenced file."""
        files = self._files()
        if files and self._size(files[-1]) < self.rotate_bytes:
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
        # Serialized before taking the lock: to_line_protocol() is pure CPU.
        blob = ''.join(p.to_line_protocol() + '\n' for p in points).encode()
        with self._lock:
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

        Counts live (.lp), quarantined (.bad) and in-flight (.replaying) files
        toward the cap, but only ever evicts the first two — unlinking a file the
        drain thread is mid-replay on would lose the points it has not yet sent.
        Always keeps at least one evictable file. Caller must hold ``_lock``.
        """
        # Count quarantined (.bad) files toward the cap: a recurring stream of
        # poison/unreadable files must not accumulate .bad files past
        # telem.spool.max_size on a constrained device. All suffixes are
        # zero-padded-sequence names, so a plain name sort is oldest-first.
        evictable = sorted(
            [*self.dir.glob(f'*{_SUFFIX}'), *self.dir.glob(f'*{_BAD_SUFFIX}')],
            key=lambda p: p.name,
        )
        claimed = list(self.dir.glob(f'*{_CLAIM_SUFFIX}'))
        total = sum(self._size(f) for f in (*evictable, *claimed))
        dropped = 0
        while total > self.max_bytes and len(evictable) > 1:
            victim = evictable.pop(0)
            total -= self._size(victim)
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

    def _write_chunked(self, write_api, bucket, lines):
        """Write ``lines`` to InfluxDB in REPLAY_CHUNK_LINES batches.

        Exceptions propagate to the caller, which owns the retry/quarantine
        decision. Blank lines are dropped: line protocol rejects an empty record,
        and a torn write can leave one behind.
        """
        for i in range(0, len(lines), REPLAY_CHUNK_LINES):
            batch = [line for line in lines[i:i + REPLAY_CHUNK_LINES] if line]
            if not batch:
                continue
            write_api.write(bucket=bucket, record='\n'.join(batch) + '\n')

    def _claim_oldest(self):
        """Rename the oldest live file out of the .lp namespace and return it.

        Returns None when there is nothing to claim. Renaming is the whole point:
        it is atomic, so _append_path (which globs .lp only) can never hand the
        appending thread a file the drain is about to read and delete.
        """
        with self._lock:
            for path in sorted(self.dir.glob(f'*{_SUFFIX}')):
                claimed = path.with_suffix(_CLAIM_SUFFIX)
                try:
                    path.rename(claimed)
                except OSError as e:
                    logger.warning("Could not claim spool file %s: %s", path.name, e)
                    continue
                return claimed
        return None

    def _release(self, path):
        """Return a claimed file to the live set after a retryable failure."""
        with self._lock:
            try:
                path.rename(path.with_suffix(_SUFFIX))
            except OSError as e:
                logger.warning("Could not release spool file %s: %s", path.name, e)

    def _finish(self, path):
        """Delete a fully-replayed claimed file. Always returns DRAINED.

        On unlink failure the file stays claimed, which keeps it out of every
        future _claim_oldest glob. That is deliberate: returning it to the live
        set would make the drain re-upload the same megabytes every cycle
        forever. It is retried once on the next process start, via
        _reclaim_orphans.
        """
        with self._lock:
            try:
                path.unlink(missing_ok=True)
            except OSError as e:
                logger.error(
                    "Could not remove replayed spool file %s: %s; leaving it "
                    "claimed so it is not replayed again this process",
                    path.name, e)
        return DRAINED

    def _quarantine(self, path):
        """Rename a file that InfluxDB will not accept to <name>.bad.

        On rename failure the file stays claimed rather than returning to the
        live set, for the same reason as _finish: re-reading a file InfluxDB has
        already rejected would replay the whole reject/salvage cycle on every
        drain pass forever. _reclaim_orphans retries it on the next process
        start.
        """
        with self._lock:
            target = path.with_suffix(_BAD_SUFFIX)
            try:
                path.rename(target)
            except OSError as e:
                logger.warning(
                    "Could not quarantine spool file %s -> %s: %s",
                    path.name, target.name, e)
                return
        logger.error("Quarantined unwritable spool file %s -> %s", path.name, target.name)

    def replay_oldest(self, write_api, bucket):
        """Replay the oldest spool file through write_api; delete it on success.

        Returns DRAINED when a file was replayed or quarantined (progress either
        way), EMPTY when there was nothing to do, and RETRY when the write failed
        for a retryable/connectivity reason — the file is kept for another
        attempt. Safe to call from a thread other than the one calling append().
        """
        if not self.enabled:
            return EMPTY
        path = self._claim_oldest()
        if path is None:
            return EMPTY
        try:
            text = path.read_text()
        except FileNotFoundError:
            # A claim is never evicted (_enforce_cap skips .replaying), so the
            # only way here is something outside this process removing the file:
            # manual cleanup, or an operator clearing the spool dir. Benign —
            # not corruption, and must not be logged as such.
            logger.debug("Spool file %s vanished before replay", path.name)
            return DRAINED
        except OSError as e:
            logger.error("Cannot read spool file %s: %s; quarantining", path.name, e)
            self._quarantine(path)
            return DRAINED
        lines = text.splitlines()
        try:
            self._write_chunked(write_api, bucket, lines)
        except ApiException as e:
            if e.status and 400 <= e.status < 500 and e.status != 429:
                return self._handle_corrupt(write_api, bucket, path, lines)
            logger.debug("Spool replay of %s failed: %s", path.name, e)
            self._release(path)
            return RETRY
        except Exception as e:
            logger.debug("Spool replay of %s failed: %s", path.name, e)
            self._release(path)
            return RETRY
        logger.info("Replayed spool file %s", path.name)
        return self._finish(path)

    def _handle_corrupt(self, write_api, bucket, path, lines):
        """Salvage a 4xx-rejected file by dropping its (possibly torn) last line;
        quarantine to <name>.bad if it still will not write.

        A *retryable* failure of the salvage write (5xx / 429 / unknown status /
        connectivity) keeps the file and returns RETRY, so its good lines are not
        thrown away over a transient hiccup between the two writes; only a genuine
        4xx rejection (or an unsalvageable single-line file) is quarantined. The
        salvage write re-chunks from the start, so any chunk that was already
        accepted before the failing chunk is re-sent here; that is harmless
        because replay is idempotent.
        """
        if len(lines) > 1:
            try:
                self._write_chunked(write_api, bucket, lines[:-1])
            except ApiException as e:
                if not (e.status and 400 <= e.status < 500 and e.status != 429):
                    self._release(path)
                    return RETRY  # retryable — keep the file, try again later
                # genuine 4xx on the salvaged data too — fall through to quarantine
            except Exception:
                self._release(path)
                return RETRY  # connectivity failure — keep the file
            else:
                logger.warning(
                    "Dropped 1 unwritable line from spool file %s", path.name
                )
                return self._finish(path)
        self._quarantine(path)
        return DRAINED
