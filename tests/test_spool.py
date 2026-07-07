import logging
from pathlib import Path
from unittest.mock import MagicMock

from influxdb_client import Point
from influxdb_client.rest import ApiException

from lemongrass._spool import DEFAULT_MAX_BYTES, DEFAULT_SPOOL_DIR, Spool


def _pt(measurement, value):
    return Point(measurement).field("value", value)


class TestConfig:
    def test_from_config_defaults(self, monkeypatch):
        monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
        monkeypatch.setattr(Spool, "_ensure_dir", lambda self: True)
        s = Spool.from_config()
        assert str(s.dir) == DEFAULT_SPOOL_DIR
        assert s.max_bytes == DEFAULT_MAX_BYTES

    def test_from_config_reads_file(self, monkeypatch, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text(
            '[telem.spool]\n'
            f'dir = "{tmp_path / "spool"}"\n'
            'max_size = "2GiB"\n'
        )
        monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
        monkeypatch.setattr(Spool, "_ensure_dir", lambda self: True)
        s = Spool.from_config()
        assert str(s.dir) == str(tmp_path / "spool")
        assert s.max_bytes == 2 * 1024 ** 3


class TestAppend:
    def test_append_writes_line_protocol(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 3500), _pt("SPEED", 60)])
        files = list((tmp_path / "spool").glob("*.lp"))
        assert len(files) == 1
        text = files[0].read_text()
        assert "RPM value=3500i" in text
        assert "SPEED value=60i" in text
        assert text.endswith("\n")

    def test_append_ignores_empty(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([])
        assert list((tmp_path / "spool").glob("*.lp")) == []

    def test_append_reuses_file_until_rotate_size(self, tmp_path):
        s = Spool(tmp_path / "spool", rotate_bytes=10_000)
        s.append([_pt("RPM", 1)])
        s.append([_pt("RPM", 2)])
        assert len(list((tmp_path / "spool").glob("*.lp"))) == 1

    def test_append_rotates_past_rotate_size(self, tmp_path):
        s = Spool(tmp_path / "spool", rotate_bytes=1)  # every append rotates
        s.append([_pt("RPM", 1)])
        s.append([_pt("RPM", 2)])
        names = sorted(p.name for p in (tmp_path / "spool").glob("*.lp"))
        assert names == ["000000000001.lp", "000000000002.lp"]

    def test_enforce_cap_drops_oldest_keeps_newest(self, tmp_path, caplog):
        s = Spool(tmp_path / "spool", max_bytes=1, rotate_bytes=1)
        with caplog.at_level(logging.WARNING):
            s.append([_pt("RPM", 1)])
            s.append([_pt("RPM", 2)])
            s.append([_pt("RPM", 3)])
        # cap is tiny, so only the newest (currently-appended) file survives
        remaining = sorted(p.name for p in (tmp_path / "spool").glob("*.lp"))
        assert remaining == ["000000000003.lp"]
        assert any("dropped" in r.message.lower() for r in caplog.records)

    def test_enforce_cap_counts_and_evicts_bad_files(self, tmp_path):
        # Quarantined .bad files must count toward the cap and be evicted
        # oldest-first alongside .lp, so poison can't grow the spool past it.
        s = Spool(tmp_path / "spool", max_bytes=1, rotate_bytes=1)
        s.append([_pt("RPM", 1)])                       # 000000000001.lp
        bad = s.dir / "000000000000.bad"                # older-ordering quarantine
        bad.write_text("old poison\n")
        s.append([_pt("RPM", 2)])                       # 000000000002.lp -> enforce
        assert not bad.exists()                         # .bad counted + evicted
        assert (s.dir / "000000000002.lp").exists()     # newest .lp preserved

    def test_append_preserves_explicit_ns_timestamp(self, tmp_path):
        # Replay idempotency depends on each spooled point carrying its capture
        # time in the line protocol (Influx upserts by measurement+tags+time).
        # Guard that append serializes an explicit ns timestamp rather than
        # dropping it, which would let Influx assign a replay-time timestamp.
        from influxdb_client import WritePrecision
        s = Spool(tmp_path / "spool")
        ts = 1_700_000_000_000_000_000  # fixed ns
        s.append([Point("RPM").field("value", 1).time(ts, WritePrecision.NS)])
        text = next((tmp_path / "spool").glob("*.lp")).read_text()
        assert text.strip().endswith(str(ts))

    def test_new_file_seq_does_not_reset_below_lingering_bad(self, tmp_path):
        # After every .lp drains, a lingering .bad must not let the sequence
        # reset to 1 and collide with / mis-order against the quarantined file.
        s = Spool(tmp_path / "spool", rotate_bytes=1)
        s.append([_pt("RPM", 1)])                        # 000000000001.lp
        # simulate: that file was quarantined high, then all live .lp drained
        (s.dir / "000000000001.lp").rename(s.dir / "000000000005.bad")
        s.append([_pt("RPM", 2)])                        # must be seq > 5
        names = sorted(p.name for p in s.dir.glob("*.lp"))
        assert names == ["000000000006.lp"]

    def test_new_file_triggers_dir_fsync(self, tmp_path, monkeypatch):
        # A newly-created (rotated) spool file needs its directory entry fsync'd
        # so the file survives a hard fault before dir metadata is flushed.
        s = Spool(tmp_path / "spool", rotate_bytes=1)  # every append rotates
        calls = []
        monkeypatch.setattr(s, "_fsync_dir", lambda: calls.append(1), raising=False)
        s.append([_pt("RPM", 1)])                        # new file -> dir fsync
        s.append([_pt("RPM", 2)])                        # new file -> dir fsync
        assert len(calls) == 2

    def test_append_to_existing_file_skips_dir_fsync(self, tmp_path, monkeypatch):
        s = Spool(tmp_path / "spool", rotate_bytes=10_000)  # reuse one file
        calls = []
        monkeypatch.setattr(s, "_fsync_dir", lambda: calls.append(1), raising=False)
        s.append([_pt("RPM", 1)])                        # new file -> 1 dir fsync
        s.append([_pt("RPM", 2)])                        # reuse -> no dir fsync
        assert len(calls) == 1


class TestDegradation:
    def test_unusable_dir_disables_without_crashing(self, tmp_path):
        # a *file* where the dir should be makes mkdir fail
        blocker = tmp_path / "blocker"
        blocker.write_text("x")
        s = Spool(blocker / "spool")
        assert s.enabled is False
        s.append([_pt("RPM", 1)])  # must not raise


BUCKET = "stats_252/autogen"


class TestReplay:
    def test_empty_spool_returns_true(self, tmp_path):
        s = Spool(tmp_path / "spool")
        assert s.replay_oldest(MagicMock(), BUCKET) is True

    def test_replays_and_deletes_oldest_first(self, tmp_path):
        s = Spool(tmp_path / "spool", rotate_bytes=1)
        s.append([_pt("RPM", 1)])   # -> 000000000001.lp
        s.append([_pt("RPM", 2)])   # -> 000000000002.lp
        write_api = MagicMock()
        assert s.replay_oldest(write_api, BUCKET) is True
        sent = write_api.write.call_args.kwargs["record"]
        assert "RPM value=1i" in sent
        remaining = sorted(p.name for p in (tmp_path / "spool").glob("*.lp"))
        assert remaining == ["000000000002.lp"]

    def test_connectivity_failure_keeps_file(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1)])
        write_api = MagicMock()
        write_api.write.side_effect = ConnectionError("influx down")
        assert s.replay_oldest(write_api, BUCKET) is False
        assert len(list((tmp_path / "spool").glob("*.lp"))) == 1

    def test_5xx_keeps_file(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1)])
        write_api = MagicMock()
        write_api.write.side_effect = ApiException(status=503, reason="unavailable")
        assert s.replay_oldest(write_api, BUCKET) is False
        assert len(list((tmp_path / "spool").glob("*.lp"))) == 1

    def test_torn_last_line_is_salvaged(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1), _pt("RPM", 2)])
        # simulate a crash-torn final line appended after the good ones
        f = next((tmp_path / "spool").glob("*.lp"))
        with open(f, "a") as fh:
            fh.write("RPM value=BROKEN")  # no newline, invalid
        write_api = MagicMock()
        # full-file write 400s; salvaged (last line dropped) write succeeds
        write_api.write.side_effect = [ApiException(status=400, reason="bad"), None]
        assert s.replay_oldest(write_api, BUCKET) is True
        salvaged = write_api.write.call_args_list[1].kwargs["record"]
        assert "RPM value=1i" in salvaged and "BROKEN" not in salvaged
        assert list((tmp_path / "spool").glob("*.lp")) == []

    def test_unsalvageable_file_is_quarantined(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1)])
        write_api = MagicMock()
        write_api.write.side_effect = ApiException(status=400, reason="bad")
        assert s.replay_oldest(write_api, BUCKET) is True  # progress: file removed from queue
        assert list((tmp_path / "spool").glob("*.lp")) == []
        assert len(list((tmp_path / "spool").glob("*.bad"))) == 1

    def test_salvage_retryable_5xx_keeps_file_not_quarantined(self, tmp_path):
        # Full-file write 400s (corrupt); the salvage retry hits a transient 5xx.
        # The good lines must be kept for a later retry, NOT quarantined.
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1), _pt("RPM", 2)])
        write_api = MagicMock()
        write_api.write.side_effect = [
            ApiException(status=400, reason="bad"),
            ApiException(status=503, reason="unavailable"),
        ]
        assert s.replay_oldest(write_api, BUCKET) is False
        assert len(list((tmp_path / "spool").glob("*.lp"))) == 1
        assert list((tmp_path / "spool").glob("*.bad")) == []

    def test_salvage_connectivity_failure_keeps_file(self, tmp_path):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1), _pt("RPM", 2)])
        write_api = MagicMock()
        write_api.write.side_effect = [
            ApiException(status=400, reason="bad"),
            ConnectionError("influx down"),
        ]
        assert s.replay_oldest(write_api, BUCKET) is False
        assert len(list((tmp_path / "spool").glob("*.lp"))) == 1
        assert list((tmp_path / "spool").glob("*.bad")) == []

    def test_salvage_second_4xx_quarantines(self, tmp_path):
        # A genuine 4xx on the salvaged data too means it really is corrupt.
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1), _pt("RPM", 2)])
        write_api = MagicMock()
        write_api.write.side_effect = [
            ApiException(status=400, reason="bad"),
            ApiException(status=400, reason="still bad"),
        ]
        assert s.replay_oldest(write_api, BUCKET) is True
        assert list((tmp_path / "spool").glob("*.lp")) == []
        assert len(list((tmp_path / "spool").glob("*.bad"))) == 1

    def test_unreadable_oldest_file_is_quarantined(self, tmp_path, monkeypatch, caplog):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1)])
        write_api = MagicMock()

        def failing_read_text(self, *args, **kwargs):
            raise OSError("I/O error")

        monkeypatch.setattr(Path, "read_text", failing_read_text)
        with caplog.at_level(logging.ERROR):
            result = s.replay_oldest(write_api, BUCKET)
        assert result is True
        assert list((tmp_path / "spool").glob("*.lp")) == []
        assert len(list((tmp_path / "spool").glob("*.bad"))) == 1
        write_api.write.assert_not_called()

    def test_unlink_oserror_does_not_propagate(self, tmp_path, monkeypatch, caplog):
        s = Spool(tmp_path / "spool")
        s.append([_pt("RPM", 1)])
        write_api = MagicMock()
        # Make unlink raise OSError to simulate permission or disk error
        def failing_unlink(self, missing_ok=False):
            raise OSError("Permission denied")
        monkeypatch.setattr(Path, "unlink", failing_unlink)
        with caplog.at_level(logging.WARNING):
            result = s.replay_oldest(write_api, BUCKET)
        # Should return True (data written successfully, cleanup error is non-fatal)
        assert result is True
        # Warning should be logged about the unlink failure
        assert any("Could not remove replayed spool file" in r.message for r in caplog.records)


class TestRoundTrip:
    def test_outage_then_recovery_delivers_exact_points(self, tmp_path):
        spool = Spool(tmp_path / "spool")

        # --- outage: three batches fail to write and are spilled ---
        spool.append([_pt("RPM", 1000), _pt("SPEED", 10)])
        spool.append([_pt("RPM", 2000)])
        spool.append([_pt("RPM", 3000)])
        assert len(list((tmp_path / "spool").glob("*.lp"))) >= 1

        # --- recovery: write_api works; drain until empty ---
        write_api = MagicMock()
        for _ in range(10):
            if spool.replay_oldest(write_api, BUCKET) and not list(
                (tmp_path / "spool").glob("*.lp")
            ):
                break

        assert list((tmp_path / "spool").glob("*.lp")) == []  # fully drained
        delivered = "".join(
            c.kwargs["record"] for c in write_api.write.call_args_list
        )
        for token in ("RPM value=1000i", "SPEED value=10i",
                      "RPM value=2000i", "RPM value=3000i"):
            assert token in delivered
