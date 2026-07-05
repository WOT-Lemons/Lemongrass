import logging
from unittest.mock import MagicMock

from influxdb_client import Point
from influxdb_client.rest import ApiException

from lemongrass._spool import DEFAULT_MAX_BYTES, DEFAULT_SPOOL_DIR, Spool


def _pt(measurement, value):
    return Point(measurement).field("value", value)


class TestConfig:
    def test_from_env_defaults(self, monkeypatch):
        monkeypatch.delenv("TELEM_SPOOL_DIR", raising=False)
        monkeypatch.delenv("TELEM_SPOOL_MAX_BYTES", raising=False)
        s = Spool.from_env()
        assert str(s.dir) == DEFAULT_SPOOL_DIR
        assert s.max_bytes == DEFAULT_MAX_BYTES

    def test_from_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("TELEM_SPOOL_DIR", str(tmp_path / "spool"))
        monkeypatch.setenv("TELEM_SPOOL_MAX_BYTES", "12345")
        s = Spool.from_env()
        assert str(s.dir) == str(tmp_path / "spool")
        assert s.max_bytes == 12345


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
