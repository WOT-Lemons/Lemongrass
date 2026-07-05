import logging
from unittest.mock import MagicMock, patch

import pytest
from influxdb_client import Point

import lemongrass.telem as _mod
from lemongrass._spool import Spool


class _Cmd:
    def __init__(self, s, name=None):
        self._s = s
        self.name = name if name is not None else s

    def __str__(self):
        return self._s


def _reset():
    _mod.pending_points.clear()
    _mod._last_dtc_count = 0
    _mod._dtc_fetch_failures = 0
    _mod._connection = None
    _mod._spool = None
    _mod._spooling = False
    _mod._vin = "unknown"


class TestConnect:
    def test_defaults_to_obd_symlink(self, monkeypatch):
        monkeypatch.delenv("OBD_PORT", raising=False)
        monkeypatch.delenv("OBD_BAUDRATE", raising=False)
        with patch.object(_mod.obd, "Async") as mock_async:
            _mod.connect()
        mock_async.assert_called_once_with(portstr="/dev/obd")

    def test_uses_obd_port_env_override(self, monkeypatch):
        monkeypatch.setenv("OBD_PORT", "/dev/ttyUSB0")
        monkeypatch.delenv("OBD_BAUDRATE", raising=False)
        with patch.object(_mod.obd, "Async") as mock_async:
            _mod.connect()
        mock_async.assert_called_once_with(portstr="/dev/ttyUSB0")

    def test_passes_baudrate_when_set(self, monkeypatch):
        monkeypatch.setenv("OBD_PORT", "socket://elm327:35000")
        monkeypatch.setenv("OBD_BAUDRATE", "38400")
        with patch.object(_mod.obd, "Async") as mock_async:
            _mod.connect()
        mock_async.assert_called_once_with(
            portstr="socket://elm327:35000", baudrate=38400
        )

    def test_omits_baudrate_when_unset(self, monkeypatch):
        monkeypatch.setenv("OBD_PORT", "/dev/obd")
        monkeypatch.delenv("OBD_BAUDRATE", raising=False)
        with patch.object(_mod.obd, "Async") as mock_async:
            _mod.connect()
        _, kwargs = mock_async.call_args
        assert "baudrate" not in kwargs


class TestNewValue:
    def setup_method(self):
        _reset()

    def test_appends_point_for_valid_response(self):
        r = MagicMock()
        r.command = _Cmd("ELM_VOLTAGE: voltage")
        r.value.magnitude = 12.5
        _mod.new_value(r)
        assert len(_mod.pending_points) == 1

    def test_skips_command_without_colon(self):
        r = MagicMock()
        r.command = _Cmd("NO_COLON")
        _mod.new_value(r)
        assert len(_mod.pending_points) == 0

    def test_skips_on_attribute_error(self):
        r = MagicMock()
        r.command = _Cmd("CMD: name")
        r.value = None  # None.magnitude raises AttributeError
        _mod.new_value(r)
        assert len(_mod.pending_points) == 0

    def test_skips_on_type_error_from_point(self):
        r = MagicMock()
        r.command = _Cmd("CMD: name")
        mock_point = MagicMock()
        mock_point.field.side_effect = TypeError
        with patch.object(_mod, 'Point', return_value=mock_point):
            _mod.new_value(r)
        assert len(_mod.pending_points) == 0

    def test_measurement_name_has_spaces_replaced(self):
        r = MagicMock()
        r.command = _Cmd("ENGINE RPM: rpm")
        r.value.magnitude = 3000.0
        captured_name = []
        original_point = _mod.Point

        def capture_point(name):
            captured_name.append(name)
            return original_point(name)

        with patch.object(_mod, 'Point', side_effect=capture_point):
            _mod.new_value(r)
        assert captured_name[0] == "-rpm"


class TestNewFuelStatus:
    def setup_method(self):
        _reset()

    def test_appends_known_status(self):
        r = MagicMock()
        r.command = _Cmd("b'0103': Fuel System Status")
        r.value = ["Closed loop, using oxygen sensor feedback to determine fuel mix"]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 1

    def test_skips_falsy_first_element(self):
        r = MagicMock()
        r.command = _Cmd("b'0103': Fuel System Status")
        r.value = [None]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 0

    def test_appends_for_unknown_status(self):
        r = MagicMock()
        r.command = _Cmd("b'0103': Fuel System Status")
        r.value = ["Unknown mode that maps to nothing"]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 1

    def test_returns_early_on_malformed_command(self):
        r = MagicMock()
        r.command = _Cmd("no colon here")
        r.value = ["Closed loop, using oxygen sensor feedback to determine fuel mix"]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 0

    def test_measurement_name_uses_description(self):
        r = MagicMock()
        r.command = _Cmd("b'0103': Fuel System Status")
        r.value = ["Closed loop, using oxygen sensor feedback to determine fuel mix"]
        captured_name = []
        original_point = _mod.Point

        def capture_point(name):
            captured_name.append(name)
            return original_point(name)

        with patch.object(_mod, "Point", side_effect=capture_point):
            _mod.new_fuel_status(r)
        assert captured_name[0] == "-Fuel-System-Status"

    def test_uses_system_1_status_when_systems_differ(self):
        """Dual-bank ECU: system 1 in failure mode must not be masked by
        system 2's closed-loop status (dict order used to decide the winner)."""
        r = MagicMock()
        r.command = _Cmd("b'0103': Fuel System Status")
        r.value = [
            "Open loop due to system failure",
            "Closed loop, using oxygen sensor feedback to determine fuel mix",
        ]
        _mod.new_fuel_status(r)
        assert _mod.pending_points[0]._fields == {"value": 3}


class TestConfigureObdLogging:
    def test_no_debug_by_default(self, monkeypatch):
        monkeypatch.delenv("OBD_DEBUG", raising=False)
        with patch.object(_mod.obd.logger, "setLevel") as mock_set_level:
            _mod._configure_obd_logging()
        mock_set_level.assert_not_called()

    def test_debug_enabled_via_env(self, monkeypatch):
        monkeypatch.setenv("OBD_DEBUG", "1")
        with patch.object(_mod.obd.logger, "setLevel") as mock_set_level:
            _mod._configure_obd_logging()
        mock_set_level.assert_called_once_with(logging.DEBUG)


class TestNewAirStatus:
    def setup_method(self):
        _reset()

    def test_appends_known_status(self):
        r = MagicMock()
        r.command = _Cmd("b'0112': Secondary Air Status")
        r.value = "Upstream"
        _mod.new_air_status(r)
        assert len(_mod.pending_points) == 1
        assert _mod.pending_points[0]._fields == {"value": 0}

    def test_skips_falsy_value(self):
        r = MagicMock()
        r.command = _Cmd("b'0112': Secondary Air Status")
        r.value = None
        _mod.new_air_status(r)
        assert len(_mod.pending_points) == 0

    def test_appends_for_unknown_status(self):
        r = MagicMock()
        r.command = _Cmd("b'0112': Secondary Air Status")
        r.value = "Some new status ELM327 hasn't seen"
        _mod.new_air_status(r)
        assert len(_mod.pending_points) == 1
        assert _mod.pending_points[0]._fields == {"value": 255}

    def test_returns_early_on_malformed_command(self):
        r = MagicMock()
        r.command = _Cmd("no colon here")
        r.value = "Upstream"
        _mod.new_air_status(r)
        assert len(_mod.pending_points) == 0

    def test_measurement_name_uses_description(self):
        r = MagicMock()
        r.command = _Cmd("b'0112': Secondary Air Status")
        r.value = "Upstream"
        captured_name = []
        original_point = _mod.Point

        def capture_point(name):
            captured_name.append(name)
            return original_point(name)

        with patch.object(_mod, "Point", side_effect=capture_point):
            _mod.new_air_status(r)
        assert captured_name[0] == "-Secondary-Air-Status"


class TestQueryFuelTypeOnce:
    def setup_method(self):
        _reset()

    def test_skips_when_unsupported(self):
        connection = MagicMock()
        connection.supports.return_value = False
        with patch.object(_mod.obd.OBD, "query") as mock_query:
            _mod._query_fuel_type_once(connection)
        mock_query.assert_not_called()
        assert len(_mod.pending_points) == 0

    def test_writes_point_when_supported(self):
        connection = MagicMock()
        connection.supports.return_value = True
        r = MagicMock()
        r.command = _Cmd("b'0151': Fuel Type")
        r.value = "Gasoline"
        with patch.object(_mod.obd.OBD, "query", return_value=r) as mock_query:
            _mod._query_fuel_type_once(connection)
        mock_query.assert_called_once_with(
            connection, _mod.obd.commands.FUEL_TYPE, force=True
        )
        assert len(_mod.pending_points) == 1
        assert _mod.pending_points[0]._fields == {"value": "Gasoline"}

    def test_skips_on_falsy_value(self):
        connection = MagicMock()
        connection.supports.return_value = True
        r = MagicMock()
        r.command = _Cmd("b'0151': Fuel Type")
        r.value = None
        with patch.object(_mod.obd.OBD, "query", return_value=r):
            _mod._query_fuel_type_once(connection)
        assert len(_mod.pending_points) == 0

    def test_measurement_name_uses_description(self):
        connection = MagicMock()
        connection.supports.return_value = True
        r = MagicMock()
        r.command = _Cmd("b'0151': Fuel Type")
        r.value = "Gasoline"
        captured_name = []
        original_point = _mod.Point

        def capture_point(name):
            captured_name.append(name)
            return original_point(name)

        with patch.object(_mod.obd.OBD, "query", return_value=r), \
                patch.object(_mod, "Point", side_effect=capture_point):
            _mod._query_fuel_type_once(connection)
        assert captured_name[0] == "-Fuel-Type"


class TestResolveVin:
    def setup_method(self):
        _reset()

    def test_uses_obd_vin_when_supported(self, monkeypatch):
        monkeypatch.delenv("CAR_VIN", raising=False)
        connection = MagicMock()
        connection.supports.return_value = True
        r = MagicMock()
        r.value = "1FATESTVIN0000001"
        with patch.object(_mod.obd.OBD, "query", return_value=r) as mock_query:
            vin = _mod._resolve_vin(connection)
        mock_query.assert_called_once_with(
            connection, _mod.obd.commands.VIN, force=True
        )
        assert vin == "1FATESTVIN0000001"

    def test_falls_back_to_env_when_obd_unsupported(self, monkeypatch):
        monkeypatch.setenv("CAR_VIN", "ENVVIN00000000001")
        connection = MagicMock()
        connection.supports.return_value = False
        with patch.object(_mod.obd.OBD, "query") as mock_query:
            vin = _mod._resolve_vin(connection)
        mock_query.assert_not_called()
        assert vin == "ENVVIN00000000001"

    def test_falls_back_to_env_when_obd_query_raises(self, monkeypatch):
        monkeypatch.setenv("CAR_VIN", "ENVVIN00000000002")
        connection = MagicMock()
        connection.supports.return_value = True
        with patch.object(_mod.obd.OBD, "query", side_effect=Exception("boom")):
            vin = _mod._resolve_vin(connection)
        assert vin == "ENVVIN00000000002"

    def test_unknown_when_nothing_resolves(self, monkeypatch):
        monkeypatch.delenv("CAR_VIN", raising=False)
        connection = MagicMock()
        connection.supports.return_value = True
        r = MagicMock()
        r.value = None
        with patch.object(_mod.obd.OBD, "query", return_value=r):
            vin = _mod._resolve_vin(connection)
        assert vin == "unknown"

    def test_warns_and_prefers_obd_on_mismatch(self, monkeypatch, caplog):
        monkeypatch.setenv("CAR_VIN", "ENVVIN00000000003")
        connection = MagicMock()
        connection.supports.return_value = True
        r = MagicMock()
        r.value = "OBDVIN00000000003"
        with patch.object(_mod.obd.OBD, "query", return_value=r), \
                caplog.at_level(logging.WARNING):
            vin = _mod._resolve_vin(connection)
        assert vin == "OBDVIN00000000003"
        assert any("differs from CAR_VIN" in m for m in caplog.messages)


class TestNewStatus:
    def setup_method(self):
        _reset()

    def test_skips_null_value(self):
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value = None
        _mod.new_status(r)
        assert len(_mod.pending_points) == 0

    def test_writes_mil_and_dtc_count_points(self):
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 0
        _mod.new_status(r)
        assert len(_mod.pending_points) == 2
        assert _mod.pending_points[0]._fields == {"value": 1}
        assert _mod.pending_points[1]._fields == {"value": 0}

    def test_returns_early_on_malformed_command(self):
        r = MagicMock()
        r.command = _Cmd("no colon here", name="STATUS")
        r.value.MIL = False
        r.value.DTC_count = 0
        _mod.new_status(r)
        assert len(_mod.pending_points) == 0

    def test_measurement_names_use_description(self):
        r = MagicMock()
        r.command = _Cmd(
            "b'0141': Monitor status this drive cycle", name="STATUS_DRIVE_CYCLE"
        )
        r.value.MIL = True
        r.value.DTC_count = 2
        captured_names = []
        original_point = _mod.Point

        def capture_point(name):
            captured_names.append(name)
            return original_point(name)

        with patch.object(_mod, "Point", side_effect=capture_point):
            _mod.new_status(r)
        assert captured_names == [
            "-Monitor-status-this-drive-cycle-MIL",
            "-Monitor-status-this-drive-cycle-DTC-Count",
        ]


class TestFetchAndStoreDtcs:
    def setup_method(self):
        _reset()

    def test_no_connection_set_does_nothing(self):
        result = _mod._fetch_and_store_dtcs()
        assert result is False
        assert len(_mod.pending_points) == 0

    def test_null_response_does_nothing(self):
        _mod._connection = MagicMock()
        r = MagicMock()
        r.value = None
        with patch.object(_mod.obd.OBD, "query", return_value=r) as mock_query:
            result = _mod._fetch_and_store_dtcs()
        mock_query.assert_called_once_with(
            _mod._connection, _mod.obd.commands.GET_DTC, force=True
        )
        assert result is False
        assert len(_mod.pending_points) == 0

    def test_empty_list_response_queues_empty_snapshot(self):
        # [] is a successful mode-03 answer ("no stored codes"), not a failure;
        # treating it as failure would retry the forced query on every STATUS.
        _mod._connection = MagicMock()
        r = MagicMock()
        r.value = []
        with patch.object(_mod.obd.OBD, "query", return_value=r):
            result = _mod._fetch_and_store_dtcs()
        assert result is True
        assert len(_mod.pending_points) == 1
        assert _mod.pending_points[0]._fields == {"value": ""}

    def test_writes_joined_codes(self):
        _mod._connection = MagicMock()
        r = MagicMock()
        r.value = [
            ("P0104", "Mass or Volume Air Flow Circuit Intermittent"),
            ("B0003", ""),
        ]
        with patch.object(_mod.obd.OBD, "query", return_value=r):
            result = _mod._fetch_and_store_dtcs()
        assert result is True
        assert len(_mod.pending_points) == 1
        assert _mod.pending_points[0]._fields == {"value": "P0104,B0003"}


class TestNewStatusDtcTrigger:
    def setup_method(self):
        _reset()
        _mod._connection = MagicMock()

    def test_triggers_fetch_on_status_dtc_count_increase(self):
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 1
        with patch.object(_mod, "_fetch_and_store_dtcs") as mock_fetch:
            _mod.new_status(r)
        mock_fetch.assert_called_once()
        assert _mod._last_dtc_count == 1

    def test_does_not_advance_count_when_fetch_fails(self):
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 1
        with patch.object(_mod, "_fetch_and_store_dtcs", return_value=False):
            _mod.new_status(r)
        assert _mod._last_dtc_count == 0

    def test_no_trigger_when_count_unchanged(self):
        _mod._last_dtc_count = 1
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 1
        with patch.object(_mod, "_fetch_and_store_dtcs") as mock_fetch:
            _mod.new_status(r)
        mock_fetch.assert_not_called()

    def test_gives_up_after_max_consecutive_fetch_failures(self):
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 1
        with patch.object(_mod, "_fetch_and_store_dtcs", return_value=False) as mock_fetch:
            for _ in range(3):
                _mod.new_status(r)
            assert _mod._last_dtc_count == 1  # gave up; stop hammering the adapter
            _mod.new_status(r)
        assert mock_fetch.call_count == 3

    def test_failure_count_resets_on_successful_fetch(self):
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 1
        with patch.object(_mod, "_fetch_and_store_dtcs", side_effect=[False, False, True]):
            for _ in range(3):
                _mod.new_status(r)
        assert _mod._last_dtc_count == 1
        assert _mod._dtc_fetch_failures == 0

    def test_no_trigger_for_status_drive_cycle(self):
        r = MagicMock()
        r.command = _Cmd(
            "b'0141': Monitor status this drive cycle", name="STATUS_DRIVE_CYCLE"
        )
        r.value.MIL = True
        r.value.DTC_count = 5
        with patch.object(_mod, "_fetch_and_store_dtcs") as mock_fetch:
            _mod.new_status(r)
        mock_fetch.assert_not_called()


class TestFlushPoints:
    def setup_method(self):
        _reset()
        _mod._spool = MagicMock()

    def test_returns_true_and_writes_all_pending(self):
        _mod.pending_points.extend(["p1", "p2", "p3"])
        write_api = MagicMock()
        assert _mod.flush_points(write_api) is True
        write_api.write.assert_called_once()
        assert len(_mod.pending_points) == 0

    def test_returns_true_when_nothing_pending(self):
        assert _mod.flush_points(MagicMock()) is True

    def test_flushes_in_batches(self):
        _mod.pending_points.extend(f"p{i}" for i in range(5))
        write_api = MagicMock()
        _mod.flush_points(write_api, batch_size=2)
        assert write_api.write.call_count == 3
        assert len(_mod.pending_points) == 0

    def test_failed_write_spills_batch_to_disk_and_clears_ram(self, tmp_path):
        # Real Spool on tmp_path: assert the batch actually lands on disk, not
        # merely that append() was called on a mock.
        _mod._spool = Spool(tmp_path / "spool")
        _mod.pending_points.append(Point("RPM").field("value", 3500))
        write_api = MagicMock()
        write_api.write.side_effect = Exception("network error")
        assert _mod.flush_points(write_api) is False
        assert _mod.pending_points == []
        spooled = list((tmp_path / "spool").glob("*.lp"))
        assert len(spooled) == 1
        assert "RPM value=3500i" in spooled[0].read_text()

    def test_spills_only_unwritten_on_midbatch_failure(self, tmp_path):
        _mod._spool = Spool(tmp_path / "spool")
        _mod.pending_points.extend(
            Point("RPM").field("value", i) for i in range(1, 6)
        )
        write_api = MagicMock()
        write_api.write.side_effect = [None, Exception("boom")]
        assert _mod.flush_points(write_api, batch_size=2) is False
        assert _mod.pending_points == []
        # First batch (values 1,2) reached Influx; only the unwritten remainder
        # (3,4,5) is spilled to disk.
        text = next((tmp_path / "spool").glob("*.lp")).read_text()
        assert "value=3i" in text and "value=4i" in text and "value=5i" in text
        assert "value=1i" not in text and "value=2i" not in text

    def test_failed_write_with_no_spool_does_not_crash(self):
        _mod._spool = None
        _mod.pending_points.append("p1")
        write_api = MagicMock()
        write_api.write.side_effect = Exception("boom")
        assert _mod.flush_points(write_api) is False

    def test_failed_write_falls_back_to_ram_when_spool_unusable(self, tmp_path):
        """A genuinely disabled spool (unusable dir) must not silently drop
        telemetry — the unwritten batch is re-queued in the bounded backlog."""
        blocker = tmp_path / "blocker"
        blocker.write_text("x")  # a file where the dir should be -> mkdir fails
        _mod._spool = Spool(blocker / "spool")
        assert _mod._spool.enabled is False
        _mod.pending_points.extend(["p1", "p2"])
        write_api = MagicMock()
        write_api.write.side_effect = Exception("network error")
        assert _mod.flush_points(write_api) is False
        assert _mod.pending_points == ["p1", "p2"]


class TestLoggingConfig:
    def test_quiets_urllib3_retry_warnings(self):
        """urllib3 logs a WARNING per retry attempt; during an outage the pump
        retries every 0.5s, which would bury our edge-triggered outage lines."""
        target = logging.getLogger("urllib3.connectionpool")
        original = target.level
        try:
            target.setLevel(logging.NOTSET)
            _mod._quiet_retry_logging()
            assert target.level == logging.ERROR
        finally:
            target.setLevel(original)


class TestInfluxConnectTuning:
    def setup_method(self):
        _reset()

    def test_main_connects_with_short_timeout_and_trimmed_retries(self):
        """The hot loop must build its Influx client with a short timeout and a
        trimmed retry budget so a downed Influx fails fast to the spool."""
        with patch.object(_mod._influx, "connect") as influx_connect, \
                patch.object(_mod.Spool, "from_env"), \
                patch.object(_mod, "_configure_obd_logging"), \
                patch.object(_mod, "connect", side_effect=RuntimeError("stop")):
            with pytest.raises(RuntimeError, match="stop"):
                _mod.main()
        influx_connect.assert_called_once_with(
            timeout=_mod.WRITE_TIMEOUT_MS, retries=_mod.WRITE_RETRIES)


class TestSpoolStateLogging:
    """Outage logging is edge-triggered: one WARN when we start spooling, one
    INFO when Influx recovers, and no per-cycle spam in between."""

    def setup_method(self):
        _reset()
        _mod._spool = MagicMock()  # append() truthy -> stays on the spool path

    def _fail(self, write_api):
        _mod.pending_points.append(Point("RPM").field("value", 1))
        write_api.write.side_effect = Exception("influx down")
        _mod.flush_points(write_api)

    def _succeed(self, write_api):
        _mod.pending_points.append(Point("RPM").field("value", 2))
        write_api.write.side_effect = None
        _mod.flush_points(write_api)

    def test_first_spool_failure_logs_warning_and_sets_flag(self, caplog):
        write_api = MagicMock()
        with caplog.at_level(logging.DEBUG, logger="telem"):
            self._fail(write_api)
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("buffering telemetry to on-disk spool" in r.message
                   for r in warnings)
        assert _mod._spooling is True

    def test_repeated_failures_warn_once_then_debug(self, caplog):
        write_api = MagicMock()
        with caplog.at_level(logging.DEBUG, logger="telem"):
            self._fail(write_api)
            self._fail(write_api)
        warnings = [r for r in caplog.records
                    if r.levelname == "WARNING"
                    and "buffering telemetry" in r.message]
        assert len(warnings) == 1
        assert any(r.levelname == "DEBUG" and "still unreachable" in r.message
                   for r in caplog.records)

    def test_recovery_logs_info_and_clears_flag(self, caplog):
        write_api = MagicMock()
        self._fail(write_api)
        with caplog.at_level(logging.DEBUG, logger="telem"):
            self._succeed(write_api)
        assert any(r.levelname == "INFO" and "reachable again" in r.message
                   for r in caplog.records)
        assert _mod._spooling is False

    def test_empty_flush_does_not_clear_spooling(self, caplog):
        write_api = MagicMock()
        self._fail(write_api)
        assert _mod._spooling is True
        with caplog.at_level(logging.DEBUG, logger="telem"):
            _mod.flush_points(write_api)  # nothing pending -> early True
        assert _mod._spooling is True  # empty flush is not proof of recovery
        assert not any("reachable again" in r.message for r in caplog.records)


class TestPump:
    def setup_method(self):
        _reset()

    def test_replays_spool_when_flush_succeeds(self):
        _mod._spool = MagicMock()
        write_api = MagicMock()
        with patch.object(_mod, "flush_points", return_value=True):
            _mod._pump(write_api)
        _mod._spool.replay_oldest.assert_called_once_with(write_api, _mod.WRITE_BUCKET)

    def test_skips_replay_when_flush_fails(self):
        _mod._spool = MagicMock()
        write_api = MagicMock()
        with patch.object(_mod, "flush_points", return_value=False):
            _mod._pump(write_api)
        _mod._spool.replay_oldest.assert_not_called()

    def test_no_spool_does_not_crash(self):
        _mod._spool = None
        with patch.object(_mod, "flush_points", return_value=True):
            _mod._pump(MagicMock())  # must not raise

    def test_crash_invariant_backlog_is_on_disk_not_ram(self, tmp_path):
        """When Influx is down, the flush inside a pump cycle must leave the
        backlog spilled to disk (durable across the watchdog sys.exit), never in
        RAM. This is the load-bearing ordering the watchdog relies on."""
        _mod._spool = Spool(tmp_path / "spool")
        _mod.pending_points.append(Point("RPM").field("value", 3500))
        write_api = MagicMock()
        write_api.write.side_effect = ConnectionError("influx down")
        _mod._pump(write_api)
        assert _mod.pending_points == []                       # nothing left in RAM
        assert len(list((tmp_path / "spool").glob("*.lp"))) == 1  # durable on disk


class TestRouteCommand:
    def test_excludes_pattern_matches(self):
        for name in [
            "PIDS_A", "MIDS_A", "O2_SENSORS", "O2_SENSORS_ALT",
            "ELM_VERSION", "OBD_COMPLIANCE",
        ]:
            assert _mod._route_command(_Cmd(name)) is None

    def test_excludes_skip_names(self):
        for name in ["FREEZE_DTC", "GET_DTC", "CLEAR_DTC", "FUEL_TYPE"]:
            assert _mod._route_command(_Cmd(name)) is None

    def test_no_longer_excludes_dtc_substring_numeric_pids(self):
        for name in [
            "WARMUPS_SINCE_DTC_CLEAR",
            "DISTANCE_SINCE_DTC_CLEAR",
            "TIME_SINCE_DTC_CLEARED",
        ]:
            assert _mod._route_command(_Cmd(name)) is _mod.new_value

    def test_routes_status_commands(self):
        assert _mod._route_command(_Cmd("STATUS")) is _mod.new_status
        assert _mod._route_command(_Cmd("STATUS_DRIVE_CYCLE")) is _mod.new_status

    def test_routes_air_status(self):
        assert _mod._route_command(_Cmd("AIR_STATUS")) is _mod.new_air_status

    def test_routes_fuel_status(self):
        assert _mod._route_command(_Cmd("FUEL_STATUS")) is _mod.new_fuel_status

    def test_routes_generic_numeric_to_new_value(self):
        assert _mod._route_command(_Cmd("RPM")) is _mod.new_value

    def test_excludes_mode2_freeze_frame_twins(self):
        # python-obd adds a DTC_-prefixed mode-2 twin for every supported
        # mode-1 PID; watching them doubles per-cycle adapter load.
        for name in ["DTC_RPM", "DTC_FUEL_STATUS", "DTC_STATUS", "DTC_COOLANT_TEMP"]:
            assert _mod._route_command(_Cmd(name)) is None

    def test_excludes_get_current_dtc(self):
        assert _mod._route_command(_Cmd("GET_CURRENT_DTC")) is None

    def test_no_real_dtc_command_is_watched(self):
        # Guard against the real library inventory, not just names we expect:
        # no mode-2 freeze-frame twin and no mode-03/07 DTC command may route
        # to a callback.
        dtc_commands = {"GET_DTC", "GET_CURRENT_DTC", "CLEAR_DTC", "FREEZE_DTC"}
        checked = 0
        for mode in _mod.obd.commands.modes:
            for command in mode:
                if command is None:
                    continue
                if command.name.startswith("DTC_") or command.name in dtc_commands:
                    assert _mod._route_command(command) is None, command.name
                    checked += 1
        assert checked > 90  # the mode-2 twins alone number ~96; 0 means vacuous


class TestQueuePoint:
    def setup_method(self):
        _reset()
        _mod._last_append_monotonic = 0.0

    def test_appends_and_stamps_last_append_time(self):
        p = Point("p")
        _mod._queue_point(p)
        assert _mod.pending_points == [p]
        assert _mod._last_append_monotonic > 0

    def test_enqueue_capped_dropping_oldest_with_warning(self, caplog):
        """A hung flush must not let callbacks grow memory unbounded — and the
        drop must leave a trace in the logs, matching flush_points."""
        _mod._dropped_since_warn = 0
        _mod._last_drop_warn_monotonic = float('-inf')
        p1, p2, p3, p4 = Point("p1"), Point("p2"), Point("p3"), Point("p4")
        with patch.object(_mod, "MAX_PENDING_POINTS", 3):
            _mod.pending_points.extend([p1, p2, p3])
            with caplog.at_level(logging.WARNING):
                _mod._queue_point(p4)
        assert _mod.pending_points == [p2, p3, p4]
        assert any("dropped" in r.message.lower() for r in caplog.records)

    def test_enqueue_drop_warning_is_rate_limited(self, caplog):
        """One warning per interval, not one per dropped point — sustained
        saturation would otherwise flood journald with a line per callback."""
        _mod._dropped_since_warn = 0
        _mod._last_drop_warn_monotonic = float('-inf')
        p1, p2, p3, p4, p5 = (
            Point("p1"), Point("p2"), Point("p3"), Point("p4"), Point("p5"))
        with patch.object(_mod, "MAX_PENDING_POINTS", 3):
            _mod.pending_points.extend([p1, p2, p3])
            with caplog.at_level(logging.WARNING):
                _mod._queue_point(p4)
                _mod._queue_point(p5)
        assert sum("dropped" in r.message.lower() for r in caplog.records) == 1

    def test_tags_every_point_with_vin(self):
        _mod._vin = "1FATESTVIN0000009"
        _mod._queue_point(_mod.Point("rpm").field("value", 1))
        assert _mod.pending_points[0]._tags == {"vin": "1FATESTVIN0000009"}


class TestConnectionHealthy:
    def test_healthy_when_connected_and_data_recent(self):
        conn = MagicMock()
        conn.is_connected.return_value = True
        _mod._last_append_monotonic = _mod.monotonic()
        assert _mod._connection_healthy(conn) is True

    def test_unhealthy_when_disconnected(self):
        conn = MagicMock()
        conn.is_connected.return_value = False
        _mod._last_append_monotonic = _mod.monotonic()
        assert _mod._connection_healthy(conn) is False

    def test_unhealthy_when_data_stale(self):
        """ELM adapter alive but ECU silent (ignition off): no callback has
        queued a point for over STALE_DATA_TIMEOUT_S — treat as dead."""
        conn = MagicMock()
        conn.is_connected.return_value = True
        _mod._last_append_monotonic = _mod.monotonic() - (_mod.STALE_DATA_TIMEOUT_S + 1)
        assert _mod._connection_healthy(conn) is False
