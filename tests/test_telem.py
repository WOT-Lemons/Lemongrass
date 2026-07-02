from unittest.mock import MagicMock, patch

import lemongrass.telem as _mod


class _Cmd:
    def __init__(self, s, name=None):
        self._s = s
        self.name = name if name is not None else s

    def __str__(self):
        return self._s


def _reset():
    _mod.pending_points.clear()


class TestConnect:
    def test_defaults_to_obd_symlink(self, monkeypatch):
        monkeypatch.delenv("OBD_PORT", raising=False)
        with patch.object(_mod.obd, "Async") as mock_async:
            _mod.connect()
        mock_async.assert_called_once_with(portstr="/dev/obd")

    def test_uses_obd_port_env_override(self, monkeypatch):
        monkeypatch.setenv("OBD_PORT", "/dev/ttyUSB0")
        with patch.object(_mod.obd, "Async") as mock_async:
            _mod.connect()
        mock_async.assert_called_once_with(portstr="/dev/ttyUSB0")


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


class TestNewAirStatus:
    def setup_method(self):
        _reset()

    def test_appends_known_status(self):
        r = MagicMock()
        r.command = _Cmd("b'0112': Secondary Air Status")
        r.value = "Upstream"
        _mod.new_air_status(r)
        assert len(_mod.pending_points) == 1

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
        r.value.MIL = False
        r.value.DTC_count = 0
        _mod.new_status(r)
        assert len(_mod.pending_points) == 2

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
        _mod._connection = None

    def test_no_connection_set_does_nothing(self):
        _mod._fetch_and_store_dtcs()
        assert len(_mod.pending_points) == 0

    def test_null_response_does_nothing(self):
        _mod._connection = MagicMock()
        r = MagicMock()
        r.value = None
        with patch.object(_mod.obd.OBD, "query", return_value=r) as mock_query:
            _mod._fetch_and_store_dtcs()
        mock_query.assert_called_once_with(
            _mod._connection, _mod.obd.commands.GET_DTC, force=True
        )
        assert len(_mod.pending_points) == 0

    def test_writes_joined_codes(self):
        _mod._connection = MagicMock()
        r = MagicMock()
        r.value = [
            ("P0104", "Mass or Volume Air Flow Circuit Intermittent"),
            ("B0003", ""),
        ]
        with patch.object(_mod.obd.OBD, "query", return_value=r):
            _mod._fetch_and_store_dtcs()
        assert len(_mod.pending_points) == 1


class TestNewStatusDtcTrigger:
    def setup_method(self):
        _reset()
        _mod._last_dtc_count = 0
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

    def test_no_trigger_when_count_unchanged(self):
        _mod._last_dtc_count = 1
        r = MagicMock()
        r.command = _Cmd("b'0101': Status since DTCs cleared", name="STATUS")
        r.value.MIL = True
        r.value.DTC_count = 1
        with patch.object(_mod, "_fetch_and_store_dtcs") as mock_fetch:
            _mod.new_status(r)
        mock_fetch.assert_not_called()

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

    def test_writes_and_clears_pending_points(self):
        _mod.pending_points.append("p1")
        write_api = MagicMock()
        _mod.flush_points(write_api)
        write_api.write.assert_called_once()
        assert len(_mod.pending_points) == 0

    def test_does_nothing_when_empty(self):
        write_api = MagicMock()
        _mod.flush_points(write_api)
        write_api.write.assert_not_called()

    def test_restores_points_on_write_failure(self):
        _mod.pending_points.append("p1")
        write_api = MagicMock()
        write_api.write.side_effect = Exception("network error")
        _mod.flush_points(write_api)
        assert len(_mod.pending_points) == 1

    def test_writes_all_pending_points(self):
        _mod.pending_points.extend(["p1", "p2", "p3"])
        write_api = MagicMock()
        _mod.flush_points(write_api)
        write_api.write.assert_called_once()
        assert len(_mod.pending_points) == 0


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
