# pylint: disable=missing-module-docstring,missing-class-docstring
# pylint: disable=missing-function-docstring,too-few-public-methods
import importlib.util
import pathlib
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "telem",
    pathlib.Path(__file__).parent.parent / "telem.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class _Cmd:
    def __init__(self, s):
        self._s = s

    def __str__(self):
        return self._s


def _reset():
    _mod.pending_points.clear()


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
        r.command = _Cmd("FUEL_STATUS")
        r.value = ["Closed loop, using oxygen sensor feedback to determine fuel mix"]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 1

    def test_skips_falsy_first_element(self):
        r = MagicMock()
        r.command = _Cmd("FUEL_STATUS")
        r.value = [None]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 0

    def test_appends_for_unknown_status(self):
        r = MagicMock()
        r.command = _Cmd("FUEL_STATUS")
        r.value = ["Unknown mode that maps to nothing"]
        _mod.new_fuel_status(r)
        assert len(_mod.pending_points) == 1


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
