import pytest

from lemongrass import _config


class TestParseSize:
    @pytest.mark.parametrize("value,expected", [
        (1073741824, 1073741824),      # int bytes passthrough
        ("1024", 1024),                # bare number = bytes
        ("512B", 512),                 # B suffix = bytes
        ("1GiB", 1073741824),          # binary
        ("1 gib", 1073741824),         # whitespace + lowercase
        ("500MiB", 524288000),         # binary
        ("1GB", 1000000000),           # decimal
        ("1.5GB", 1500000000),         # fraction
        (2.0, 2),                      # float floored to int
    ])
    def test_parses_valid(self, value, expected):
        assert _config.parse_size(value) == expected

    @pytest.mark.parametrize("value", ["1XB", "-1GiB", "GiB", "", 0, -5, "0GiB", True])
    def test_rejects_invalid(self, value):
        with pytest.raises(ValueError):
            _config.parse_size(value)
