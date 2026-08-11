import pytest

from lemongrass import _tracks


@pytest.fixture(autouse=True)
def _clear_cache():
    _tracks.reset_cache()
    yield
    _tracks.reset_cache()


def _write(tmp_path, text):
    p = tmp_path / "tracks.toml"
    p.write_text(text, encoding="utf-8")
    return p


def test_normalize_collapses_separators_and_casefolds():
    assert _tracks.normalize("Road Atlanta, Braselton GA") == "road atlanta braselton ga"
    assert _tracks.normalize("High Plains Raceway - Deer Trail, CO") == (
        "high plains raceway deer trail co")
    assert _tracks.normalize("") == ""
    assert _tracks.normalize(None) == ""


def test_shipped_file_loads_and_has_the_known_venues():
    data = _tracks.data()
    ids = {v.venue_id for v in data.venues}
    assert {"thompson", "gingerman", "the-ridge", "nola", "road-atlanta",
            "sonoma", "high-plains", "autobahn", "njmp"} <= ids
    njmp = next(v for v in data.venues if v.venue_id == "njmp")
    assert {lay.layout_id for lay in njmp.layouts} == {"thunderbolt", "lightning"}
    thompson = next(v for v in data.venues if v.venue_id == "thompson")
    # A venue with one unnamed course declares no layouts at all.
    assert thompson.layouts == ()


def test_shipped_file_is_readable_as_package_data():
    # uv-build copies the module directory into the wheel; a future exclude
    # could silently drop this file, and every resolve() would then raise.
    from importlib.resources import files
    assert (files("lemongrass.data") / "tracks.toml").is_file()


def test_candidates_are_normalized_and_longest_first():
    data = _tracks.data()
    thompson = next(v for v in data.venues if v.venue_id == "thompson")
    assert thompson.candidates == tuple(
        sorted(thompson.candidates, key=len, reverse=True))
    assert "thompson motor speedway" in thompson.candidates
    assert all(c == _tracks.normalize(c) for c in thompson.candidates)


def test_duplicate_venue_id_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"

[[venue]]
id = "a"
name = "B"
""")
    with pytest.raises(_tracks.TrackDataError, match="duplicate venue id"):
        _tracks.load(path)


def test_duplicate_layout_id_within_a_venue_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"

  [[venue.layout]]
  id = "x"
  name = "X"

  [[venue.layout]]
  id = "x"
  name = "Y"
""")
    with pytest.raises(_tracks.TrackDataError, match="duplicate layout id"):
        _tracks.load(path)


def test_duplicate_event_id_across_series_is_rejected(tmp_path):
    # event_id is a single-column primary key, so uniqueness is global.
    path = _write(tmp_path, """
[[series]]
id = 145

  [[series.event]]
  id = "e"
  name = "E"
  keywords = ["e"]

[[series]]
id = 184

  [[series.event]]
  id = "e"
  name = "E2"
  keywords = ["f"]
""")
    with pytest.raises(_tracks.TrackDataError, match="duplicate event id"):
        _tracks.load(path)


def test_duplicate_event_id_within_one_series_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[series]]
id = 145

  [[series.event]]
  id = "e"
  name = "E"
  keywords = ["e"]

  [[series.event]]
  id = "e"
  name = "E2"
  keywords = ["f"]
""")
    with pytest.raises(_tracks.TrackDataError, match="duplicate event id"):
        _tracks.load(path)


def test_event_with_no_keywords_is_rejected(tmp_path):
    # It would load silently and never match anything — a curation mistake the
    # "fail loudly at load" contract exists to catch.
    path = _write(tmp_path, """
[[series]]
id = 145

  [[series.event]]
  id = "e"
  name = "E"
""")
    with pytest.raises(_tracks.TrackDataError, match="keyword"):
        _tracks.load(path)


def test_empty_alias_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"
aliases = ["  "]
""")
    with pytest.raises(_tracks.TrackDataError, match="empty"):
        _tracks.load(path)


def test_empty_keyword_is_rejected(tmp_path):
    # An empty keyword is a substring of every race name.
    path = _write(tmp_path, """
[[series]]
id = 145

  [[series.event]]
  id = "e"
  name = "E"
  keywords = [""]
""")
    with pytest.raises(_tracks.TrackDataError, match="empty"):
        _tracks.load(path)


def test_unknown_key_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"
colour = "blue"
""")
    with pytest.raises(_tracks.TrackDataError, match="unknown key"):
        _tracks.load(path)


def test_missing_name_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
""")
    with pytest.raises(_tracks.TrackDataError, match="name"):
        _tracks.load(path)


def test_data_is_cached(tmp_path):
    first = _tracks.data()
    assert _tracks.data() is first
    _tracks.reset_cache()
    assert _tracks.data() is not first
