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


@pytest.mark.parametrize("track_name,venue_id,layout_id", [
    ("Thompson Motor Speedway", "thompson", None),
    ("Thompson Speedway Motorsports Park", "thompson", None),
    ("Thompson Raceway Motorsports Park", "thompson", None),
    ("Gingerman", "gingerman", None),
    ("Gingerman Raceway", "gingerman", None),
    ("The Ridge", "the-ridge", None),
    ("The Ridge Motorsports Park", "the-ridge", None),
    ("Nola", "nola", None),
    ("NOLA Motorsports Park", "nola", None),
    ("Road Atlanta", "road-atlanta", None),
    ("Road Atlanta, Braselton GA", "road-atlanta", None),
    ("Sonoma Raceway", "sonoma", None),
    ("Sonoma Raceway, Sonoma CA", "sonoma", None),
    ("High Plains Raceway", "high-plains", None),
    ("High Plains Raceway - Deer Trail, CO", "high-plains", None),
    ("Autobahn Country Club", "autobahn", None),
    ("Autobahn Country Club - Joliet, IL", "autobahn", None),
    ("New Jersey Motorsports Park", "njmp", None),
    ("New Jersey Motorsports Park - Thunderbolt Course", "njmp", "thunderbolt"),
    ("New Jersey Motorsports Park - Lightning Course", "njmp", "lightning"),
])
def test_resolve_maps_every_known_spelling(track_name, venue_id, layout_id):
    got = _tracks.resolve(track_name, "", None)
    assert (got.venue_id, got.layout_id) == (venue_id, layout_id)


def test_unmapped_track_resolves_to_nothing():
    got = _tracks.resolve("Some Unknown Kart Track", "GP du Lac 2024", 145)
    assert got == _tracks.TrackIdentity()


def test_empty_input_is_safe():
    # _resolve_race_metadata returns track_name='' on a failed details fetch.
    assert _tracks.resolve("", "", None) == _tracks.TrackIdentity()
    assert _tracks.resolve("   ", "", 145) == _tracks.TrackIdentity()


def test_longest_venue_candidate_wins(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "short"
name = "Summit Point"

[[venue]]
id = "long"
name = "Summit Point Raceway"
""")
    _tracks._DATA = _tracks.load(path)
    assert _tracks.resolve("Summit Point Raceway", "", None).venue_id == "long"
    assert _tracks.resolve("Summit Point", "", None).venue_id == "short"


def test_layout_does_not_cross_match_between_venues(tmp_path):
    # The regression test for the flat-layout-namespace defect: a layout owned
    # by one venue must never be returned for another, because (venue_id,
    # layout_id) is a composite foreign key and the mismatched tuple would make
    # store_race fail mid-race.
    path = _write(tmp_path, """
[[venue]]
id = "other"
name = "Other Park"

  [[venue.layout]]
  id = "full"
  name = "Full Course"

[[venue]]
id = "atlanta"
name = "Road Atlanta"
""")
    _tracks._DATA = _tracks.load(path)
    got = _tracks.resolve("Road Atlanta - Full Course", "", None)
    assert (got.venue_id, got.layout_id) == ("atlanta", None)


def test_bare_venue_yields_no_layout():
    got = _tracks.resolve("Thompson Speedway Motorsports Park", "", None)
    assert got.layout_id is None


@pytest.mark.parametrize("race_name", [
    "GP du Lac Chargoggagoggmanchauggagoggchaubunagungamaugg",
    "GP du Lac 2023",
    "The GP du Lac",
    "Lemons Chargoggagogg 24",
])
def test_event_matches_every_race_name_spelling(race_name):
    got = _tracks.resolve("Thompson Motor Speedway", race_name, 145)
    assert got.event_id == "gp-du-lac"


def test_event_lookup_is_scoped_to_the_named_series():
    got = _tracks.resolve("Thompson Motor Speedway", "GP du Lac 2023", 999)
    assert got.event_id is None


def test_event_lookup_with_no_series_searches_every_series():
    # Influx never stored series_id, so every db import-legacy race has it NULL.
    got = _tracks.resolve("Thompson Motor Speedway", "GP du Lac 2023", None)
    assert got.event_id == "gp-du-lac"


def test_event_is_not_resolved_when_the_venue_is_not():
    got = _tracks.resolve("Nowhere Speedway", "GP du Lac 2023", 145)
    assert got.event_id is None
