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
            "sonoma", "high-plains", "autobahn", "njmp", "pittrace"} <= ids
    njmp = next(v for v in data.venues if v.venue_id == "njmp")
    assert {lay.layout_id for lay in njmp.layouts} == {"thunderbolt", "lightning"}
    thompson = next(v for v in data.venues if v.venue_id == "thompson")
    # A venue with one unnamed course declares no layouts at all.
    assert thompson.layouts == ()
    # Events are a naming convention, not a place, and are deliberately not
    # curated in the shipped file; race.event_id stays NULL until they have a
    # better home.
    assert data.series == ()


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


def test_venue_name_that_normalizes_to_nothing_is_rejected(tmp_path):
    # Otherwise the venue's only candidate is '', which equals the empty track
    # name a failed details fetch produces -- tagging every unresolved race
    # with this venue.
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "---"
""")
    with pytest.raises(_tracks.TrackDataError, match="normalizes to nothing"):
        _tracks.load(path)


def test_layout_name_that_normalizes_to_nothing_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"

  [[venue.layout]]
  id = "l"
  name = "!!!"
""")
    with pytest.raises(_tracks.TrackDataError, match="normalizes to nothing"):
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


def test_scalar_venue_section_is_rejected(tmp_path):
    path = _write(tmp_path, "venue = 1\n")
    with pytest.raises(_tracks.TrackDataError, match="venue"):
        _tracks.load(path)


def test_scalar_venue_layout_section_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"
layout = 1
""")
    with pytest.raises(_tracks.TrackDataError, match="layout"):
        _tracks.load(path)


def test_scalar_series_section_is_rejected(tmp_path):
    path = _write(tmp_path, "series = 1\n")
    with pytest.raises(_tracks.TrackDataError, match="series"):
        _tracks.load(path)


def test_scalar_series_event_section_is_rejected(tmp_path):
    path = _write(tmp_path, """
[[series]]
id = 145
event = 1
""")
    with pytest.raises(_tracks.TrackDataError, match="event"):
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
    ("Thompson Raceway", "thompson", None),
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
    ("NJMP Thunderbolt", "njmp", "thunderbolt"),
    ("Thunderbolt", "njmp", "thunderbolt"),
    ("Thunderbolt Course", "njmp", "thunderbolt"),
    ("NJMP Lightning", "njmp", "lightning"),
    ("Pittsburgh International Race Complex", "pittrace", None),
    ("Pittsburgh Int'l Race Complex", "pittrace", None),
    ("Pittsburgh International Raceway", "pittrace", None),
])
def test_resolve_maps_every_known_spelling(track_name, venue_id, layout_id):
    got = _tracks.resolve(track_name, "", None)
    assert (got.venue_id, got.layout_id) == (venue_id, layout_id)


def test_a_null_series_id_matching_two_series_resolves_to_nothing(tmp_path):
    # Legacy races have series_id NULL permanently (read_legacy_races never
    # re-fetches it). Taking the first match in file order would tag them with
    # an event they never ran, and identify_races reports that as an ordinary
    # change rather than as a gap. Unresolved is the honest answer.
    path = _write(tmp_path, """
[[series]]
id = 145

  [[series.event]]
  id = "lemons-spring"
  name = "Spring"
  keywords = ["spring classic"]

[[series]]
id = 900

  [[series.event]]
  id = "other-spring"
  name = "Spring"
  keywords = ["spring classic"]
""")
    data = _tracks.load(path)
    assert _tracks._match_event("spring classic", data.series, None) is None
    # Naming the series still resolves it.
    assert _tracks._match_event(
        "spring classic", data.series, 145) == "lemons-spring"


def test_two_events_in_one_series_matching_one_name_resolve_to_nothing(tmp_path):
    # Naming the series narrows the search but does not disambiguate within
    # it, so the same "unresolved beats wrong" rule has to apply inside a
    # single series.
    path = _write(tmp_path, """
[[series]]
id = 145

  [[series.event]]
  id = "lemons-spring"
  name = "Spring"
  keywords = ["spring classic"]

  [[series.event]]
  id = "lemons-spring-enduro"
  name = "Spring Enduro"
  keywords = ["classic"]
""")
    data = _tracks.load(path)
    assert _tracks._match_event("spring classic", data.series, 145) is None
    assert _tracks._match_event("spring classic", data.series, None) is None
    # An unambiguous name still resolves.
    assert _tracks._match_event("the classic", data.series, 145) == (
        "lemons-spring-enduro")


def test_a_candidate_must_end_on_a_word_boundary():
    # "Nola" is a real curated candidate and a bare character prefix of
    # "Nolan Speedway"; matching it there would tag a different track.
    got = _tracks.resolve("Nolan Speedway", "", None)
    assert got.venue_id is None


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


def test_colliding_venue_aliases_are_rejected(tmp_path):
    # Two venues whose normalized candidates tie in length: _best_match's
    # strict "longer than best" comparison would silently let file order pick
    # a winner, and every race at either venue gets tagged with one venue_id
    # forever, with no error to notice it. This must be rejected at load time.
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "Park Raceway"

[[venue]]
id = "b"
name = "Park"
aliases = ["Park Raceway"]
""")
    with pytest.raises(_tracks.TrackDataError, match="park raceway"):
        _tracks.load(path)


def test_colliding_layout_candidates_within_one_venue_are_rejected(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"

  [[venue.layout]]
  id = "full"
  name = "Full Course"

  [[venue.layout]]
  id = "long"
  name = "Long"
  aliases = ["Full Course"]
""")
    with pytest.raises(_tracks.TrackDataError, match="full course"):
        _tracks.load(path)


def test_same_layout_candidate_under_two_different_venues_is_accepted(tmp_path):
    path = _write(tmp_path, """
[[venue]]
id = "a"
name = "A"

  [[venue.layout]]
  id = "full"
  name = "Full Course"

[[venue]]
id = "b"
name = "B"

  [[venue.layout]]
  id = "full"
  name = "Full Course"
""")
    data = _tracks.load(path)
    assert {v.venue_id for v in data.venues} == {"a", "b"}


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


def test_event_is_not_resolved_when_the_venue_is_not():
    got = _tracks.resolve("Nowhere Speedway", "GP du Lac 2023", 145)
    assert got.event_id is None
