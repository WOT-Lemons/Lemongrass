"""Curated track identity: the shipped tracks.toml, its loader, and the resolver.

A leaf module — it imports nothing else from lemongrass and knows nothing about
storage, so ``_db`` may import it without a cycle. ``resolve`` turns
RaceMonitor's free-text ``Track`` and race ``Name`` into ids;
``_db.sync_tracks`` is what puts rows with those ids in the database.

The file ships inside the package, so a validation failure here is a packaging
or authoring bug, not bad user input — hence the loud exception rather than a
warning and a fallback.
"""
import re
import tomllib
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

# Everything that is not a lowercase letter or a digit separates words. Applied
# after casefold, so the class does not need an upper-case range.
_SEPARATORS = re.compile(r"[^0-9a-z]+")


class TrackDataError(Exception):
    """Raised when tracks.toml is malformed, duplicated, or empty where it must not be."""


def normalize(text):
    """Casefold, collapse runs of non-alphanumerics to single spaces, strip.

    After this there are no separators left except single spaces, which is what
    makes the prefix test in ``resolve`` a plain string comparison.
    """
    return _SEPARATORS.sub(" ", (text or "").casefold()).strip()


@dataclass(frozen=True)
class Layout:
    """One named course at a venue, with its match candidates."""

    layout_id: str
    name: str
    candidates: tuple


@dataclass(frozen=True)
class Venue:
    """One venue, its layouts (possibly none), and its match candidates."""

    venue_id: str
    name: str
    candidates: tuple
    layouts: tuple


@dataclass(frozen=True)
class Event:
    """One recurring event, matched by keyword against a race name."""

    event_id: str
    series_id: int
    name: str
    keywords: tuple


@dataclass(frozen=True)
class Series:
    """One RaceMonitor series and the events curated under it.

    Carries no name: the API already populates ``races.series_name``, and a
    second name here would have nothing keeping the two in agreement.
    """

    series_id: int
    events: tuple


@dataclass(frozen=True)
class TrackData:
    """The whole curated file, validated and normalized."""

    venues: tuple
    series: tuple


@dataclass(frozen=True)
class TrackIdentity:
    """What ``resolve`` returns: three nullable ids, no names.

    Names live in the tables and are reachable by join, so nothing here can
    drift out of step with what the database says.
    """

    venue_id: str | None = None
    layout_id: str | None = None
    event_id: str | None = None


def _reject_unknown(table, allowed, where):
    """Raise TrackDataError if ``table`` is not a table or has unexpected keys."""
    if not isinstance(table, dict):
        raise TrackDataError(f"{where} must be a table, not {type(table).__name__}")
    extra = set(table) - allowed
    if extra:
        raise TrackDataError(f"unknown key(s) in {where}: {', '.join(sorted(extra))}")


def _text(table, key, where):
    """Return a required non-empty string value."""
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TrackDataError(f"{where} needs a non-empty string {key}")
    return value


def _string_list(table, key, where):
    """Return an optional list-of-strings value, rejecting empty entries.

    Absent means absent: under tomllib a venue with no aliases has no key at
    all rather than an empty list.
    """
    value = table.get(key, [])
    if not (isinstance(value, list) and all(isinstance(x, str) for x in value)):
        raise TrackDataError(f"{where}.{key} must be a list of strings")
    for item in value:
        if not normalize(item):
            raise TrackDataError(f"{where}.{key} contains an empty entry: {item!r}")
    return value


def _candidates(name, aliases):
    """Normalized name plus aliases, deduplicated, longest first.

    Longest-first is the whole tie-breaking story: the first candidate that
    matches is the longest one that matches, and two distinct candidates of
    equal length cannot both match the same text (an equal-length prefix is an
    equality).
    """
    seen = {normalize(name), *(normalize(a) for a in aliases)}
    # Secondary alphabetical key: sorting a set by length alone leaves
    # equal-length candidates in hash order, which varies with PYTHONHASHSEED.
    return tuple(sorted(seen, key=lambda c: (-len(c), c)))


def _load_venues(raw_venues):
    """Build the validated venue tuple."""
    venues, venue_ids = [], set()
    for raw in raw_venues:
        _reject_unknown(raw, {"id", "name", "aliases", "layout"}, "[[venue]]")
        venue_id = _text(raw, "id", "[[venue]]")
        if venue_id in venue_ids:
            raise TrackDataError(f"duplicate venue id: {venue_id!r}")
        venue_ids.add(venue_id)
        where = f"[[venue]] {venue_id}"
        name = _text(raw, "name", where)
        aliases = _string_list(raw, "aliases", where)
        layouts, layout_ids = [], set()
        for raw_layout in raw.get("layout", []):
            _reject_unknown(raw_layout, {"id", "name", "aliases"},
                             f"{where} [[venue.layout]]")
            layout_id = _text(raw_layout, "id", f"{where} [[venue.layout]]")
            if layout_id in layout_ids:
                raise TrackDataError(
                    f"duplicate layout id {layout_id!r} in venue {venue_id!r}")
            layout_ids.add(layout_id)
            lwhere = f"{where} layout {layout_id}"
            layout_name = _text(raw_layout, "name", lwhere)
            layouts.append(Layout(
                layout_id=layout_id, name=layout_name,
                candidates=_candidates(
                    layout_name, _string_list(raw_layout, "aliases", lwhere))))
        venues.append(Venue(venue_id=venue_id, name=name,
                             candidates=_candidates(name, aliases),
                             layouts=tuple(layouts)))
    return tuple(venues)


def _load_series(raw_series):
    """Build the validated series tuple, enforcing globally unique event ids."""
    series, series_ids, event_ids = [], set(), set()
    for raw in raw_series:
        _reject_unknown(raw, {"id", "event"}, "[[series]]")
        series_id = raw.get("id")
        if isinstance(series_id, bool) or not isinstance(series_id, int):
            raise TrackDataError("[[series]] needs an integer id")
        if series_id in series_ids:
            raise TrackDataError(f"duplicate series id: {series_id}")
        series_ids.add(series_id)
        events = []
        for raw_event in raw.get("event", []):
            where = f"[[series.event]] in series {series_id}"
            _reject_unknown(raw_event, {"id", "name", "keywords"}, where)
            event_id = _text(raw_event, "id", where)
            if event_id in event_ids:
                raise TrackDataError(f"duplicate event id: {event_id!r}")
            event_ids.add(event_id)
            keywords = _string_list(raw_event, "keywords", f"{where} {event_id}")
            if not keywords:
                raise TrackDataError(
                    f"event {event_id!r} needs at least one keyword; without "
                    f"one it can never match a race name")
            events.append(Event(
                event_id=event_id, series_id=series_id,
                name=_text(raw_event, "name", f"{where} {event_id}"),
                keywords=tuple(normalize(k) for k in keywords)))
        series.append(Series(series_id=series_id, events=tuple(events)))
    return tuple(series)


def load(path=None):
    """Parse and validate the track data. ``path`` overrides the shipped file."""
    if path is None:
        raw_bytes = (files("lemongrass.data") / "tracks.toml").read_bytes()
    else:
        raw_bytes = Path(path).read_bytes()
    try:
        doc = tomllib.loads(raw_bytes.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as e:
        raise TrackDataError(f"tracks.toml is not valid TOML: {e}") from e
    _reject_unknown(doc, {"venue", "series"}, "top level")
    return TrackData(venues=_load_venues(doc.get("venue", [])),
                      series=_load_series(doc.get("series", [])))


_DATA = None


def data():
    """Return the shipped track data, loading and caching it on first use."""
    global _DATA
    if _DATA is None:
        _DATA = load()
    return _DATA


def reset_cache():
    """Forget the cached track data (tests)."""
    global _DATA
    _DATA = None
