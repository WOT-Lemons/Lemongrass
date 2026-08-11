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


def _candidates(name, aliases, where):
    """Normalized name plus aliases, deduplicated, longest first.

    Longest-first is the whole tie-breaking story: the first candidate that
    matches is the longest one that matches, and two distinct candidates of
    equal length cannot both match the same text (an equal-length prefix is an
    equality).

    A name of nothing but separators (``"---"``, or any name with no ASCII
    alphanumerics at all) normalizes to the empty string, which _match would
    then treat as a candidate that equals the empty track name every failed
    details fetch produces -- silently tagging every unresolved race with that
    venue. _string_list already rejects an empty alias; this rejects the name.
    """
    if not normalize(name):
        raise TrackDataError(
            f"{where} name {name!r} normalizes to nothing; names need at "
            f"least one letter or digit")
    seen = {normalize(name), *(normalize(a) for a in aliases)}
    # Secondary alphabetical key: sorting a set by length alone leaves
    # equal-length candidates in hash order, which varies with PYTHONHASHSEED.
    return tuple(sorted(seen, key=lambda c: (-len(c), c)))


def _list_of_tables(value, where):
    """Return ``value`` as a list, raising TrackDataError if it is not one.

    tomllib parses ``key = 1`` and ``key = [[array]]`` into the same Python
    key with entirely different types; without this check a scalar leaks a
    raw TypeError out of the ``for`` loop below instead of the documented
    TrackDataError.
    """
    if not isinstance(value, list):
        raise TrackDataError(f"{where} must be an array of tables, not "
                              f"{type(value).__name__}")
    return value


def _load_venues(raw_venues):
    """Build the validated venue tuple."""
    venues, venue_ids = [], set()
    # Maps a normalized candidate to the venue id that claimed it, so a
    # collision names both colliding venues and the shared candidate rather
    # than just the one being loaded.
    venue_candidate_owners = {}
    for raw in raw_venues:
        _reject_unknown(raw, {"id", "name", "aliases", "layout"}, "[[venue]]")
        venue_id = _text(raw, "id", "[[venue]]")
        if venue_id in venue_ids:
            raise TrackDataError(f"duplicate venue id: {venue_id!r}")
        venue_ids.add(venue_id)
        where = f"[[venue]] {venue_id}"
        name = _text(raw, "name", where)
        aliases = _string_list(raw, "aliases", where)
        venue_candidates = _candidates(name, aliases, where)
        for candidate in venue_candidates:
            owner = venue_candidate_owners.get(candidate)
            if owner is not None:
                raise TrackDataError(
                    f"venue {owner!r} and venue {venue_id!r} both normalize "
                    f"to {candidate!r}; venue candidates must be unique")
            venue_candidate_owners[candidate] = venue_id
        layouts, layout_ids = [], set()
        layout_candidate_owners = {}
        for raw_layout in _list_of_tables(raw.get("layout", []), f"{where} layout"):
            _reject_unknown(raw_layout, {"id", "name", "aliases"},
                             f"{where} [[venue.layout]]")
            layout_id = _text(raw_layout, "id", f"{where} [[venue.layout]]")
            if layout_id in layout_ids:
                raise TrackDataError(
                    f"duplicate layout id {layout_id!r} in venue {venue_id!r}")
            layout_ids.add(layout_id)
            lwhere = f"{where} layout {layout_id}"
            layout_name = _text(raw_layout, "name", lwhere)
            layout_candidates = _candidates(
                layout_name, _string_list(raw_layout, "aliases", lwhere), lwhere)
            for candidate in layout_candidates:
                owner = layout_candidate_owners.get(candidate)
                if owner is not None:
                    raise TrackDataError(
                        f"layout {owner!r} and layout {layout_id!r} in venue "
                        f"{venue_id!r} both normalize to {candidate!r}; layout "
                        f"candidates must be unique within a venue")
                layout_candidate_owners[candidate] = layout_id
            layouts.append(Layout(
                layout_id=layout_id, name=layout_name, candidates=layout_candidates))
        venues.append(Venue(venue_id=venue_id, name=name,
                             candidates=venue_candidates,
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
        for raw_event in _list_of_tables(raw.get("event", []),
                                          f"[[series]] {series_id} event"):
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
    return TrackData(
        venues=_load_venues(_list_of_tables(doc.get("venue", []), "[[venue]]")),
        series=_load_series(_list_of_tables(doc.get("series", []), "[[series]]")))


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


def _match(text, candidates):
    """Return (matched candidate, remainder) for the first candidate that fits.

    Candidates arrive longest-first, so the first fit is the longest fit. The
    test is equality or a whole-word prefix — never a bare ``startswith``,
    which would match "Nola" inside "Nolan Speedway", and never an empty
    candidate, which the loader already rejects.
    """
    for candidate in candidates:
        if text == candidate:
            return candidate, ""
        if text.startswith(candidate + " "):
            return candidate, text[len(candidate) + 1:]
    return None, text


def _best_match(text, entries):
    """Return (entry, remainder) for the entry with the longest matching candidate.

    ``entries`` is any sequence of objects carrying a ``candidates`` tuple —
    venues or one venue's layouts. Longest-first inside a single entry is
    ``_match``'s job; this picks between entries. The comparison is strictly
    ``>``, so an exact-length tie keeps the first entry in file order, which is
    the only tie-break the file's own uniqueness rules leave possible.

    Returns (None, text) when nothing matched, so the caller can keep threading
    the untouched text through.
    """
    best = None
    for entry in entries:
        candidate, remainder = _match(text, entry.candidates)
        if candidate is not None and (best is None or len(candidate) > best[0]):
            best = (len(candidate), entry, remainder)
    return (best[1], best[2]) if best is not None else (None, text)


def _match_event(race_name, series, series_id):
    """Return the event id whose keyword appears in the race name, or None.

    An integer series_id selects only that series; None searches every series,
    which is what the back catalogue needs — Influx never stored series_id, so
    every legacy race has it NULL, and read_legacy_races leaves it NULL
    permanently rather than re-fetching it.

    A race name matching more than one event resolves to None rather than to
    whichever event the file lists first, whether the rivals sit in two series
    or side by side in one. Keyword overlap cannot be rejected at load time —
    two events with unrelated keywords still collide on a race name that
    happens to contain both — so ambiguity is decided here, per name. "First
    in file order" would silently tag ~184 legacy races with an event they
    never ran, and a wrong event id is worse than an unresolved one:
    identify_races reports unresolved names as a worklist, but reports a wrong
    tag as a perfectly ordinary change.
    """
    if not race_name:
        return None
    matches = set()
    for entry in series:
        if series_id is not None and entry.series_id != series_id:
            continue
        for event in entry.events:
            if any(keyword in race_name for keyword in event.keywords):
                matches.add(event.event_id)
    return matches.pop() if len(matches) == 1 else None


def resolve(track_name, race_name, series_id):
    """Resolve free-text race metadata to a TrackIdentity of three nullable ids.

    Venue first, then that venue's layouts against whatever text the venue
    match left over, then the event by keyword. A venue that does not match
    stops resolution: layouts are keyed (venue_id, layout_id), so a layout
    without its venue is a foreign key violation waiting to happen, and an
    event without a venue is of no use to a year-over-year comparison.
    """
    track_data = data()
    venue, remainder = _best_match(normalize(track_name), track_data.venues)
    if venue is None:
        return TrackIdentity()
    layout = _best_match(remainder, venue.layouts)[0] if remainder else None
    return TrackIdentity(
        venue_id=venue.venue_id,
        layout_id=layout.layout_id if layout is not None else None,
        event_id=_match_event(normalize(race_name), track_data.series, series_id),
    )
