"""Interactive refinement of the race-backfill selection.

refine_races() (added with the Textual app) is the public entry point: it shows
discovered races in a two-pane TUI — search terms on the left, a race checklist
on the right — and returns the confirmed selection. RaceListModel below holds
every piece of state and all merge/dedup logic, with no UI imports, so the
behavior is unit-testable without Textual.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RefineResult:
    """Confirmed outcome of a refinement session.

    races is the checked subset (date-sorted race dicts); terms the final
    search terms in display order; terms_changed whether they differ from the
    terms the session started with.
    """

    races: list
    terms: tuple
    terms_changed: bool


class RaceListModel:
    """State for the refinement UI: active terms, cached results, checked IDs.

    The per-term cache outlives term removal, so re-adding a removed term costs
    no API call. races() derives the visible list on demand: dedup by race ID
    across active terms, filter to StartDateEpoc >= start_epoc, sort by date.
    """

    def __init__(self, terms, races_by_term, start_epoc):
        """Seed with the initial terms and their (already fetched) results."""
        self.start_epoc = start_epoc
        self._initial_terms = tuple(terms)
        self.terms = list(terms)
        self._cache = {term: list(races_by_term.get(term, [])) for term in terms}
        self.checked = {race['ID'] for race in self.races()}

    def races(self):
        """Return visible races: deduped, date-filtered, date-sorted."""
        seen = {}
        for term in self.terms:
            for race in self._cache.get(term, []):
                if race['StartDateEpoc'] >= self.start_epoc:
                    seen[race['ID']] = race
        return sorted(seen.values(), key=lambda r: r['StartDateEpoc'])

    def has_cached(self, term):
        """True if term already has session-cached search results."""
        return term in self._cache

    def add_term(self, term, results=None):
        """Activate a term; newly visible races become checked.

        results is the term's raw search-result list, required unless the term
        is already cached from earlier in the session. Blank and already-active
        terms are ignored.
        """
        term = term.strip()
        if not term or term in self.terms:
            return
        if results is not None:
            self._cache[term] = list(results)
        elif term not in self._cache:
            raise ValueError(f"no cached results for {term!r}; pass results")
        before = {race['ID'] for race in self.races()}
        self.terms.append(term)
        self.checked |= {race['ID'] for race in self.races()} - before

    def remove_term(self, term):
        """Deactivate a term; races matched only by it drop from the list."""
        if term not in self.terms:
            return
        self.terms.remove(term)
        self.checked &= {race['ID'] for race in self.races()}

    def toggle(self, race_id):
        """Flip one race's checked state."""
        if race_id in self.checked:
            self.checked.discard(race_id)
        else:
            self.checked.add(race_id)

    def set_all(self, checked):
        """Check (True) or uncheck (False) every visible race."""
        self.checked = {r['ID'] for r in self.races()} if checked else set()

    def invert(self):
        """Invert the checked state of every visible race."""
        self.checked = {r['ID'] for r in self.races()} - self.checked

    def selected(self):
        """Return the checked races, date-sorted."""
        return [race for race in self.races() if race['ID'] in self.checked]

    @property
    def terms_changed(self):
        """True if the active terms differ from the session's initial terms."""
        return tuple(self.terms) != self._initial_terms
