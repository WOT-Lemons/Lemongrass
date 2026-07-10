"""Interactive refinement of the race-backfill selection.

refine_races() (added with the Textual app) is the public entry point: it shows
discovered races in a two-pane TUI — search terms on the left, a race checklist
on the right — and returns the confirmed selection. RaceListModel below holds
every piece of state and all merge/dedup logic, with no UI imports, so the
behavior is unit-testable without Textual.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from race_monitor import RaceMonitorError
from textual import work
from textual.app import App
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Input, Label, ListItem, ListView, SelectionList
from textual.widgets.selection_list import Selection


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


def _race_label(race):
    """One checklist row: 'YYYY-MM-DD  Name  (#id)'."""
    day = datetime.fromtimestamp(race['StartDateEpoc'], tz=UTC).strftime('%Y-%m-%d')
    return f"{day}  {race['Name']}  (#{race['ID']})"


class BackfillApp(App):
    """Two-pane refinement UI; exit value is a RefineResult, or None on cancel.

    All state changes are delegated to the RaceListModel; widgets are rebuilt
    from the model after every mutation, so the model stays the single source
    of truth.
    """

    CSS = """
    #terms-pane { width: 36; }
    #terms-pane, #races-pane { border: round $primary; padding: 0 1; }
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('enter', 'confirm', 'Confirm', priority=True),
        Binding('a', 'select_all', 'All'),
        Binding('i', 'invert', 'Invert'),
        Binding('d', 'remove_term', 'Remove term'),
        Binding('escape', 'cancel', 'Cancel'),
        Binding('q', 'cancel', 'Cancel', show=False),
        Binding('ctrl+c', 'cancel', 'Cancel', show=False, priority=True),
    ]

    def __init__(self, client, model):
        """client is the shared RaceMonitorClient; model a seeded RaceListModel."""
        super().__init__()
        self.client = client
        self.model = model

    def compose(self):
        """Lay out the terms pane, races checklist, and footer."""
        with Horizontal():
            with Vertical(id='terms-pane'):
                yield Label('Search terms')
                yield ListView(id='terms')
                yield Input(placeholder='add search term…', id='new-term')
            with Vertical(id='races-pane'):
                yield Label(id='count')
                yield SelectionList(id='races')
        yield Footer()

    def on_mount(self):
        """Populate both panes from the model and focus the checklist."""
        self._refresh_all()
        self.query_one('#races', SelectionList).focus()

    def _refresh_all(self):
        """Rebuild both panes from the model."""
        terms_view = self.query_one('#terms', ListView)
        terms_view.clear()
        for term in self.model.terms:
            terms_view.append(ListItem(Label(term)))
        races_view = self.query_one('#races', SelectionList)
        races_view.clear_options()
        for race in self.model.races():
            races_view.add_option(Selection(
                _race_label(race), race['ID'], race['ID'] in self.model.checked))
        self._update_count()

    def _update_count(self):
        """Refresh the 'N of M selected' header above the checklist."""
        total = len(self.model.races())
        self.query_one('#count', Label).update(
            f'Races — {len(self.model.checked)} of {total} selected')

    def on_selection_list_selection_toggled(self, event):
        """Mirror a checkbox toggle into the model."""
        self.model.toggle(event.selection.value)
        self._update_count()

    def action_select_all(self):
        """Check every visible race."""
        self.model.set_all(True)
        self._refresh_all()

    def action_invert(self):
        """Invert every visible race's checked state."""
        self.model.invert()
        self._refresh_all()

    def action_confirm(self):
        """Exit with the confirmed selection.

        enter is a priority binding so it wins over the SelectionList; when the
        term Input is focused it submits the typed term instead of confirming
        (the Input would otherwise swallow the key).
        """
        term_input = self.query_one('#new-term', Input)
        if self.focused is term_input:
            self._submit_term(term_input.value)
            return
        self.exit(RefineResult(races=self.model.selected(),
                               terms=tuple(self.model.terms),
                               terms_changed=self.model.terms_changed))

    def action_cancel(self):
        """Exit without a selection."""
        self.exit(None)

    def action_remove_term(self):
        """Remove the term highlighted in the terms pane."""
        terms_view = self.query_one('#terms', ListView)
        if terms_view.index is None or not self.model.terms:
            return
        self.model.remove_term(self.model.terms[terms_view.index])
        self._refresh_all()

    def on_input_submitted(self, event):
        """Fallback submit path if the priority enter binding is bypassed."""
        self._submit_term(event.value)

    def _submit_term(self, value):
        """Add a term: from the session cache if present, else a live search."""
        term = value.strip()
        self.query_one('#new-term', Input).value = ''
        if not term or term in self.model.terms:
            return
        if self.model.has_cached(term):
            self.model.add_term(term)
            self._refresh_all()
            return
        self.notify(f'searching "{term}"…')
        self._search_term(term)

    @work(thread=True)
    def _search_term(self, term):
        """Fetch a term's results off the UI thread (rate limit may block ~10s)."""
        try:
            resp = self.client.results.search_results(term)
        except RaceMonitorError as exc:
            self.call_from_thread(
                self.notify, f'search "{term}" failed: {exc}', severity='error')
            return
        self.call_from_thread(self._merge_results, term, resp.get('Races', []))

    def _merge_results(self, term, results):
        """Merge a completed term search into the model and rebuild the panes."""
        self.model.add_term(term, results)
        self._refresh_all()


def refine_races(client, terms, races_by_term, start_epoc):
    """Run the refinement app; return a RefineResult, or None if cancelled.

    client is the already-open RaceMonitorClient from the initial search (its
    rate-limiter window is shared with in-app term searches). races_by_term is
    the per-term output of search_races_by_term(); start_epoc filters in-app
    search results the same way --start-date filtered the initial ones.
    """
    model = RaceListModel(terms, races_by_term, start_epoc)
    app = BackfillApp(client, model)
    try:
        return app.run()
    except KeyboardInterrupt:
        return None
