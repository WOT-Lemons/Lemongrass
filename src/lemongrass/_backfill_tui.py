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
from textual.screen import ModalScreen
from textual.widgets import Footer, Input, Label, ListItem, ListView, SelectionList
from textual.widgets.selection_list import Selection
from textual.worker import get_current_worker


@dataclass(frozen=True)
class RefineResult:
    """Confirmed outcome of a refinement session.

    races is the checked subset (date-sorted race dicts); terms the final
    search terms in display order; terms_changed whether they differ from the
    terms the session started with. series_id is the pinned series (0 if
    none); series_changed whether it differs from the config-seeded one.
    """

    races: list
    terms: tuple
    terms_changed: bool
    series_id: int = 0
    series_changed: bool = False


# Cache key for the pinned-series source. An object sentinel, not a string:
# it can never collide with a user-typed search term.
_SERIES_KEY = object()


class RaceListModel:
    """State for the refinement UI: sources, cached results, checked IDs.

    Sources are the pinned series (at most one, keyed by the _SERIES_KEY
    sentinel) plus the active search terms. The per-source cache outlives
    removal, so re-adding a removed term costs no API call. races() derives
    the visible list on demand: dedup by race ID across active sources,
    filter to StartDateEpoc >= start_epoc, sort by date.
    """

    def __init__(self, terms, races_by_term, start_epoc, series=None):
        """Seed with initial terms/results and an optional pinned series.

        series is (series_id, series_name, races) from the config-driven
        enumeration, or None when no series is configured.
        """
        self.start_epoc = start_epoc
        self._initial_terms = tuple(terms)
        self.terms = list(terms)
        self._cache = {term: list(races_by_term.get(term, [])) for term in terms}
        self._series = None
        self._initial_series_id = series[0] if series else 0
        if series:
            series_id, series_name, races = series
            self._series = (series_id, series_name)
            self._cache[_SERIES_KEY] = list(races)
        self.checked = {race['ID'] for race in self.races()}

    def _sources(self):
        """Active source keys: the series sentinel (if pinned) plus terms."""
        return ([_SERIES_KEY] if self._series else []) + self.terms

    def races(self):
        """Return visible races: deduped, date-filtered, date-sorted."""
        seen = {}
        for source in self._sources():
            for race in self._cache.get(source, []):
                if race['StartDateEpoc'] >= self.start_epoc:
                    seen[race['ID']] = race
        return sorted(seen.values(), key=lambda r: r['StartDateEpoc'])

    def set_series(self, series_id, series_name, races):
        """Pin (or replace) the series source; newly visible races become
        checked, races matched only by a previous series drop out."""
        before = {race['ID'] for race in self.races()}
        self._series = (series_id, series_name)
        self._cache[_SERIES_KEY] = list(races)
        visible = {race['ID'] for race in self.races()}
        self.checked = (self.checked & visible) | (visible - before)

    def cache_results(self, term, results):
        """Session-cache a term's raw results without activating the term
        (used by the series modal so its searches double as term cache)."""
        self._cache.setdefault(term, list(results))

    @property
    def series(self):
        """(series_id, series_name, visible_count), or None if unpinned."""
        if self._series is None:
            return None
        series_id, series_name = self._series
        count = sum(1 for r in self._cache.get(_SERIES_KEY, [])
                    if r['StartDateEpoc'] >= self.start_epoc)
        return (series_id, series_name, count)

    @property
    def series_id(self):
        """The pinned series' ID, or 0 when no series is pinned."""
        return self._series[0] if self._series else 0

    @property
    def series_changed(self):
        """True if the pinned series differs from the config-seeded one."""
        return self.series_id != self._initial_series_id

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


class SeriesSearchModal(ModalScreen):
    """Find a series: search race names, pick a race, pin its series.

    Flow: query → results.search_results (hits listed) → pick a hit →
    race.details reveals its SeriesID → enumerate_series fetches the full
    series. Dismisses with (series_id, series_name, races), or None on
    escape. Every RaceMonitor call runs off-thread (each may block ~10s
    under the shared rate limiter). Errors notify and leave the modal open
    for retry. Search hits are session-cached in the model's term cache, so
    a later term search for the same string is free.
    """

    CSS = """
    SeriesSearchModal { align: center middle; }
    #series-modal { width: 72; height: 20; border: round $primary; padding: 0 1; }
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'cancel', 'Cancel', priority=True),
    ]

    def __init__(self, client, model):
        """client is the shared RaceMonitorClient; model the app's RaceListModel
        (for start_epoc filtering and search-result caching)."""
        super().__init__()
        self.client = client
        self.model = model
        self._hits = []

    def compose(self):
        """Lay out the query input, hit list, and status line."""
        with Vertical(id='series-modal'):
            yield Label('Find series — search by race name, pick a race')
            yield Input(placeholder='race name…', id='series-query')
            yield ListView(id='series-hits')
            yield Label('', id='series-status')

    def on_mount(self):
        """Focus the query input."""
        self.query_one('#series-query', Input).focus()

    def action_cancel(self):
        """Dismiss without pinning."""
        self.dismiss(None)

    def _status(self, text):
        """Update the status line."""
        self.query_one('#series-status', Label).update(text)

    def on_input_submitted(self, event):
        """Kick off an off-thread race-name search.

        Stops the event so it doesn't bubble past this modal to
        BackfillApp.on_input_submitted (its '#new-term' fallback path), which
        would otherwise also fire for the same keypress.
        """
        event.stop()
        term = event.value.strip()
        if not term:
            return
        self._status(f'searching "{term}"…')
        self._search(term)

    @work(thread=True)
    def _search(self, term):
        """Fetch search hits off the UI thread; report errors via notify."""
        worker = get_current_worker()
        try:
            resp = self.client.results.search_results(term)
        except RaceMonitorError as exc:
            if worker.is_cancelled:
                return
            self.app.call_from_thread(
                self._fail, f'search "{term}" failed: {exc}')
            return
        if worker.is_cancelled:
            return
        self.app.call_from_thread(self._show_hits, term, resp.get('Races', []))

    def _fail(self, message):
        """Clear the status line and surface an error notification."""
        self._status('')
        self.app.notify(message, severity='error')

    def _show_hits(self, term, races):
        """Render search hits and cache them for the term pane."""
        self.model.cache_results(term, races)
        self._hits = list(races)
        view = self.query_one('#series-hits', ListView)
        view.clear()
        for race in self._hits:
            view.append(ListItem(Label(_race_label(race))))
        self._status(f'{len(self._hits)} races — pick one to pin its series'
                     if self._hits else 'no races found')

    def on_list_view_selected(self, event):
        """Resolve the picked race's series off-thread."""
        index = self.query_one('#series-hits', ListView).index
        if index is None or not self._hits:
            return
        race = self._hits[index]
        self._status(f'resolving series for "{race["Name"]}"…')
        self._resolve(race['ID'])

    @work(thread=True)
    def _resolve(self, race_id):
        """race.details → SeriesID → enumerate_series; dismiss with the pin.

        Two-plus rate-limited calls, so runs off the UI thread. A KeyError
        covers a malformed details payload (Beta-adjacent defensiveness).
        """
        # Imported here, not at module level: race_backfill imports this
        # module lazily inside main(), so a top-level import back into
        # race_backfill would be circular.
        from lemongrass.race_backfill import enumerate_series
        worker = get_current_worker()
        try:
            details = self.client.race.details(race_id)
            series_id = details['Race']['SeriesID']
            races = enumerate_series(self.client, series_id, self.model.start_epoc)
        except (KeyError, RaceMonitorError) as exc:
            if worker.is_cancelled:
                return
            self.app.call_from_thread(
                self._fail, f'series lookup failed: {exc!r}')
            return
        if worker.is_cancelled:
            return
        name = races[0]['SeriesName'] if races else f'series {series_id}'
        self.app.call_from_thread(self.dismiss, (series_id, name, races))


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
        Binding('s', 'find_series', 'Series'),
        Binding('escape', 'cancel', 'Cancel'),
        Binding('q', 'cancel', 'Cancel', show=False),
        Binding('ctrl+c', 'cancel', 'Cancel', show=False, priority=True),
    ]

    def __init__(self, client, model, series_error=None):
        """client is the shared RaceMonitorClient; model a seeded RaceListModel.
        series_error, when set, is the exception from a failed config-driven
        series enumeration, surfaced as an error state in the Series pane."""
        super().__init__()
        self.client = client
        self.model = model
        self.series_error = series_error
        self._main_screen = None

    def compose(self):
        """Lay out the series section, terms pane, races checklist, and footer."""
        with Horizontal():
            with Vertical(id='terms-pane'):
                yield Label('Series')
                yield Label('', id='series')
                yield Label('Search terms')
                yield ListView(id='terms')
                yield Input(placeholder='add search term…', id='new-term')
            with Vertical(id='races-pane'):
                yield Label(id='count')
                yield SelectionList(id='races')
        yield Footer()

    def on_mount(self):
        """Populate both panes from the model and focus the checklist."""
        self._main_screen = self.screen
        self._refresh_all()
        if self.series_error is not None:
            self.notify(
                f'series enumeration failed — showing term matches only: '
                f'{self.series_error}', severity='error')
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
        self._update_series()

    def _update_series(self):
        """Refresh the Series section: pin, error state, or how-to hint."""
        series = self.model.series
        label = self.query_one('#series', Label)
        if series is not None:
            _series_id, name, count = series
            label.update(f'📌 {name} ({count} races)')
        elif self.series_error is not None:
            label.update('⚠ series enumeration failed')
        else:
            label.update('none — press s to find')

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

    def check_action(self, action, parameters):
        """Disable 'confirm' while a modal (e.g. SeriesSearchModal) is on top.

        'enter' is a priority binding, which — unlike regular bindings — is
        not confined to the active screen's modal boundary: Textual checks it
        against the whole focus chain, App included, before the focused
        widget ever sees the key. Without this guard, pressing enter inside
        the series-search modal's Input/ListView would exit the whole app
        instead of submitting the search or picking a hit.
        """
        return not (action == 'confirm' and self.screen is not self._main_screen)

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
                               terms_changed=self.model.terms_changed,
                               series_id=self.model.series_id,
                               series_changed=self.model.series_changed))

    def action_cancel(self):
        """Exit without a selection."""
        self.exit(None)

    def action_find_series(self):
        """Open the series-search modal; pin its result into the model."""
        def _pinned(result):
            if result is None:
                return
            series_id, series_name, races = result
            self.model.set_series(series_id, series_name, races)
            self._refresh_all()
        self.push_screen(SeriesSearchModal(self.client, self.model), _pinned)

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
        """Fetch a term's results off the UI thread (rate limit may block ~10s).

        If the app exits while the request is in flight, this worker is cancelled;
        skip call_from_thread in that case since the event loop is already closed.
        """
        worker = get_current_worker()
        try:
            resp = self.client.results.search_results(term)
        except RaceMonitorError as exc:
            if worker.is_cancelled:
                return
            self.call_from_thread(
                self.notify, f'search "{term}" failed: {exc}', severity='error')
            return
        if worker.is_cancelled:
            return
        self.call_from_thread(self._merge_results, term, resp.get('Races', []))

    def _merge_results(self, term, results):
        """Merge a completed term search into the model and rebuild the panes."""
        self.model.add_term(term, results)
        self._refresh_all()


def refine_races(client, terms, races_by_term, start_epoc, series=None,
                 series_error=None):
    """Run the refinement app; return a RefineResult, or None if cancelled.

    client is the already-open RaceMonitorClient from the initial search (its
    rate-limiter window is shared with in-app searches). races_by_term is the
    per-term output of search_races_by_term(); series the config-driven
    (series_id, series_name, races) enumeration or None; series_error the
    exception from a failed enumeration (shown as an in-app error state).
    start_epoc filters in-app search results the same way --start-date
    filtered the initial ones.
    """
    model = RaceListModel(terms, races_by_term, start_epoc, series=series)
    app = BackfillApp(client, model, series_error=series_error)
    try:
        return app.run()
    except KeyboardInterrupt:
        return None
