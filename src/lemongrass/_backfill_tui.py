"""Interactive refinement of the race-backfill selection.

refine_races() (added with the Textual app) is the public entry point: it shows
discovered races in a two-pane TUI — search terms on the left, a race checklist
on the right — and returns the confirmed selection. RaceListModel below holds
every piece of state and all merge/dedup logic, with no UI imports, so the
behavior is unit-testable without Textual.
"""

from dataclasses import dataclass
from typing import ClassVar

from race_monitor import RaceMonitorError
from textual import work
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Footer,
    Input,
    Label,
    ListItem,
    ListView,
    RichLog,
    SelectionList,
)
from textual.widgets.selection_list import Selection
from textual.worker import get_current_worker

from lemongrass._tui import LogPaneScreen, _race_label, _routed_output, _sink_bound
from lemongrass.race_backfill import enumerate_series, filter_races_by_terms


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

    Pinned (a series is set): the series is the only candidate set and the
    active terms filter it by race name — empty terms means the whole
    series. Unpinned: terms are search sources, unioned and deduped by race
    ID. The per-term cache outlives removal, so re-adding a removed term in
    unpinned mode costs no API call. races() derives the visible list on
    demand, filtered to StartDateEpoc >= start_epoc and date-sorted.
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

    def races(self):
        """Return visible races, date-filtered and date-sorted (see class
        docstring for pinned vs unpinned semantics)."""
        if self._series:
            races = [r for r in self._cache.get(_SERIES_KEY, [])
                     if r['StartDateEpoc'] >= self.start_epoc]
            return sorted(filter_races_by_terms(races, self.terms),
                          key=lambda r: r['StartDateEpoc'])
        seen = {}
        for term in self.terms:
            for race in self._cache.get(term, []):
                if race['StartDateEpoc'] >= self.start_epoc:
                    seen[race['ID']] = race
        return sorted(seen.values(), key=lambda r: r['StartDateEpoc'])

    def _rebalance_checked(self, before):
        """Re-derive checked after a visibility change: newly visible races
        become checked, hidden races drop out, and the user's choices on
        still-visible races are preserved."""
        visible = {race['ID'] for race in self.races()}
        self.checked = (self.checked & visible) | (visible - before)

    def set_series(self, series_id, series_name, races):
        """Pin (or replace) the series source and re-derive the view."""
        before = {race['ID'] for race in self.races()}
        self._series = (series_id, series_name)
        self._cache[_SERIES_KEY] = list(races)
        self._rebalance_checked(before)

    def cache_results(self, term, results):
        """Session-cache a term's raw results without activating the term
        (used by the series modal so its searches double as term cache)."""
        self._cache.setdefault(term, list(results))

    @property
    def series(self):
        """(series_id, series_name, matched, total), or None if unpinned.

        matched counts series races passing the date and term filters; total
        counts date-filtered only (what clearing every term would show).
        """
        if self._series is None:
            return None
        series_id, series_name = self._series
        dated = [r for r in self._cache.get(_SERIES_KEY, [])
                 if r['StartDateEpoc'] >= self.start_epoc]
        return (series_id, series_name,
                len(filter_races_by_terms(dated, self.terms)), len(dated))

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

    def cached(self, term):
        """Return term's session-cached search results (KeyError if absent)."""
        return self._cache[term]

    def add_term(self, term, results=None):
        """Activate a term and re-derive the visible set.

        Unpinned, results is the term's raw search-result list, required
        unless the term is already session-cached. Pinned, terms are local
        name filters and need no results. Blank and already-active terms are
        ignored.
        """
        term = term.strip()
        if not term or term in self.terms:
            return
        if results is not None:
            self._cache[term] = list(results)
        elif not self._series and term not in self._cache:
            raise ValueError(f"no cached results for {term!r}; pass results")
        before = {race['ID'] for race in self.races()}
        self.terms.append(term)
        self._rebalance_checked(before)

    def remove_term(self, term):
        """Deactivate a term and re-derive the visible set (pinned mode can
        broaden: removing the last term shows the whole series)."""
        if term not in self.terms:
            return
        before = {race['ID'] for race in self.races()}
        self.terms.remove(term)
        self._rebalance_checked(before)

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
        RefineScreen.on_input_submitted (its '#new-term' fallback path), which
        would otherwise also fire for the same keypress.
        """
        event.stop()
        term = event.value.strip()
        if not term:
            return
        if self.model.has_cached(term):
            self._show_hits(term, self.model.cached(term))
            return
        self._status(f'searching "{term}"…')
        self._search(term)

    @work(thread=True, exclusive=True)
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

    @work(thread=True, exclusive=True)
    def _resolve(self, race_id):
        """race.details → SeriesID → enumerate_series; dismiss with the pin.

        Two-plus rate-limited calls, so runs off the UI thread. A KeyError
        covers a malformed details payload (Beta-adjacent defensiveness).
        Exclusive so a second hit selection cancels the first resolve rather
        than racing to dismiss an already-popped modal.
        """
        worker = get_current_worker()
        try:
            details = self.client.race.details(race_id)
            series_id = details['Race']['SeriesID']
            if not series_id:
                # past_races treats series_id=0 as "return all races"; it is
                # also the disabled sentinel, so pinning it is never intended.
                raise RaceMonitorError(
                    f'race {race_id} is not part of a series')
            races = enumerate_series(self.client, series_id, self.model.start_epoc)
        except (KeyError, RaceMonitorError) as exc:
            if worker.is_cancelled:
                return
            self.app.call_from_thread(
                self._fail, f'series lookup failed: {exc!r}')
            return
        if worker.is_cancelled:
            return
        name = (races[0].get('SeriesName') if races else None) or f'series {series_id}'
        self.app.call_from_thread(self.dismiss, (series_id, name, races))


class RefineScreen(LogPaneScreen, Screen):
    """Two-pane refinement UI; dismisses with a RefineResult, or None on cancel.

    All state changes are delegated to the RaceListModel; widgets are rebuilt
    from the model after every mutation, so the model stays the single source
    of truth.
    """

    CSS = """
    #terms-pane { width: 36; }
    #terms-pane, #races-pane { border: round $primary; padding: 0 1; }
    #log { height: 4; }
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
        self._init_sink()
        self.client = client
        self.model = model
        self.series_error = series_error

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
        yield RichLog(id='log')
        yield Footer()

    def on_mount(self):
        """Populate both panes from the model and focus the checklist."""
        self._refresh_all()
        if self.series_error is not None:
            self.notify(
                f'series enumeration failed — showing term matches only: '
                f'{self.series_error}', severity='error')
        self.set_interval(0.25, self._drain_log)
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
            _series_id, name, matched, total = series
            label.update(f'📌 {name} ({matched} of {total} races)')
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

    def action_confirm(self):
        """Dismiss with the confirmed selection.

        enter is a priority binding so it wins over the SelectionList; when the
        term Input is focused it submits the typed term instead of confirming
        (the Input would otherwise swallow the key). Textual only consults a
        priority binding against the *active* screen's binding chain (see
        Screen._binding_chain), so this action cannot fire while
        SeriesSearchModal — a separate screen — is on top; no extra guard is
        needed here (unlike when this UI lived directly on the App).
        """
        term_input = self.query_one('#new-term', Input)
        if self.focused is term_input:
            self._submit_term(term_input.value)
            return
        self.dismiss(RefineResult(races=self.model.selected(),
                                  terms=tuple(self.model.terms),
                                  terms_changed=self.model.terms_changed,
                                  series_id=self.model.series_id,
                                  series_changed=self.model.series_changed))

    def action_cancel(self):
        """Dismiss without a selection."""
        self.dismiss(None)

    def action_find_series(self):
        """Open the series-search modal; pin its result into the model."""
        def _pinned(result):
            if result is None:
                return
            series_id, series_name, races = result
            self.model.set_series(series_id, series_name, races)
            self._refresh_all()
        self.app.push_screen(SeriesSearchModal(self.client, self.model), _pinned)

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
        """Add a term: a local name filter when a series is pinned, else from
        the session cache or a live search."""
        term = value.strip()
        self.query_one('#new-term', Input).value = ''
        if not term or term in self.model.terms:
            return
        if self.model.series_id or self.model.has_cached(term):
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
        with _sink_bound(self.sink):
            try:
                resp = self.client.results.search_results(term)
            except RaceMonitorError as exc:
                if worker.is_cancelled:
                    return
                self.app.call_from_thread(
                    self.notify, f'search "{term}" failed: {exc}', severity='error')
                return
        if worker.is_cancelled:
            return
        self.app.call_from_thread(self._merge_results, term, resp.get('Races', []))

    def _merge_results(self, term, results):
        """Merge a completed term search into the model and rebuild the panes."""
        self.model.add_term(term, results)
        self._refresh_all()


def refine_races(client, terms, races_by_term, start_epoc, series=None,
                 series_error=None):
    """Run the refinement UI; return a RefineResult, or None if cancelled.

    Seeds RefineScreen as the start screen of the shared LemongrassApp and mirrors
    its dismissal to the app exit value. client is the already-open
    RaceMonitorClient; races_by_term the per-term search output; series the
    config-driven (series_id, series_name, races) enumeration or None; series_error
    the exception from a failed enumeration; start_epoc filters in-app searches.
    While the app runs, root logging is routed into the per-screen sinks."""
    from lemongrass._home_tui import LemongrassApp  # lazy: breaks home→races→backfill cycle
    model = RaceListModel(terms, races_by_term, start_epoc, series=series)
    screen = RefineScreen(client, model, series_error=series_error)
    app = LemongrassApp(client, start_screen=screen, exit_on_start_dismiss=True)
    try:
        with _routed_output():
            return app.run()
    except KeyboardInterrupt:
        return None
