"""Interactive laps TUI: find a race, then watch it live or import it.

LapBoardModel holds the dashboard's derived state (lap rows, leaderboard rows)
with no Textual imports, so the rendering logic is unit-testable. The screens
below drive it from a background worker via _TuiObserver.
"""

from typing import ClassVar

from race_monitor import RaceMonitorError
from textual import work
from textual.app import App
from textual.binding import Binding, BindingType
from textual.screen import Screen
from textual.widgets import (
    Footer,
    Input,
    Label,
    ListItem,
    ListView,
)
from textual.worker import get_current_worker

from lemongrass._tui import _race_label, _TuiLogHandler


def _as_int(value):
    """int(value) or None."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


class LapBoardModel:
    """Derived state for the live dashboard: tracked-car laps + field standings."""

    def __init__(self):
        self._laps = []
        self._standings = []  # list of (pos, number, name, laps, best_lap)

    def set_laps(self, laps):
        """Replace the tracked car's lap list."""
        self._laps = list(laps)

    def add_lap(self, lap):
        """Append one newly-arrived lap."""
        self._laps.append(lap)

    def lap_rows(self):
        """Rows for the lap table: (lap_no, lap_time, position, class_pos)."""
        rows = []
        for lap in self._laps:
            lap_no = _as_int(lap.get('Lap'))
            if lap_no is None:
                continue
            pos = _as_int(lap.get('Position'))
            rows.append((lap_no, lap.get('LapTime', ''),
                         pos if pos is not None else '-',
                         lap.get('ClassPosition', '-')))
        return rows

    def set_standings(self, session_response):
        """Store leaderboard rows from a live get_session response."""
        if not session_response.get('Successful'):
            return
        competitors = session_response.get('Session', {}).get('Competitors', {})
        rows = []
        for comp in competitors.values():
            pos = _as_int(comp.get('Position'))
            if pos is None:
                continue
            name = f"{comp.get('FirstName', '')} {comp.get('LastName', '')}".strip()
            rows.append((pos, comp.get('Number', ''), name,
                         _as_int(comp.get('Laps')) or 0,
                         comp.get('BestLapTime', '')))
        self._standings = sorted(rows, key=lambda r: r[0])

    def leaderboard_rows(self):
        """Rows for the leaderboard: (position, number, name, laps, best_lap)."""
        return list(self._standings)


class LapsApp(App):
    """Laps TUI root. Owns the shared client and the routed-log handler; the
    exit value is unused (screens perform their own work in place)."""

    def __init__(self, client):
        super().__init__()
        self.client = client
        self.log_handler = _TuiLogHandler()
        self.picked = None  # (race_details, is_live) once a race is chosen

    def on_mount(self):
        """Open on the picker."""
        self.push_screen(PickerScreen(self.client))

    def _on_race_resolved(self, details, is_live):
        """Branch to the live or import flow once a race is resolved.

        Filled in by later tasks; for now it just records the pick so tests can
        assert the resolve path ran.
        """
        self.picked = (details, is_live)


class PickerScreen(Screen):
    """Find a race by name (search) or numeric ID, then resolve is_live."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'app.pop_screen', 'Back', show=False),
    ]

    def __init__(self, client):
        super().__init__()
        self.client = client
        self._hits = []

    def compose(self):
        yield Label('Find a race — type a name to search, or paste a race ID')
        yield Input(placeholder='race name or numeric ID…', id='query')
        yield ListView(id='hits')
        yield Label('', id='status')
        yield Footer()

    def on_mount(self):
        self.query_one('#query', Input).focus()

    def _status(self, text):
        self.query_one('#status', Label).update(text)

    def on_input_submitted(self, event):
        event.stop()
        query = event.value.strip()
        if not query:
            return
        if query.isdigit():
            self._status(f'resolving race {query}…')
            self._resolve(int(query))
        else:
            self._status(f'searching "{query}"…')
            self._search(query)

    @work(thread=True, exclusive=True)
    def _search(self, term):
        worker = get_current_worker()
        try:
            resp = self.client.results.search_results(term)
        except RaceMonitorError as exc:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._fail, f'search failed: {exc}')
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._show_hits, resp.get('Races', []))

    def _fail(self, message):
        self._status('')
        self.app.notify(message, severity='error')

    def _show_hits(self, races):
        self._hits = list(races)
        view = self.query_one('#hits', ListView)
        view.clear()
        for race in self._hits:
            view.append(ListItem(Label(_race_label(race))))
        self._status(f'{len(self._hits)} races — pick one'
                     if self._hits else 'no races found')

    def on_list_view_selected(self, event):
        index = self.query_one('#hits', ListView).index
        if index is None or not self._hits:
            return
        self._status('resolving…')
        self._resolve(self._hits[index]['ID'])

    @work(thread=True, exclusive=True)
    def _resolve(self, race_id):
        worker = get_current_worker()
        try:
            details = self.client.race.details(race_id)
            live_resp = self.client.race.is_live(race_id)
        except RaceMonitorError as exc:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._fail, f'lookup failed: {exc}')
            return
        if worker.is_cancelled:
            return
        is_live = bool(live_resp.get('Successful') and live_resp.get('IsLive'))
        self.app.call_from_thread(self.app._on_race_resolved, details, is_live)
