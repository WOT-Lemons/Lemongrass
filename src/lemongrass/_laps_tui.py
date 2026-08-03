"""Interactive laps TUI: find a race, then watch it live or import it.

LapBoardModel holds the dashboard's derived state (lap rows, leaderboard rows)
with no Textual imports, so the rendering logic is unit-testable. The screens
below drive it from a background worker via _TuiObserver.
"""

import logging
import threading
import time
from datetime import UTC, datetime
from typing import ClassVar

from race_monitor import RaceMonitorError
from textual import work
from textual.app import App
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Checkbox,
    DataTable,
    Footer,
    Input,
    Label,
    ListItem,
    ListView,
    RichLog,
)
from textual.worker import get_current_worker

from lemongrass._tui import _LogSink, _race_label, _routed_output, _sink_bound


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


class LapsFlowMixin:
    """App-level coordination for the laps flow, shared by the standalone LapsApp and
    the unified LemongrassApp. Both hold a RaceMonitorClient and push the laps
    screens; the screens only ever reach these methods via self.app."""

    def _init_laps_flow(self, client):
        self.client = client
        self.picked = None  # (race_details, is_live) once a race is chosen
        self.monitor_args = None   # set by CarSelectScreen

    def start_laps(self):
        """Enter the laps flow at the race picker."""
        self.push_screen(PickerScreen(self.client))

    def _on_race_resolved(self, details, is_live, race_id):
        """Branch to the live or import flow once a race is resolved."""
        self.picked = (details, is_live)
        race = details.get('Race', {})
        name = race.get('Name', str(race_id))
        if is_live:
            self.push_screen(CarSelectScreen(self.client, race_id, name))
        else:
            # Not live covers two very different races — one that has not started
            # yet and one that finished — and is_live cannot tell them apart, so
            # let the user choose instead of assuming the race is over.
            self.push_screen(NotLiveScreen(self.client, race_id, name, details))

    def _confirm_import(self, race_id, race_name):
        self.push_screen(ImportConfirmScreen(self.client, race_id, race_name))

    def _start_wait(self, race_id, race_name):
        self.push_screen(WaitForLiveScreen(self.client, race_id, race_name))

    def _start_monitor(self, race_id, car_number, network, interval):
        self.monitor_args = (race_id, car_number, network, interval)
        self.push_screen(MonitorScreen(self.client, race_id, car_number, network, interval))

    def _start_import(self, race_id, race_name):
        self.push_screen(ImportScreen(self.client, race_id, race_name))

    def offer_final_import(self, race_id):
        """After a race ends live, offer to run the authoritative import."""
        def _answer(yes):
            if yes:
                self._start_import(race_id, str(race_id))
        self.push_screen(_ConfirmModal(
            'Race ended — run authoritative final import now? [y/n]'), _answer)


class LapsApp(LapsFlowMixin, App):
    """Laps TUI root (standalone `lemongrass laps` path). Owns the shared client
    and the routed-log handler; the exit value is unused (screens perform their
    own work in place)."""

    def __init__(self, client):
        super().__init__()
        self._init_laps_flow(client)

    def on_mount(self):
        """Open on the picker."""
        self.start_laps()


def run_laps_tui(client):
    """Run the laps TUI against an already-open RaceMonitorClient.

    Root logging is routed into per-screen log sinks (via _routed_output()) for
    the app's lifetime and restored afterwards. Returns 0.
    """
    app = LapsApp(client)
    with _routed_output():
        app.run()
    return 0


class _ConfirmModal(ModalScreen):
    """Tiny yes/no modal. Dismisses True on 'y', False on 'n'/escape."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('y', 'yes', 'Yes'),
        Binding('n', 'no', 'No'),
        Binding('escape', 'no', 'No', show=False),
    ]

    def __init__(self, prompt):
        super().__init__()
        self._prompt = prompt

    def compose(self):
        yield Label(self._prompt)
        yield Footer()

    def action_yes(self):
        self.dismiss(True)

    def action_no(self):
        self.dismiss(False)


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
        if self._hits:
            noun = 'race' if len(self._hits) == 1 else 'races'
            self._status(f'{len(self._hits)} {noun} — pick one')
        else:
            self._status('no races found')

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
        self.app.call_from_thread(self.app._on_race_resolved, details, is_live, race_id)


class NotLiveScreen(Screen):
    """A race that isn't live: wait for the green flag, or import it as completed."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'app.pop_screen', 'Back'),
    ]

    def __init__(self, client, race_id, race_name, race_details=None):
        super().__init__()
        self.client = client
        self.race_id = race_id
        self.race_name = race_name
        self._race = (race_details or {}).get('Race', {})

    def compose(self):
        yield Label(f'{self.race_name} (#{self.race_id}) is not live right now.')
        yield Label(f'Scheduled start: {self._scheduled()}', id='scheduled')
        yield ListView(
            ListItem(Label('Wait for the green flag, then monitor'), id='wait'),
            ListItem(Label('Import as a completed race'), id='import'),
            id='menu')
        yield Footer()

    def _scheduled(self):
        start = self._race.get('StartDateEpoc') or 0
        if not start:
            return 'unknown'
        return datetime.fromtimestamp(start, tz=UTC).strftime('%Y-%m-%d %H:%M UTC')

    def _upcoming(self):
        """True if the race's stored end time hasn't passed (0 = unknown, treat
        as upcoming)."""
        end = self._race.get('EndDateEpoc') or 0
        return end == 0 or end > time.time()

    def on_mount(self):
        # Focus is a hint about which choice is likely, never a decision — both
        # rows stay one arrow key apart because RaceMonitor's dates can be wrong.
        view = self.query_one('#menu', ListView)
        view.index = 0 if self._upcoming() else 1
        view.focus()

    def on_list_view_selected(self, event):
        if event.item.id == 'wait':
            self.app._start_wait(self.race_id, self.race_name)
        elif event.item.id == 'import':
            self.app._confirm_import(self.race_id, self.race_name)


class WaitForLiveScreen(Screen):
    """Wait for a race to go live, then start monitoring a pre-chosen car.

    One worker, two phases: laps.wait_for_live for the green flag, then
    laps.wait_for_car until the number appears in the timing feed. Neither phase
    ever gives up — the point of this screen is that lap capture starts while
    nobody is at the keyboard — so the only exits are the car being found, 'c'
    (open the car picker), and escape/unmount.

    The car list CarSelectScreen shows cannot exist before the feed is live,
    which is why the number is typed here instead.
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'cancel', 'Cancel'),
        Binding('c', 'change_car', 'Change car'),
    ]

    def __init__(self, client, race_id, race_name):
        super().__init__()
        self.client = client
        self.race_id = race_id
        self.race_name = race_name
        self.car_number = None   # set once the wait starts; also the "started" flag
        self.network = True
        self.interval = 30
        self.live = False        # phase flag: gates the 'c' binding
        self._stop = threading.Event()

    def compose(self):
        yield Label(f'{self.race_name} (#{self.race_id}) — wait for the green flag.')
        yield Input(placeholder='car number to monitor…', id='car-number')
        yield Checkbox('Write to InfluxDB', value=True, id='write')
        yield Input(value='30', id='interval')
        yield Label('enter a car number to start waiting', id='status')
        yield Footer()

    def on_mount(self):
        self.query_one('#car-number', Input).focus()

    def check_action(self, action, parameters):
        # Hide 'c' from the footer until the race is live: before then there is
        # no field for CarSelectScreen to list.
        if action == 'change_car' and not self.live:
            return None
        return True

    def on_input_submitted(self, event):
        # Only the car-number Input starts the wait; Enter in the interval Input
        # is inert, mirroring CarSelectScreen.
        event.stop()
        if event.input.id == 'car-number':
            self._begin(event.value.strip())

    def _begin(self, car_number):
        if self.car_number is not None:
            return  # already waiting
        if not car_number:
            self._status('enter a car number to start waiting')
            return
        # Read the settings once and freeze the widgets: the wait can run for
        # hours, and the worker must not query widgets that may be torn down.
        self.car_number = car_number
        self.network = self.query_one('#write', Checkbox).value
        self.interval = _as_int(self.query_one('#interval', Input).value) or 30
        for widget_id in ('#car-number', '#write', '#interval'):
            self.query_one(widget_id).disabled = True
        self._status('waiting for green flag…')
        self._wait()

    # --- main-thread UI updates ---
    def _status(self, text):
        self.query_one('#status', Label).update(text)

    def _mark_live(self):
        self.live = True
        self.refresh_bindings()  # 'c' becomes available now that a field exists

    def _go_live(self):
        # Guarded here (main thread), not on the worker thread: _go_live runs
        # via call_from_thread, so by the time it actually executes,
        # action_change_car may already have popped this screen and pushed
        # CarSelectScreen. Checking self.app.screen is not self catches that —
        # a worker-side is_set() check does not, because it runs earlier and
        # Textual's screen prune is deferred.
        if self._stop.is_set() or self.app.screen is not self:
            return
        # Pop first so quitting the monitor lands back on NotLiveScreen rather
        # than on a wait screen whose worker is already finished.
        self.app.pop_screen()
        self.app._start_monitor(self.race_id, self.car_number, self.network, self.interval)

    # --- background wait ---
    def _call(self, fn, *args):
        # Same guard as _TuiObserver._call: never marshal into a torn-down event
        # loop or against removed widgets.
        if get_current_worker().is_cancelled or not self.is_mounted:
            return
        self.app.call_from_thread(fn, *args)

    def _tick_live(self, count, error):
        text = f'waiting for green flag — {count} checks, last {self._stamp()}'
        if error:
            text += f' (last check failed: {error})'
        self._call(self._status, text)

    def _tick_car(self, count, state, error):
        if state == 'no_feed':
            text = f'live — waiting for the timing feed… {count} checks, last {self._stamp()}'
        else:
            text = (f'live — car #{self.car_number} not in the field yet — '
                    f'{count} checks, last {self._stamp()}')
        if error:
            text += f' (last check failed: {error})'
        self._call(self._status, text)

    @staticmethod
    def _stamp():
        return datetime.now().strftime('%H:%M:%S')

    @work(thread=True, exit_on_error=False)
    def _wait(self):
        # exit_on_error=False: call_from_thread re-raises the callback's
        # exception in this worker thread, and the default exit_on_error=True
        # would route that into app._handle_exception and kill the app — the
        # opposite of the "must never crash the app" guarantee below.
        from lemongrass import laps as laps_mod
        try:
            if not laps_mod.wait_for_live(self.client, self.race_id, self._stop,
                                          on_tick=self._tick_live):
                return
            self._call(self._mark_live)
            if not laps_mod.wait_for_car(self.client, self.race_id, self.car_number,
                                         self._stop, on_tick=self._tick_car):
                return
        except Exception as exc:  # last resort: a TUI worker must never crash the app
            logging.exception("wait worker failed")
            self._call(self._status, f'wait failed: {exc}')
            return
        # The invariant (don't pop/push behind a user who already left via 'c'
        # or escape) is enforced inside _go_live itself, on the main thread —
        # see its docstring/comment for why a worker-side check here would not
        # be race-free.
        self._call(self._go_live)

    # --- user exits ---
    def action_cancel(self):
        self._stop.set()
        self.app.pop_screen()

    def action_change_car(self):
        if not self.live:
            return
        self._stop.set()
        self.app.pop_screen()
        self.app.push_screen(CarSelectScreen(self.client, self.race_id, self.race_name))

    def on_unmount(self):
        # End the wait on ANY teardown (app exit, ctrl+c), not just escape.
        self._stop.set()


class CarSelectScreen(Screen):
    """Pick the tracked car from the live feed (or type a number) + options."""

    # No priority Enter: it would intercept the key before ListView.Selected
    # fires, so a keyboard user could never pick a car from the list. Enter is
    # instead consumed by whichever widget is focused — the Input emits
    # Input.Submitted, the ListView emits ListView.Selected.
    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'app.pop_screen', 'Back'),
    ]

    def __init__(self, client, race_id, race_name):
        super().__init__()
        self.client = client
        self.race_id = race_id
        self.race_name = race_name
        self._competitors = []

    def compose(self):
        yield Label(f'{self.race_name} — LIVE. Pick your car:')
        yield ListView(id='cars')
        yield Input(placeholder='…or type a car number', id='car-number')
        yield Checkbox('Write to InfluxDB', value=True, id='write')
        yield Input(value='30', id='interval')
        yield Label('', id='status')
        yield Footer()

    def on_mount(self):
        self._load()

    @work(thread=True, exclusive=True)
    def _load(self):
        worker = get_current_worker()
        try:
            resp = self.client.live.get_session(self.race_id)
        except RaceMonitorError as exc:
            if not worker.is_cancelled:
                self.app.call_from_thread(self.app.notify, f'session load failed: {exc}',
                                          severity='error')
            return
        if worker.is_cancelled:
            return
        comps = list(resp.get('Session', {}).get('Competitors', {}).values())
        self.app.call_from_thread(self._show, comps)

    def _show(self, competitors):
        self._competitors = competitors
        view = self.query_one('#cars', ListView)
        view.clear()
        for comp in competitors:
            name = f"{comp.get('FirstName', '')} {comp.get('LastName', '')}".strip()
            view.append(ListItem(Label(f"#{comp.get('Number', '?')}  {name}")))

    def on_list_view_selected(self, event):
        index = self.query_one('#cars', ListView).index
        if index is not None and self._competitors:
            self._confirm(str(self._competitors[index].get('Number', '')))

    def on_input_submitted(self, event):
        # Only the car-number Input confirms; Enter in the interval Input is inert
        # (its value must not be mistaken for a car number).
        event.stop()
        if event.input.id == 'car-number':
            self._confirm(event.value.strip())

    def _confirm(self, car_number):
        if not car_number:
            self.query_one('#status', Label).update('pick or type a car number')
            return
        network = self.query_one('#write', Checkbox).value
        interval = _as_int(self.query_one('#interval', Input).value) or 30
        self.app._start_monitor(self.race_id, car_number, network, interval)


class _TuiObserver:
    """laps.RaceObserver that marshals live events onto MonitorScreen.

    Not a subclass import to avoid a circular import at module load; laps
    accepts any object with these methods (duck-typed observer).
    """

    def __init__(self, screen):
        self._screen = screen

    def _call(self, fn, *args):
        # Runs on the monitor worker thread. Skip marshalling into a torn-down
        # event loop if the worker was cancelled (app exit not routed through
        # action_quit_monitor). Also skip once the screen is unmounted: pressing
        # 'q' pops the screen but can't interrupt an in-flight blocking API call,
        # and is_cancelled may not have propagated by the time it returns — a
        # call_from_thread against removed widgets would raise NoMatches.
        if get_current_worker().is_cancelled or not self._screen.is_mounted:
            return
        self._screen.app.call_from_thread(fn, *args)

    def on_rankings(self, sorted_competitors, race_live, selected_class, categories):
        pass  # the TUI renders its own leaderboard from on_standings

    def on_live_detail(self, competitor_details, class_name, class_position):
        name = competitor_details.get('Name', '')
        header = f"#{competitor_details.get('Number', '?')} {name} — class {class_name}"
        if class_position is not None:
            header += f" (P{class_position})"
        self._call(self._screen.set_header, header)

    def on_laps(self, laps):
        self._call(self._screen.set_laps, laps)

    def on_lap(self, lap):
        self._call(self._screen.add_lap, lap)

    def on_session_change(self, session_name):
        self._call(self._screen.log_line, f'New session: {session_name}')

    def on_standings(self, session_response):
        self._call(self._screen.set_standings, session_response)

    def on_status(self, text):
        # Drop the 80-dash separator lines the terminal path emits — they are
        # noise in the log pane.
        if text.strip('-\n'):
            self._call(self._screen.log_line, text)

    def on_race_ended(self):
        self._call(self._screen.on_race_ended)


class MonitorScreen(Screen):
    """Live dashboard: tracked-car laps, field leaderboard, log pane."""

    CSS = """
    #laps { height: 1fr; }
    #board { height: 1fr; }
    #log { height: 6; }
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('q', 'quit_monitor', 'Quit'),
    ]

    def __init__(self, client, race_id, car_number, network, interval):
        super().__init__()
        self.sink = _LogSink()
        self.client = client
        self.race_id = race_id
        self.car_number = car_number
        self.network = network
        self.interval = interval
        self.board = LapBoardModel()
        self._stop = threading.Event()

    def compose(self):
        yield Label('', id='header')
        with Horizontal():
            with Vertical():
                yield Label('Laps')
                yield DataTable(id='laps')
            with Vertical():
                yield Label('Leaderboard')
                yield DataTable(id='board')
        yield RichLog(id='log')
        yield Footer()

    def on_mount(self):
        self.query_one('#laps', DataTable).add_columns('Lap', 'Time', 'Pos', 'ClassPos')
        self.query_one('#board', DataTable).add_columns('Pos', '#', 'Name', 'Laps', 'Best')
        self.set_interval(0.25, self._drain_log)
        self._run()

    # --- observer-driven UI updates (main thread) ---
    def set_header(self, text):
        self.query_one('#header', Label).update(text)

    def set_laps(self, laps):
        self.board.set_laps(laps)
        self._rebuild_laps()

    def add_lap(self, lap):
        self.board.add_lap(lap)
        self._rebuild_laps()

    def _rebuild_laps(self):
        # Newest lap first: for a live race the most recent lap is what matters,
        # so render in reverse of the model's ascending order (the table is
        # rebuilt each tick and scroll resets to the top, so row 0 is always
        # on-screen). The model stays ascending/UI-agnostic.
        table = self.query_one('#laps', DataTable)
        table.clear()
        for row in reversed(self.board.lap_rows()):
            table.add_row(*(str(c) for c in row))

    def set_standings(self, session_response):
        self.board.set_standings(session_response)
        table = self.query_one('#board', DataTable)
        table.clear()
        for row in self.board.leaderboard_rows():
            table.add_row(*(str(c) for c in row))

    def log_line(self, text):
        self.query_one('#log', RichLog).write(text)

    def on_race_ended(self):
        self.log_line('Race has ended.')
        self.app.offer_final_import(self.race_id)  # Task 10

    def _drain_log(self):
        log_view = self.query_one('#log', RichLog)
        while self.sink.lines:
            log_view.write(self.sink.lines.popleft())

    # --- background monitor loop ---
    @work(thread=True)
    def _run(self):
        from lemongrass import _influx
        from lemongrass import laps as laps_mod
        opts = laps_mod.RaceOptions(
            network_mode=self.network, monitor_mode=True, interval=self.interval)
        observer = _TuiObserver(self)
        with _sink_bound(self.sink):
            try:
                if self.network:
                    # Writing needs a live Influx handle + resolved metadata,
                    # exactly as laps.backfill_race sets up before live_race. The
                    # connection stays open for the whole poll loop.
                    with _influx.connect() as influx_client:
                        ctx = self._network_ctx(laps_mod, influx_client)
                        self._monitor_until_done(laps_mod, ctx, opts, observer)
                else:
                    ctx = laps_mod.RaceContext(
                        str(self.race_id), str(self.car_number), self.client, None, 0)
                    self._monitor_until_done(laps_mod, ctx, opts, observer)
            except RaceMonitorError as exc:
                if not get_current_worker().is_cancelled:
                    self.app.call_from_thread(self.log_line, f'error: {exc}')
            except Exception as exc:  # last resort: a TUI worker must never crash the app
                logging.exception("monitor worker failed")
                if not get_current_worker().is_cancelled:
                    self.app.call_from_thread(self.log_line, f'error: {exc}')

    def _network_ctx(self, laps_mod, influx_client):
        """Build a write-enabled RaceContext, mirroring backfill_race's live
        setup: a SYNCHRONOUS write_api, delete/query handles, and race metadata
        resolved from a race.details fetch (also the source of the start epoch).
        Without this, network writes silently no-op (write_api=None)."""
        from influxdb_client.client.write_api import SYNCHRONOUS
        race_details = self.client.race.details(self.race_id)
        metadata = laps_mod._resolve_race_metadata(race_details, self.client)
        start_epoc = (race_details['Race'].get('StartDateEpoc', 0)
                      if race_details.get('Successful') else 0)
        return laps_mod.RaceContext(
            str(self.race_id), str(self.car_number), self.client,
            influx_client.write_api(write_options=SYNCHRONOUS), start_epoc,
            metadata=metadata,
            delete_api=influx_client.delete_api(),
            query_api=influx_client.query_api())

    def _monitor_until_done(self, laps_mod, ctx, opts, observer):
        """Run live_race, re-waiting for the car whenever it isn't in the feed.

        Cars trickle into the timing feed after the green flag, so a
        NO_LIVE_DATA at launch is a "not yet", not a failure — returning here
        would leave an unattended session collecting nothing. Every other
        outcome (race ended, stop event, error) is returned to _run unchanged.
        Each retry re-checks is_live: the race can end (or be red-flagged to a
        finish) before the car ever appears, and without that check this would
        poll a rate-limited API forever."""
        attempts = 0
        while not self._stop.is_set():
            result = laps_mod.live_race(ctx, opts, observer=observer,
                                        _stop_event=self._stop)
            if result is not laps_mod.MonitorStatus.NO_LIVE_DATA:
                return result
            if attempts and self._stop.wait(laps_mod._WAIT_POLL_S):
                # get_session and get_racer can disagree; pace the retry so a car
                # listed in the field but with no racer detail yet doesn't spin.
                return None
            attempts += 1
            # A failed/unsuccessful check is "unknown", not "ended", so only an
            # explicit successful-and-not-live response stops the wait.
            try:
                is_live_resp = self.client.race.is_live(self.race_id)
                if is_live_resp.get('Successful') and not is_live_resp.get('IsLive'):
                    self._call(
                        self.log_line,
                        f'race {self.race_id} ended before car #{self.car_number} appeared')
                    return None
            except Exception:
                logging.debug("is_live check failed; skipping")
            self._call(self.log_line,
                       f'car #{self.car_number} is not in the live feed yet — waiting…')
            if not laps_mod.wait_for_car(self.client, self.race_id, self.car_number,
                                         self._stop, on_tick=self._car_tick):
                return None
        return None

    def _call(self, fn, *args):
        # Same guard as _TuiObserver._call: never marshal into a torn-down loop.
        if get_current_worker().is_cancelled or not self.is_mounted:
            return
        self.app.call_from_thread(fn, *args)

    def _car_tick(self, count, state, error):
        if state == 'absent':
            text = f'car #{self.car_number} still not in the field — {count} checks'
        else:
            text = f'waiting for the timing feed — {count} checks'
        if error:
            text += f' (last check failed: {error})'
        self._call(self.log_line, text)
        # Re-check is_live from inside the wait itself, every 30th tick (~once
        # per 5 minutes at the 10s poll cadence): the car may never appear at
        # all (wrong number, or the race red-flagged before it got out), and
        # wait_for_car has no timeout of its own — without this the wait
        # outlives the race it's waiting on. A failed/unsuccessful check is
        # "unknown", not "ended".
        if count % 30 == 0:
            try:
                is_live_resp = self.client.race.is_live(self.race_id)
                if is_live_resp.get('Successful') and not is_live_resp.get('IsLive'):
                    self._call(
                        self.log_line,
                        f'race {self.race_id} ended before car #{self.car_number} appeared')
                    self._stop.set()
            except Exception:
                logging.debug("is_live check failed; skipping")

    def action_quit_monitor(self):
        self._stop.set()
        self.app.pop_screen()

    def on_unmount(self):
        # End the poll loop on ANY teardown (app exit, ctrl+c), not just `q`, so
        # the worker never keeps polling into a torn-down event loop.
        self._stop.set()


class ImportConfirmScreen(Screen):
    """Confirm a fieldwide import of a completed race."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('enter', 'confirm', 'Import', priority=True),
        Binding('escape', 'app.pop_screen', 'Back'),
    ]

    def __init__(self, client, race_id, race_name):
        super().__init__()
        self.client = client
        self.race_id = race_id
        self.race_name = race_name

    def compose(self):
        yield Label(f'{self.race_name} (#{self.race_id}) is completed.')
        yield Label('Press Enter to import all lap data into InfluxDB.')
        yield Footer()

    def action_confirm(self):
        self.app._start_import(self.race_id, self.race_name)


class ImportScreen(Screen):
    """Run the fieldwide import in a worker; stream logs + a final summary."""

    CSS = "#log { height: 1fr; }"

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('q', 'app.pop_screen', 'Back'),
    ]

    def __init__(self, client, race_id, race_name):
        super().__init__()
        self.sink = _LogSink()
        self.client = client
        self.race_id = race_id
        self.race_name = race_name

    def compose(self):
        yield Label(f'Importing {self.race_name} (#{self.race_id})…', id='title')
        yield RichLog(id='log')
        yield Footer()

    def on_mount(self):
        self.set_interval(0.25, self._drain_log)
        self._run()

    def _drain_log(self):
        log_view = self.query_one('#log', RichLog)
        while self.sink.lines:
            log_view.write(self.sink.lines.popleft())

    @work(thread=True)
    def _run(self):
        from lemongrass import laps as laps_mod
        opts = laps_mod.RaceOptions(network_mode=True)
        summary = None
        with _sink_bound(self.sink):
            try:
                rc = laps_mod.backfill_race(str(self.race_id), None, self.client, opts)
                summary = 'Import complete.' if rc == 0 else f'Import failed (exit {rc}).'
            except RaceMonitorError as exc:
                summary = f'import failed: {exc}'
            except Exception as exc:  # last resort: a TUI worker must never crash the app
                logging.exception("import worker failed")
                summary = f'import failed: {exc}'
            finally:
                self.sink.flush()
        if summary is not None and not get_current_worker().is_cancelled:
            self.app.call_from_thread(self._done, summary)

    def _done(self, message):
        self.query_one('#title', Label).update(message)
        self.query_one('#log', RichLog).write(message)
