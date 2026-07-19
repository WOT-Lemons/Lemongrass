"""Interactive races browser: list stored races, then prune / diagnose / backfill /
re-import them. The race table (the `list` view) is the hub; keys act on the checked
batch (prune) or the highlighted row (diagnose / re-import)."""

import contextlib
import logging
from typing import ClassVar

from textual import work
from textual.binding import Binding, BindingType
from textual.containers import Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import Footer, Input, Label, ListItem, ListView, RichLog, SelectionList
from textual.widgets.selection_list import Selection
from textual.worker import get_current_worker

from lemongrass import _influx
from lemongrass._backfill_tui import RaceListModel, RefineScreen
from lemongrass._laps_tui import ImportScreen, _StdoutToLines
from lemongrass._tui import _STDOUT_LOCK, LogPaneScreen
from lemongrass.race_backfill import run_backfill
from lemongrass.race_diagnose import diagnose_api, diagnose_influx
from lemongrass.races import fetch_race_rows, prune_races


def _row_label(row):
    """One checklist row: 'date  name  (#id)  N laps  <schema-status>'."""
    if row['total'] == 0:
        schema = 'no laps'
    elif row['current'] == row['total']:
        schema = f'current v{row["schema_version"]}'
    else:
        schema = f'stale {row["current"]}/{row["total"]}'
    return (f"{row['date']}  {row['name'][:32]:<32}  (#{row['race_id']})  "
            f"{row['total']:>4} laps  {schema}")


EPOCH_START = '1970-01-01T00:00:00Z'


def distinct_car_numbers(query_api, race_id):
    """Distinct car_number tag values stored for a race (cheap; not rate-limited)."""
    tables = query_api.query(
        f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "lap" and r.race_id == "{race_id}"\n'
        f'      and r._field == "lap_no")\n'
        f'  |> keep(columns: ["car_number"])\n'
        f'  |> group()\n'
        f'  |> distinct(column: "car_number")')
    return [r.get_value() for t in tables for r in t.records]


class PruneConfirmModal(ModalScreen):
    """Confirm deleting the checked races. Dismisses True on 'y', False otherwise."""

    CSS = ("PruneConfirmModal { align: center middle; } "
           "#box { width: 60; border: round $error; padding: 1; }")

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('y', 'yes', 'Delete'),
        Binding('n', 'no', 'Cancel'),
        Binding('escape', 'no', 'Cancel', show=False),
    ]

    def __init__(self, rows):
        """Store the checked rows to render in the confirmation prompt."""
        super().__init__()
        self._rows = rows

    def compose(self):
        """Render the race list and yes/no footer."""
        with Vertical(id='box'):
            yield Label(f'Delete ALL data for {len(self._rows)} race(s)?')
            for row in self._rows:
                yield Label(f'  #{row["race_id"]}  {row["name"]}')
            yield Footer()

    def action_yes(self):
        """Confirm the prune."""
        self.dismiss(True)

    def action_no(self):
        """Cancel the prune."""
        self.dismiss(False)


class DiagnoseCarScreen(Screen):
    """Pick the car to diagnose: quick-pick stored car numbers or type any number."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'app.pop_screen', 'Back'),
    ]

    def __init__(self, race_id, race_name):
        """Store the race being diagnosed."""
        super().__init__()
        self.race_id = race_id
        self.race_name = race_name

    def compose(self):
        """Render the title, quick-pick list, type-any input, and status line."""
        yield Label(f'Diagnose {self.race_name} (#{self.race_id}) — pick a car')
        yield ListView(id='cars')
        yield Input(placeholder='…or type a car number', id='car')
        yield Label('', id='status')
        yield Footer()

    def on_mount(self):
        """Load the distinct stored car numbers for this race off-thread."""
        self._load()

    @work(thread=True, exclusive=True)
    def _load(self):
        worker = get_current_worker()
        try:
            with _influx.connect() as client:
                cars = distinct_car_numbers(client.query_api(), self.race_id)
        except Exception as exc:  # surface, never crash the app
            logging.exception('car list load failed')
            if not worker.is_cancelled:
                self.app.call_from_thread(
                    self.query_one('#status', Label).update,
                    f'car list load failed: {exc}')
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._show, cars)

    def _show(self, cars):
        view = self.query_one('#cars', ListView)
        view.clear()
        for car in cars:
            view.append(ListItem(Label(f'#{car}')))
        self._cars = cars

    def on_list_view_selected(self, event):
        """Diagnose the car under the chosen quick-pick row."""
        index = self.query_one('#cars', ListView).index
        if index is not None and getattr(self, '_cars', None):
            self._go(self._cars[index])

    def on_input_submitted(self, event):
        """Diagnose the typed car number."""
        event.stop()
        self._go(event.value.strip())

    def _go(self, car_number):
        if not car_number:
            return
        if _influx.invalid_flux_ids([car_number]):
            self.query_one('#status', Label).update(f'invalid car number: {car_number!r}')
            return
        self.app.push_screen(DiagnoseOutputScreen(self.race_id, car_number))


class RacesBrowserScreen(LogPaneScreen, Screen):
    """Checklist of stored races. p=prune (checked), d=diagnose / r=re-import /
    b=backfill (highlighted). Rows load off-thread from InfluxDB."""

    CSS = "#log { height: 6; }"

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('a', 'select_all', 'All'),
        Binding('i', 'invert', 'Invert'),
        Binding('p', 'prune', 'Prune checked'),
        Binding('d', 'diagnose', 'Diagnose'),
        Binding('r', 'reimport', 'Re-import'),
        Binding('b', 'backfill', 'Backfill'),
        Binding('escape', 'back', 'Back'),
    ]

    def __init__(self):
        super().__init__()
        self._rows = []

    def action_back(self):
        """Return to the previous screen, or exit if this browser is the root.

        Opened via the Home menu there is a screen beneath to pop back to;
        opened directly (bare `races`) the only thing beneath is the app's blank
        default screen, so popping would strand the user with no bindings —
        exit cleanly instead. (screen_stack[0] is always the default screen.)"""
        if len(self.app.screen_stack) > 2:
            self.app.pop_screen()
        else:
            self.app.exit()

    def compose(self):
        """Build the screen: title, checklist, status line, log pane, footer."""
        yield Label('Stored races', id='title')
        yield SelectionList(id='races')
        yield Label('', id='status')
        yield RichLog(id='log')
        yield Footer()

    def on_mount(self):
        """Start the log drain timer, then load rows if Influx is configured."""
        self.set_interval(0.25, self._drain_log)
        if not _influx.influx_token_present():
            self.query_one('#status', Label).update(
                '⚠ Influx token not set — race data unavailable')
            return
        self._load()

    def row_for_highlight(self):
        """The row dict under the SelectionList's highlight, or None."""
        sl = self.query_one('#races', SelectionList)
        if sl.highlighted is None or not self._rows:
            return None
        return self._rows[sl.highlighted]

    def _update_status(self):
        sl = self.query_one('#races', SelectionList)
        hi = self.row_for_highlight()
        checked = len(sl.selected)
        self.query_one('#status', Label).update(
            f'highlighted: #{hi["race_id"] if hi else "-"} · checked: {checked}')

    @work(thread=True, exclusive=True)
    def _load(self):
        worker = get_current_worker()
        try:
            with _influx.connect() as client:
                rows = fetch_race_rows(client.query_api())
        except Exception as exc:  # surface, never crash the app
            logging.exception('races list load failed')
            if not worker.is_cancelled:
                self.app.call_from_thread(self._fail, str(exc))
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._show, rows)

    def _fail(self, message):
        self.query_one('#status', Label).update(f'load failed: {message}')

    def _show(self, rows):
        self._rows = rows
        sl = self.query_one('#races', SelectionList)
        sl.clear_options()
        for i, row in enumerate(rows):
            sl.add_option(Selection(_row_label(row), i, False))
        self._update_status()

    def on_selection_list_selection_toggled(self, event):
        """Refresh the status line when a row's checked state changes."""
        self._update_status()

    def on_selection_list_selection_highlighted(self, event):
        """Refresh the status line when the highlighted row changes."""
        self._update_status()

    def action_select_all(self):
        """Check every row."""
        self.query_one('#races', SelectionList).select_all()
        self._update_status()

    def action_invert(self):
        """Flip the checked state of every row."""
        sl = self.query_one('#races', SelectionList)
        for i in range(len(self._rows)):
            sl.toggle(i)
        self._update_status()

    def action_prune(self):
        """Confirm, then prune the checked races."""
        sl = self.query_one('#races', SelectionList)
        rows = [self._rows[i] for i in sl.selected]
        if not rows:
            self.app.notify('no races checked', severity='warning')
            return

        def _confirmed(ok):
            if ok:
                self._run_prune([r['race_id'] for r in rows])

        self.app.push_screen(PruneConfirmModal(rows), _confirmed)

    @work(thread=True, exclusive=True)
    def _run_prune(self, race_ids):
        worker = get_current_worker()
        try:
            with _influx.connect() as client:
                failed = prune_races(
                    client.delete_api(), race_ids,
                    on_progress=lambda m: self.app.log_handler.lines.append(m))
        except Exception as exc:  # surface, never crash the app
            logging.exception('prune failed')
            if not worker.is_cancelled:
                self.app.call_from_thread(self.app.notify, f'prune failed: {exc}',
                                           severity='error')
            return
        if worker.is_cancelled:
            return
        if failed:
            self.app.call_from_thread(
                self.app.notify, f'failed: {", ".join(failed)}', severity='error')
        self.app.call_from_thread(self._load)  # reload the list

    def action_diagnose(self):
        """Diagnose the highlighted race: pick a car, then stream diagnose output."""
        row = self.row_for_highlight()
        if row is None:
            self.app.notify('highlight a race first', severity='warning')
            return
        self.app.push_screen(DiagnoseCarScreen(row['race_id'], row['name']))

    def action_reimport(self):
        """Re-import the highlighted race via the laps ImportScreen."""
        row = self.row_for_highlight()
        if row is None:
            self.app.notify('highlight a race first', severity='warning')
            return
        self.app.push_screen(ImportScreen(self.app.client, row['race_id'], row['name']))

    def action_backfill(self):
        """Discover races by term in RefineScreen, then backfill the chosen set.

        Discovery starts from an empty selection; the user searches terms in the
        RefineScreen exactly as `races backfill` does. start_epoc=0 shows all.
        """
        model = RaceListModel([], {}, 0)
        refine = RefineScreen(self.app.client, model)

        def _picked(result):
            if result is not None and result.races:
                self.app.push_screen(BackfillRunScreen(result.races))

        self.app.push_screen(refine, _picked)


class DiagnoseOutputScreen(LogPaneScreen, Screen):
    """Run diagnose_api + diagnose_influx for one race/car, streaming their print()
    output into a scrollable log pane."""

    CSS = "#log { height: 1fr; }"

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('escape', 'app.pop_screen', 'Back'),
        Binding('q', 'app.pop_screen', 'Back', show=False),
    ]

    def __init__(self, race_id, car_number):
        """Store the race/car being diagnosed."""
        super().__init__()
        self.race_id = race_id
        self.car_number = car_number

    def compose(self):
        """Render the title, log pane, and footer."""
        yield Label(f'Diagnose #{self.race_id} car {self.car_number}', id='title')
        yield RichLog(id='log')
        yield Footer()

    def on_mount(self):
        """Start the log drain timer, then kick off the diagnose worker."""
        self.set_interval(0.25, self._drain_log)
        self._run()

    @work(thread=True, exclusive=True)
    def _run(self):
        worker = get_current_worker()
        writer = _StdoutToLines(self.app.log_handler.lines)
        try:
            with _STDOUT_LOCK, contextlib.redirect_stdout(writer):
                start_epoc, end_epoc = diagnose_api(
                    self.app.client, self.race_id, self.car_number)
                with _influx.connect() as client:
                    diagnose_influx(client.query_api(), self.race_id,
                                     self.car_number, start_epoc, end_epoc)
        except Exception as exc:
            logging.exception('diagnose failed')
            self.app.log_handler.lines.append(f'diagnose failed: {exc}')
        finally:
            writer.flush()
        if not worker.is_cancelled:
            self.app.call_from_thread(
                self.query_one('#title', Label).update,
                f'Diagnose #{self.race_id} car {self.car_number} — done')


class BackfillRunScreen(LogPaneScreen, Screen):
    """Import a chosen set of races via run_backfill, streaming its output."""

    CSS = "#log { height: 1fr; }"

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('q', 'app.pop_screen', 'Back'),
        Binding('escape', 'app.pop_screen', 'Back', show=False),
    ]

    def __init__(self, races):
        """Store the races to backfill."""
        super().__init__()
        self._races = races

    def compose(self):
        """Render the title, log pane, and footer."""
        yield Label(f'Backfilling {len(self._races)} race(s)…', id='title')
        yield RichLog(id='log')
        yield Footer()

    def on_mount(self):
        """Start the log drain timer, then kick off the backfill worker."""
        self.set_interval(0.25, self._drain_log)
        self._run()

    @work(thread=True, exclusive=True)
    def _run(self):
        worker = get_current_worker()
        writer = _StdoutToLines(self.app.log_handler.lines)
        failures = None
        crashed = False
        try:
            with _STDOUT_LOCK, contextlib.redirect_stdout(writer):
                failures = run_backfill(self._races, dry_run=False, force=False)
        except Exception as exc:
            crashed = True
            logging.exception('backfill run failed')
            self.app.log_handler.lines.append(f'backfill failed: {exc}')
        finally:
            writer.flush()
        if not worker.is_cancelled:
            if crashed:
                msg = 'Backfill failed — see log.'
            elif failures:
                msg = f'Backfill finished with {len(failures)} failure(s).'
            else:
                msg = 'Backfill complete.'
            self.app.call_from_thread(self.query_one('#title', Label).update, msg)
