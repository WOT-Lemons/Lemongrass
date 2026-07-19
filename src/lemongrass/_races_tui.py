"""Interactive races browser: list stored races, then prune / diagnose / backfill /
re-import them. The race table (the `list` view) is the hub; keys act on the checked
batch (prune) or the highlighted row (diagnose / re-import)."""

import logging
from typing import ClassVar

from textual import work
from textual.binding import Binding, BindingType
from textual.containers import Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import Footer, Label, RichLog, SelectionList
from textual.widgets.selection_list import Selection
from textual.worker import get_current_worker

from lemongrass import _influx
from lemongrass._tui import LogPaneScreen
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
        Binding('escape', 'app.pop_screen', 'Back'),
    ]

    def __init__(self):
        super().__init__()
        self._rows = []

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
        """Diagnose the highlighted race. Stub — implemented in Task 6."""

    def action_reimport(self):
        """Re-import the highlighted race. Stub — implemented in Task 11."""

    def action_backfill(self):
        """Backfill the highlighted race. Stub — implemented in Task 11."""
