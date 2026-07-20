"""Top-level Lemongrass TUI: a Home menu routing to the Laps flow and the Races
browser, all as Screens under one app. `telem` and `pisugar-monitor` stay CLI-only.
"""

from typing import ClassVar

from textual.app import App
from textual.binding import Binding, BindingType
from textual.screen import Screen
from textual.widgets import Footer, Label, ListItem, ListView

from lemongrass._laps_tui import LapsFlowMixin
from lemongrass._races_tui import RacesBrowserScreen
from lemongrass._tui import _routed_output


class HomeScreen(Screen):
    """Menu: Laps or Races."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('q', 'app.quit', 'Quit', show=False),
    ]

    def compose(self):
        """Render the title, the menu list, and the footer."""
        yield Label('lemongrass — choose a tool')
        yield ListView(
            ListItem(Label('Laps — find & monitor/import a race'), id='laps'),
            ListItem(Label('Races — list, prune, diagnose, backfill'), id='races'),
            id='menu')
        yield Footer()

    def on_mount(self):
        """Focus the menu so arrow keys/enter work immediately."""
        self.query_one('#menu', ListView).focus()

    def on_list_view_selected(self, event):
        """Route to the laps flow or the races browser based on the picked item."""
        if event.item.id == 'laps':
            self.app.start_laps()
        elif event.item.id == 'races':
            self.app.open_races()


class LemongrassApp(LapsFlowMixin, App):
    """Root app: opens on Home, routes to the laps flow or the races browser."""

    def __init__(self, client, start_screen=None, exit_on_start_dismiss=False):
        """Store the shared RaceMonitorClient and set up the laps flow state.

        start_screen, if given, is pushed on mount instead of the Home menu.
        exit_on_start_dismiss mirrors the seeded screen's dismissal to the app's
        exit value (used by refine_races to get a RefineResult back from app.run())."""
        super().__init__()
        self._init_laps_flow(client)
        self._start_screen = start_screen
        self._exit_on_start_dismiss = exit_on_start_dismiss

    def on_mount(self):
        """Open on start_screen if given, else the Home menu."""
        if self._start_screen is not None:
            self.push_screen(
                self._start_screen,
                callback=self.exit if self._exit_on_start_dismiss else None)
        else:
            self.push_screen(HomeScreen())

    def open_races(self):
        """Enter the races browser."""
        self.push_screen(RacesBrowserScreen())


def run_home_tui(client):
    """Run the top-level TUI against an already-open RaceMonitorClient.

    Root logging is routed into per-screen log sinks (via _routed_output()) for
    the app's lifetime and restored afterwards. Returns 0.
    """
    app = LemongrassApp(client)
    with _routed_output():
        app.run()
    return 0
