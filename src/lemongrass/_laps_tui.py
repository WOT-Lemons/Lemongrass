"""Interactive laps TUI: find a race, then watch it live or import it.

LapBoardModel holds the dashboard's derived state (lap rows, leaderboard rows)
with no Textual imports, so the rendering logic is unit-testable. The screens
below drive it from a background worker via _TuiObserver.
"""


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
