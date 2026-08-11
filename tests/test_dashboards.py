"""Run the local-testing dashboards' SQL against a real schema.

The dashboard JSON is the only place these queries exist, and nothing else
type-checks them. A query that silently renders the wrong thing -- a NULL
timestamp as 1970, say -- is invisible until someone looks at Grafana during a
race, so the ones with a NULL-handling decision in them are exercised here.
"""
import json
import pathlib

from sqlalchemy import text

DASHBOARD = (pathlib.Path(__file__).resolve().parents[1]
             / 'local-testing/grafana/provisioning/dashboards/json/laps.json')


def _panels(node):
    """Yield every panel in the dashboard, descending into row panels."""
    for panel in node.get('panels', []):
        yield panel
        yield from _panels(panel)


def _session_label_sql():
    """The session-title panel's query, with Grafana's macros filled in."""
    for panel in _panels(json.loads(DASHBOARD.read_text())):
        for target in panel.get('targets', []):
            sql = target.get('rawSql', '')
            if 'FROM sessions' in sql and 'AS session' in sql:
                return (sql.replace('${raceid:sqlstring}', "'42'")
                           .replace('${session:regex}', '7'))
    raise AssertionError('no session-title panel query found in the dashboard')


def test_a_session_with_no_start_time_renders_without_a_date(db):
    # The live monitor learns a session's id before its start time and writes
    # the row with start_time NULL, which is why the column is nullable at all
    # ("NULL renders as an honest blank in the picker rather than 1970").
    # Coalescing that NULL to an epoch in the panel puts 1970 back on screen
    # for the whole of every live race.
    with db.begin() as conn:
        conn.execute(text(
            "INSERT INTO races (race_id, race_time) VALUES ('42', now())"))
        conn.execute(text(
            "INSERT INTO sessions (session_id, race_id, name) "
            "VALUES (7, '42', 'Race Session')"))
        label = conn.execute(text(_session_label_sql())).scalar()
    assert label == 'Race Session'


def test_a_session_with_a_start_time_still_renders_its_date(db):
    with db.begin() as conn:
        conn.execute(text(
            "INSERT INTO races (race_id, race_time) VALUES ('42', now())"))
        conn.execute(text(
            "INSERT INTO sessions (session_id, race_id, name, start_time) "
            "VALUES (7, '42', 'Race Session', '2026-05-03T14:00:00Z')"))
        label = conn.execute(text(_session_label_sql())).scalar()
    assert label == 'Race Session (5/3/26)'


YEAR_OVER_YEAR = (pathlib.Path(__file__).resolve().parents[1]
                  / 'local-testing/grafana/provisioning/dashboards/json'
                  / 'year-over-year.json')


def _variable_sql(name, **subs):
    """The rawSql of a named template variable, with Grafana's macros filled in.

    subs maps a variable name to the literal SQL text that Grafana's
    ${name:sqlstring} interpolation would produce -- quotes included, since
    that is what sqlstring supplies.
    """
    for var in json.loads(YEAR_OVER_YEAR.read_text())['templating']['list']:
        if var['name'] == name:
            sql = var['query']['rawSql']
            for token, value in subs.items():
                sql = sql.replace('${' + token + ':sqlstring}', value)
            return sql
    raise AssertionError(f'no template variable named {name!r} in the dashboard')


def test_the_event_picker_query_executes(db):
    # The obvious way to sort the 'all' sentinel first -- ORDER BY 1 = 'all'
    # DESC, 2 -- does not work after a UNION ALL: the bare 1 is an integer
    # literal inside a larger expression, not an ordinal, and Postgres rejects
    # it with "invalid input syntax for type integer". The failure is total, so
    # merely executing the query is the assertion that matters.
    with db.begin() as conn:
        rows = conn.execute(text(_variable_sql('event', venue="'thompson'"))).fetchall()
    assert rows == [('all', 'All events')]


def test_the_event_picker_sorts_the_all_sentinel_first(db):
    with db.begin() as conn:
        conn.execute(text("INSERT INTO venues (venue_id, name) VALUES ('thompson', 'Thompson')"))
        conn.execute(text(
            "INSERT INTO events (event_id, series_id, name) VALUES ('aaa', 1, 'AAA Event')"))
        conn.execute(text(
            "INSERT INTO races (race_id, race_time, venue_id, event_id) "
            "VALUES ('1', now(), 'thompson', 'aaa')"))
        rows = conn.execute(text(_variable_sql('event', venue="'thompson'"))).fetchall()
    # 'AAA Event' sorts before 'All events' alphabetically, so a plain
    # ORDER BY __text would bury the sentinel.
    assert rows == [('all', 'All events'), ('aaa', 'AAA Event')]
