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


def _venue_with_two_races(conn):
    """One venue, two races a year apart, two teams, one shared car number.

    The shared number is the point: our team ran 252 at the first race and 253
    at the second, and the other team ran 253 at the first. A filter that
    collects race ids and car numbers separately matches that other car.
    """
    conn.execute(text("INSERT INTO venues (venue_id, name) VALUES ('thompson', 'Thompson')"))
    conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('wot', 'WOT Lemons')"))
    conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('other', 'Other Team')"))
    conn.execute(text(
        "INSERT INTO races (race_id, race_time, venue_id) VALUES "
        "('1', '2024-06-01T12:00:00Z', 'thompson'), "
        "('2', '2025-06-01T12:00:00Z', 'thompson')"))
    conn.execute(text(
        "INSERT INTO entries (race_id, car_number, team_id) VALUES "
        "('1', '252', 'wot'), ('2', '253', 'wot'), ('1', '253', 'other')"))


def test_the_pair_predicate_keeps_pairs_paired(db):
    with db.begin() as conn:
        _venue_with_two_races(conn)
        predicate = conn.execute(text(_variable_sql(
            'pairs', team="'wot'", venue="'thompson'", event="'all'"))).scalar()
    # Our two cars, each bound to its own race.
    assert '(r.race_id == "1" and r.car_number == "252")' in predicate
    assert '(r.race_id == "2" and r.car_number == "253")' in predicate
    # And never the other team's car 253 at race 1.
    assert '(r.race_id == "1" and r.car_number == "253")' not in predicate


def test_the_pair_predicate_emits_flux_double_quotes(db):
    # Flux has no single-quoted string literal, so %L (which quotes with single
    # quotes) produces source that fails to compile. Nothing downstream would
    # report this as anything but a broken panel.
    with db.begin() as conn:
        _venue_with_two_races(conn)
        predicate = conn.execute(text(_variable_sql(
            'pairs', team="'wot'", venue="'thompson'", event="'all'"))).scalar()
    assert "'" not in predicate


def test_an_empty_selection_matches_nothing_rather_than_everything(db):
    # The failure mode this guards: an empty predicate, or an unanchored empty
    # regex, matches every lap in the bucket -- so a venue with no resolved
    # entries renders every venue's data under one venue's name.
    with db.begin() as conn:
        _venue_with_two_races(conn)
        predicate = conn.execute(text(_variable_sql(
            'pairs', team="'nobody'", venue="'thompson'", event="'all'"))).scalar()
    assert predicate == 'false'


def test_the_year_map_is_a_flux_list_of_utc_years(db):
    with db.begin() as conn:
        _venue_with_two_races(conn)
        yearmap = conn.execute(text(_variable_sql(
            'yearmap', venue="'thompson'", event="'all'"))).scalar()
    assert '{key: "1", value: "2024"}' in yearmap
    assert '{key: "2", value: "2025"}' in yearmap
    assert yearmap.startswith('[') and yearmap.endswith(']')


def test_the_maps_fall_back_to_a_typed_sentinel_row(db):
    # dict.fromList(pairs: []) has no types to infer and fails the whole query
    # with a 500 ("invalid key nature: invalid") -- not an empty panel. A typed
    # sentinel row returns empty cleanly instead.
    with db.begin() as conn:
        _venue_with_two_races(conn)
        for name in ('yearmap', 'racemap'):
            emitted = conn.execute(text(_variable_sql(
                name, venue="'no-such-venue'", event="'all'"))).scalar()
            assert emitted == '[{key: "", value: ""}]', name


def test_the_race_map_strips_embedded_double_quotes_from_names(db):
    # races.name is free text. Without the replace(...) strip, a race named
    # The "Big" One closes the Flux string literal early and breaks every
    # panel that interpolates racemap -- the same failure mode the pairs
    # predicate now guards against for car_number.
    with db.begin() as conn:
        conn.execute(text("INSERT INTO venues (venue_id, name) VALUES ('thompson', 'Thompson')"))
        conn.execute(text(
            "INSERT INTO races (race_id, race_time, venue_id, name) VALUES "
            "('1', '2024-06-01T12:00:00Z', 'thompson', 'The \"Big\" One')"))
        racemap = conn.execute(text(_variable_sql(
            'racemap', venue="'thompson'", event="'all'"))).scalar()
    assert '{key: "1", value: "The Big One"}' in racemap
    assert racemap.startswith('[') and racemap.endswith(']')
    assert racemap.count('"') == 4  # exactly the key/value quote pairs, none stray


def test_the_pair_predicate_strips_embedded_double_quotes_from_car_number(db):
    # entries.car_number has no CHECK constraint and reaches the DB via a bare
    # CLI arg with only .strip() applied -- "digit strings" is not enforced.
    # A stray quote in car_number would close the Flux string literal early,
    # the same failure mode racemap guards against for race names.
    with db.begin() as conn:
        conn.execute(text("INSERT INTO venues (venue_id, name) VALUES ('thompson', 'Thompson')"))
        conn.execute(text("INSERT INTO teams (team_id, name) VALUES ('wot', 'WOT Lemons')"))
        conn.execute(text(
            "INSERT INTO races (race_id, race_time, venue_id) VALUES "
            "('1', '2024-06-01T12:00:00Z', 'thompson')"))
        conn.execute(text(
            "INSERT INTO entries (race_id, car_number, team_id) VALUES "
            "('1', '25\"2', 'wot')"))
        predicate = conn.execute(text(_variable_sql(
            'pairs', team="'wot'", venue="'thompson'", event="'all'"))).scalar()
    assert '(r.race_id == "1" and r.car_number == "252")' in predicate
    assert "'" not in predicate


def test_the_best_lap_tile_links_into_the_per_race_dashboard():
    # The link is the only drill-down from an aggregate back to a single race.
    # laps.json's uid and its race variable name are what make it resolve; a
    # typo here degrades silently into a link that opens an unfiltered board.
    for panel in _panels(json.loads(YEAR_OVER_YEAR.read_text())):
        if panel.get('title') == 'Best lap ever':
            links = panel['fieldConfig']['defaults']['links']
            assert len(links) == 1
            assert 'SI7eTlIMk' in links[0]['url']
            assert 'var-raceid=${__data.fields.race_id}' in links[0]['url']
            return
    raise AssertionError('no "Best lap ever" panel found in the dashboard')
