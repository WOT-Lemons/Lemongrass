"""PostgreSQL engine and connection handling for lemongrass.

Mirrors the shape of ``_influx``: settings come from the config layer, the
secret is read from the env var the config names, and nothing connects until a
caller asks. Unlike ``_influx`` the engine is built lazily rather than at import
— every command imports the CLI, and a command that never touches the database
must not require a password to be set.

All SQL statements belong in this module. Keeping that boundary is what would
make a later move to the SQLAlchemy ORM a one-module change.
"""
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from lemongrass import _config, _schema

_engine = None


def database_url():
    """Build the SQLAlchemy URL from config plus the configured password env var.

    Logs an error and exits with status 1 when the password variable is unset,
    matching ``_influx.connect``'s handling of a missing token.
    """
    from sqlalchemy import URL
    cfg = _config.load_config().postgres
    password = os.environ.get(cfg.password_env)
    if not password:
        logging.error("%s environment variable not set", cfg.password_env)
        sys.exit(1)
    return URL.create(
        'postgresql+psycopg',
        username=cfg.user,
        password=password,
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
    )


def engine():
    """Return the process-wide Engine, creating it on first use.

    ``pool_pre_ping`` is on because the monitor runs for hours: an idle pooled
    connection reaped by the server or a NAT timeout becomes a transparent
    reconnect instead of a mid-race exception.
    """
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(database_url(), pool_pre_ping=True)
    return _engine


def reset_engine():
    """Dispose and forget the memoized engine (tests, and config changes)."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


@contextmanager
def connect():
    """Yield a Connection inside a transaction, committing on clean exit.

    Rolls back if the body raises, so a partial write is never left behind.
    """
    with engine().begin() as conn:
        yield conn


def db_password_present():
    """True iff the configured database password env var is set.

    Cheap check (no connection, no sys.exit) so a TUI can validate up front and
    surface a clean error instead of a worker-thread SystemExit.
    """
    return bool(os.environ.get(_config.load_config().postgres.password_env))


def alembic_config(url=None):
    """Build an Alembic Config pointed at the migrations shipped in this package.

    Resolving ``script_location`` through the package directory is what lets
    ``lemongrass db upgrade`` work from an installed wheel or a container, with
    no checkout and no alembic.ini on disk.
    """
    from pathlib import Path

    from alembic.config import Config
    # Config's `stdout` parameter defaults to `sys.stdout` bound once, at the
    # time alembic.config is first imported — not at each call. Pass it
    # explicitly so output goes to whatever sys.stdout currently is (tests
    # rely on this to land in capsys's per-test buffer).
    cfg = Config(stdout=sys.stdout)
    cfg.set_main_option('script_location',
                        str(Path(__file__).parent / 'migrations'))
    if url is None:
        url = database_url()
    # A URL object hides the password in str(); a caller-supplied string is
    # already rendered. Accept either so tests can pass a plain URL string.
    if hasattr(url, 'render_as_string'):
        url = url.render_as_string(hide_password=False)
    # set_main_option writes into a ConfigParser, which reads a bare '%' as
    # interpolation syntax and raises. render_as_string percent-encodes every
    # reserved character in the password, so any password containing '@', '%',
    # '/' or ':' arrives here already full of them. '%%' is the escape and
    # reads back as a single '%', so the URL round-trips unchanged.
    cfg.set_main_option('sqlalchemy.url', str(url).replace('%', '%%'))
    return cfg


@dataclass
class RaceRow:
    """One row of the races table, in the shape callers pass and receive.

    race_time is required and timezone-aware; the column is NOT NULL. Every
    other attribute defaults, because the live-monitor write path knows only
    identity and timing — completeness fields arrive from a backfill.
    """

    race_id: str
    race_time: datetime
    name: str = ''
    track_name: str = ''
    series_id: int | None = None
    series_name: str | None = None
    end_time: datetime | None = None
    expected_lap_count: int | None = None
    session_count: int | None = None
    lap_schema_version: int | None = None
    # Curated identity, resolved by _tracks.resolve. All three are NULL when
    # the track name matched no curated venue.
    venue_id: str | None = None
    layout_id: str | None = None
    event_id: str | None = None


@contextmanager
def connection(conn=None):
    """Yield the caller's connection, or open a fresh transaction.

    Every statement helper takes an optional ``conn`` so a caller can compose
    several into one transaction; when it is omitted the helper is
    self-contained.
    """
    if conn is not None:
        yield conn
    else:
        with connect() as own:
            yield own


def _race_row(row):
    """Build a RaceRow from a result row."""
    return RaceRow(
        race_id=row.race_id,
        race_time=row.race_time,
        name=row.name,
        track_name=row.track_name,
        series_id=row.series_id,
        series_name=row.series_name,
        end_time=row.end_time,
        expected_lap_count=row.expected_lap_count,
        session_count=row.session_count,
        lap_schema_version=row.lap_schema_version,
        venue_id=row.venue_id,
        layout_id=row.layout_id,
        event_id=row.event_id,
    )


def upsert_race(row, conn=None):
    """Insert or update one race by primary key, in a single statement.

    The conflict-update list is explicit rather than "set everything": the
    live-monitor path writes no expected_lap_count, session_count, or
    lap_schema_version, so those three COALESCE against the stored value.
    Blanket EXCLUDED would erase a backfilled race's completeness on the very
    next live poll, and the backfill would then redo the race under the
    6 req/min limit.

    The identity columns (name, track_name, series_id, series_name, end_time)
    are protected the same way: a race.details fetch that failed produces a
    blank name/track_name and a None series_id/series_name/end_time (see
    ``_resolve_race_metadata``), and Postgres — unlike Influx before it — is
    the system of record these columns are read back from, with no fallback.
    A blank/None EXCLUDED value falls back to the stored value via NULLIF/
    COALESCE, so a genuinely new race with a failed details fetch still gets
    a row, and a later successful fetch's non-blank values still win.
    ``race_time`` keeps blanket EXCLUDED — it is NOT NULL and always
    genuinely supplied.

    venue_id and event_id COALESCE for the same reason as the text columns
    above: a failed details fetch resolves to all-None and must not erase a
    good tag. layout_id does NOT — it is a CASE keyed to the incoming
    venue_id, for the reason spelled out where it is built below. Clearing or
    correcting a tag downward is `races identify`'s job, which issues an
    explicit UPDATE.
    """
    from sqlalchemy import case, func
    from sqlalchemy.dialects.postgresql import insert
    stmt = insert(_schema.races).values(
        race_id=row.race_id,
        name=row.name or '',
        track_name=row.track_name or '',
        series_id=row.series_id,
        series_name=row.series_name,
        race_time=row.race_time,
        end_time=row.end_time,
        expected_lap_count=row.expected_lap_count,
        session_count=row.session_count,
        lap_schema_version=row.lap_schema_version,
        venue_id=row.venue_id,
        layout_id=row.layout_id,
        event_id=row.event_id,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[_schema.races.c.race_id],
        set_={
            'name': func.coalesce(
                func.nullif(stmt.excluded.name, ''), _schema.races.c.name),
            'track_name': func.coalesce(
                func.nullif(stmt.excluded.track_name, ''), _schema.races.c.track_name),
            'series_id': func.coalesce(
                stmt.excluded.series_id, _schema.races.c.series_id),
            'series_name': func.coalesce(
                stmt.excluded.series_name, _schema.races.c.series_name),
            'race_time': stmt.excluded.race_time,
            'end_time': func.coalesce(
                stmt.excluded.end_time, _schema.races.c.end_time),
            'expected_lap_count': func.coalesce(
                stmt.excluded.expected_lap_count,
                _schema.races.c.expected_lap_count),
            'session_count': func.coalesce(
                stmt.excluded.session_count, _schema.races.c.session_count),
            'lap_schema_version': func.coalesce(
                stmt.excluded.lap_schema_version,
                _schema.races.c.lap_schema_version),
            'venue_id': func.coalesce(
                stmt.excluded.venue_id, _schema.races.c.venue_id),
            # layout_id must follow whichever source venue_id came from — a
            # per-column COALESCE could pair a newly-resolved venue with the
            # PREVIOUS write's layout_id, a combination that was never
            # resolved together and can violate the composite FK (or worse,
            # silently match an unrelated same-named layout at the new
            # venue). A blank write (excluded.venue_id IS NULL) still keeps
            # the stored layout, same as before.
            'layout_id': case(
                (stmt.excluded.venue_id.isnot(None), stmt.excluded.layout_id),
                else_=_schema.races.c.layout_id,
            ),
            'event_id': func.coalesce(
                stmt.excluded.event_id, _schema.races.c.event_id),
            'updated_at': func.now(),
        },
    )
    with connection(conn) as c:
        c.execute(stmt)


def get_race(race_id, conn=None):
    """Return the RaceRow for race_id, or None when it is not stored."""
    from sqlalchemy import select
    with connection(conn) as c:
        row = c.execute(
            select(_schema.races).where(_schema.races.c.race_id == race_id)
        ).first()
    return _race_row(row) if row is not None else None


def list_races(conn=None):
    """Return every stored race, newest race_time first.

    race_id breaks ties so the order is total and stable — export_legacy writes
    in this order, and two races sharing a race_time would otherwise shuffle
    between runs.
    """
    from sqlalchemy import select
    with connection(conn) as c:
        rows = c.execute(
            select(_schema.races).order_by(_schema.races.c.race_time.desc(),
                                           _schema.races.c.race_id)
        ).all()
    return [_race_row(r) for r in rows]


@dataclass
class RaceListRow:
    """A race as the listing surfaces need it: identity plus joined names.

    Deliberately not RaceRow: venue_name and event_name are joined, not
    writable columns, and RaceRow is the shape upsert_race accepts.
    """

    race_id: str
    name: str
    race_time: datetime
    venue_name: str | None = None
    event_name: str | None = None


def list_races_with_venue(conn=None):
    """Return every race with its venue and event names, newest first.

    Outer joins, so an unresolved race still appears with both names NULL. One
    query, not a round trip per race.
    """
    from sqlalchemy import select
    stmt = (
        select(_schema.races.c.race_id, _schema.races.c.name,
               _schema.races.c.race_time,
               _schema.venues.c.name.label('venue_name'),
               _schema.events.c.name.label('event_name'))
        .select_from(
            _schema.races
            .outerjoin(_schema.venues,
                       _schema.races.c.venue_id == _schema.venues.c.venue_id)
            .outerjoin(_schema.events,
                       _schema.races.c.event_id == _schema.events.c.event_id))
        .order_by(_schema.races.c.race_time.desc(), _schema.races.c.race_id))
    with connection(conn) as c:
        rows = c.execute(stmt).all()
    return [RaceListRow(race_id=r.race_id, name=r.name, race_time=r.race_time,
                        venue_name=r.venue_name, event_name=r.event_name)
            for r in rows]


def delete_race(race_id, conn=None):
    """Delete one race, cascading to its sessions. True if a row was removed."""
    from sqlalchemy import delete
    with connection(conn) as c:
        result = c.execute(
            delete(_schema.races).where(_schema.races.c.race_id == race_id))
    return result.rowcount > 0


@dataclass
class SessionRow:
    """One row of the sessions table.

    session_id is RaceMonitor's own integer identifier and is the primary key
    on its own: the live path (client.live.get_session -> session['ID']) and
    the backfill path (results.session_details -> Session['ID'], itself sourced
    from results.sessions_for_race's session ids) both name sessions with the
    same 'ID' field from the same RaceMonitor API family, for the same race —
    one id space. The old Influx session writer's session_id-only delete
    predicate already assumed this. start_time is nullable — the live path
    learns a session's id before its start time, and NULL beats storing 1970.
    """

    session_id: int
    race_id: str
    name: str = ''
    start_time: datetime | None = None


def _session_row(row):
    """Build a SessionRow from a result row."""
    return SessionRow(session_id=row.session_id, race_id=row.race_id,
                      name=row.name, start_time=row.start_time)


def _session_upsert(row):
    """Build the insert-or-update statement for one session.

    The conflict update is conditional on the stored race_id. session_id is
    the primary key on the assumption that RaceMonitor mints ids globally, but
    nothing enforces that and the legacy import writes whatever Influx held.
    A row another race already owns satisfies neither the insert nor the
    update, so the statement touches no rows and ``_write_session`` raises —
    rather than moving the session off that race's picker with no error.

    The condition lives in the statement instead of a preflight SELECT so a
    concurrent writer cannot slip in between the check and the write.
    """
    from sqlalchemy import func
    from sqlalchemy.dialects.postgresql import insert
    stmt = insert(_schema.sessions).values(
        session_id=row.session_id,
        race_id=row.race_id,
        name=row.name or '',
        start_time=row.start_time,
    )
    return stmt.on_conflict_do_update(
        index_elements=[_schema.sessions.c.session_id],
        set_={
            'race_id': stmt.excluded.race_id,
            'name': stmt.excluded.name,
            'start_time': stmt.excluded.start_time,
            'updated_at': func.now(),
        },
        where=_schema.sessions.c.race_id == row.race_id,
    ).returning(_schema.sessions.c.session_id)


def _write_session(c, row):
    """Run one guarded session upsert, raising if another race owns the id.

    A filtered-out conflict update writes nothing and returns nothing, which
    is why the statement carries a RETURNING clause: rowcount is -1 for this
    insert, so an empty result is the only reliable signal. The owning race is
    looked up afterwards — on the error path only — to name it in the message.
    """
    if c.execute(_session_upsert(row)).first() is not None:
        return
    from sqlalchemy import select
    owner = c.execute(
        select(_schema.sessions.c.race_id)
        .where(_schema.sessions.c.session_id == row.session_id)).scalar()
    raise ValueError(
        f"session {row.session_id} belongs to race {owner!r}; refusing to "
        f"reassign it to {row.race_id!r}")


def upsert_session(row, conn=None):
    """Insert or update one session by primary key.

    The live monitor's per-session write. Backfill uses replace_sessions.
    """
    with connection(conn) as c:
        _write_session(c, row)


def replace_sessions(race_id, rows, conn=None):
    """Make the stored sessions for race_id exactly `rows`, in one transaction.

    Upsert-only is not enough: a session that dedupe collapsed away would
    linger forever and keep appearing in the dashboard's session picker — the
    duplicate-session symptom this project exists to stop reintroducing. The
    delete is scoped to this race, so the live monitor's sessions for other
    races are untouched. One transaction means a failed row leaves the
    previous set intact for the next backfill to redo.

    A session id already owned by a different race raises ValueError from
    _write_session rather than being reassigned, and the transaction rolls the
    whole rewrite back.
    """
    from sqlalchemy import delete
    with connection(conn) as c:
        for row in rows:
            _write_session(c, row)
        stmt = delete(_schema.sessions).where(
            _schema.sessions.c.race_id == race_id)
        keep = [r.session_id for r in rows]
        if keep:
            stmt = stmt.where(_schema.sessions.c.session_id.notin_(keep))
        c.execute(stmt)


def list_sessions(race_id=None, conn=None):
    """Return sessions for one race, or every stored session.

    Ordered by start_time with NULLs last, then session_id, so the ordering is
    total and stable for the dashboard picker and for export.
    """
    from sqlalchemy import select
    stmt = select(_schema.sessions)
    if race_id is not None:
        stmt = stmt.where(_schema.sessions.c.race_id == race_id)
    stmt = stmt.order_by(_schema.sessions.c.start_time.nulls_last(),
                         _schema.sessions.c.session_id)
    with connection(conn) as c:
        rows = c.execute(stmt).all()
    return [_session_row(r) for r in rows]


def sync_tracks(data, dry_run=False, conn=None):
    """Make the venue/layout/event tables agree with the curated track data.

    Upsert-only, deliberately: a venue removed from the file may still be
    referenced by stored races, and silently breaking that is worse than a
    stale row. Rows with no file counterpart are reported instead, for manual
    handling. (Consequently layouts' ON DELETE CASCADE never fires in practice
    — fk_races_venue_id_layout_id_layouts is NO ACTION and would block the
    delete anyway.)

    ``data`` is a ``_tracks.TrackData``; it is read structurally so this module
    keeps its SQL-only role and takes no import on the curated-data layer.
    Returns a summary of what changed (or would change, under dry_run).
    """
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert
    summary = {
        'venues_created': 0, 'venues_updated': 0,
        'layouts_created': 0, 'layouts_updated': 0,
        'events_created': 0, 'events_updated': 0,
        'orphan_venues': [], 'orphan_layouts': [], 'orphan_events': [],
    }
    with connection(conn) as c:
        stored_venues = {r.venue_id: r.name
                         for r in c.execute(select(_schema.venues)).all()}
        stored_layouts = {(r.venue_id, r.layout_id): r.name
                          for r in c.execute(select(_schema.layouts)).all()}
        stored_events = {r.event_id: (r.series_id, r.name)
                         for r in c.execute(select(_schema.events)).all()}

        file_venues, file_layouts, file_events = set(), set(), set()
        for venue in data.venues:
            file_venues.add(venue.venue_id)
            if venue.venue_id not in stored_venues:
                summary['venues_created'] += 1
            elif stored_venues[venue.venue_id] != venue.name:
                summary['venues_updated'] += 1
            if not dry_run:
                stmt = insert(_schema.venues).values(
                    venue_id=venue.venue_id, name=venue.name)
                c.execute(stmt.on_conflict_do_update(
                    index_elements=[_schema.venues.c.venue_id],
                    set_={'name': stmt.excluded.name}))
            for layout in venue.layouts:
                key = (venue.venue_id, layout.layout_id)
                file_layouts.add(key)
                if key not in stored_layouts:
                    summary['layouts_created'] += 1
                elif stored_layouts[key] != layout.name:
                    summary['layouts_updated'] += 1
                if not dry_run:
                    stmt = insert(_schema.layouts).values(
                        venue_id=venue.venue_id, layout_id=layout.layout_id,
                        name=layout.name)
                    c.execute(stmt.on_conflict_do_update(
                        index_elements=[_schema.layouts.c.venue_id,
                                        _schema.layouts.c.layout_id],
                        set_={'name': stmt.excluded.name}))

        for series in data.series:
            for event in series.events:
                file_events.add(event.event_id)
                current = (event.series_id, event.name)
                if event.event_id not in stored_events:
                    summary['events_created'] += 1
                elif stored_events[event.event_id] != current:
                    summary['events_updated'] += 1
                if not dry_run:
                    stmt = insert(_schema.events).values(
                        event_id=event.event_id, series_id=event.series_id,
                        name=event.name)
                    c.execute(stmt.on_conflict_do_update(
                        index_elements=[_schema.events.c.event_id],
                        set_={'series_id': stmt.excluded.series_id,
                              'name': stmt.excluded.name}))

    summary['orphan_venues'] = sorted(set(stored_venues) - file_venues)
    summary['orphan_layouts'] = sorted(set(stored_layouts) - file_layouts)
    summary['orphan_events'] = sorted(set(stored_events) - file_events)
    return summary


def set_race_identity(race_id, venue_id, layout_id, event_id, conn=None):
    """Set one race's three identity columns. True if the race exists.

    An explicit UPDATE rather than an upsert, so `races identify` can also
    clear a tag back to NULL after a tracks.toml correction — which the
    COALESCE in upsert_race deliberately cannot do.
    """
    from sqlalchemy import func, update
    with connection(conn) as c:
        result = c.execute(
            update(_schema.races)
            .where(_schema.races.c.race_id == race_id)
            .values(venue_id=venue_id, layout_id=layout_id, event_id=event_id,
                    updated_at=func.now()))
    return result.rowcount > 0


@dataclass
class TeamRow:
    """One row of the teams table."""

    team_id: str
    name: str


def upsert_team(team_id, name, conn=None):
    """Insert a team, or rename an existing one.

    `teams add` on an existing id is a rename rather than an error: the team
    name has changed before and will again, and the id is what entries point at.

    The name must normalize to something, for the same reason add_team_alias
    requires it: merge_teams records the source team's name as an alias of the
    target, and a name that normalizes to nothing would make that guarantee
    silently not happen — no alias written and no warning either, since there
    is no conflicting owner to report.
    """
    from sqlalchemy.dialects.postgresql import insert

    from lemongrass import _tracks
    if not _tracks.normalize(name):
        raise ValueError(f"team name {name!r} normalizes to nothing")
    stmt = insert(_schema.teams).values(team_id=team_id, name=name)
    with connection(conn) as c:
        c.execute(stmt.on_conflict_do_update(
            index_elements=[_schema.teams.c.team_id],
            set_={'name': stmt.excluded.name}))


def get_team(team_id, conn=None):
    """Return the TeamRow for team_id, or None."""
    from sqlalchemy import select
    with connection(conn) as c:
        row = c.execute(
            select(_schema.teams).where(_schema.teams.c.team_id == team_id)
        ).first()
    return TeamRow(team_id=row.team_id, name=row.name) if row is not None else None


def list_teams(conn=None):
    """Return every team, ordered by id so output is stable."""
    from sqlalchemy import select
    with connection(conn) as c:
        rows = c.execute(
            select(_schema.teams).order_by(_schema.teams.c.team_id)).all()
    return [TeamRow(team_id=r.team_id, name=r.name) for r in rows]


def add_team_alias(team_id, alias, conn=None):
    """Record a historical spelling for a team, stored normalized.

    Normalizing here rather than at the call site is what keeps "aliases are
    stored normalized" true: every caller passes raw text and every reader
    compares against already-normalized stored values. merge_teams is the one
    other writer of this table and normalizes for itself, deliberately — it
    needs the owner lookup this function's ValueError would abort on.

    Re-recording an alias the team already owns is a silent no-op — the
    `entries propose` -> `confirm_proposals` loop offers to record the alias
    for every matching proposal, including ones an earlier run already
    recorded, and re-answering "y" must not raise. An alias already claimed by
    a *different* team, or a nonexistent team_id, raises ValueError instead of
    a raw IntegrityError, so callers can report it instead of crashing on a
    FK/unique-key violation mid-loop.
    """
    from sqlalchemy import insert, select

    from lemongrass import _tracks
    normalized = _tracks.normalize(alias)
    if not normalized:
        # alias is the primary key, so an alias of nothing but punctuation
        # would take the '' row and lock every other team out of it.
        raise ValueError(f"alias {alias!r} normalizes to nothing")
    with connection(conn) as c:
        if c.execute(select(_schema.teams.c.team_id).where(
                _schema.teams.c.team_id == team_id)).scalar() is None:
            raise ValueError(f"no team {team_id!r}")
        owner = c.execute(select(_schema.team_aliases.c.team_id).where(
            _schema.team_aliases.c.alias == normalized)).scalar()
        if owner == team_id:
            return
        if owner is not None:
            raise ValueError(
                f"alias {normalized!r} is already recorded for team "
                f"{owner!r}, not {team_id!r}")
        c.execute(insert(_schema.team_aliases).values(
            team_id=team_id, alias=normalized))


def list_team_aliases(team_id=None, conn=None):
    """Return (team_id, alias) pairs for one team or for every team."""
    from sqlalchemy import select
    stmt = select(_schema.team_aliases)
    if team_id is not None:
        stmt = stmt.where(_schema.team_aliases.c.team_id == team_id)
    with connection(conn) as c:
        rows = c.execute(stmt.order_by(_schema.team_aliases.c.alias)).all()
    return [(r.team_id, r.alias) for r in rows]


@dataclass
class MergeResult:
    """What a merge did: entries moved, and who else already owns the name.

    name_alias_owner is set only when the source team's name could not be
    recorded as an alias of the target because a third team already holds that
    spelling. The merge itself still happened; the caller reports the gap.
    """

    entries_moved: int
    name_alias_owner: str | None = None


def merge_teams(from_id, into_id, conn=None):
    """Fold one team into another in a single transaction. Returns a MergeResult.

    The repair path for history recorded under two identities before anyone
    noticed. It is a handful of statements and not a data migration precisely
    because entries reference a team_id rather than a name. The source team's
    own name is kept as an alias of the target — it is the spelling that made
    the merge necessary, and `entries propose` searches aliases.

    Raises ValueError, which callers must catch, when from_id and into_id are
    the same team or when either does not exist.
    """
    from sqlalchemy import delete, insert, select, update

    from lemongrass import _tracks
    if from_id == into_id:
        # Without this guard the final DELETE removes the (only) team row
        # entries were just re-pointed at, either cascading away its aliases
        # (no entries referenced it) or failing on the entries FK — neither
        # is "merged into itself", so reject it up front instead.
        raise ValueError(f"cannot merge team {from_id!r} into itself")
    with connection(conn) as c:
        source = c.execute(select(_schema.teams).where(
            _schema.teams.c.team_id == from_id)).first()
        if source is None:
            raise ValueError(f"no team {from_id!r}")
        if c.execute(select(_schema.teams.c.team_id).where(
                _schema.teams.c.team_id == into_id)).scalar() is None:
            raise ValueError(f"no team {into_id!r}")
        moved = c.execute(
            update(_schema.entries)
            .where(_schema.entries.c.team_id == from_id)
            .values(team_id=into_id)).rowcount
        # alias is the primary key, so re-pointing rows cannot collide.
        c.execute(update(_schema.team_aliases)
                  .where(_schema.team_aliases.c.team_id == from_id)
                  .values(team_id=into_id))
        # ON CONFLICT DO NOTHING would cover "the target already has it" and
        # "a third team has it" identically, and the second case means the
        # documented guarantee silently did not happen: `entries propose` for
        # the target would keep missing that spelling with nothing to explain
        # why. Look the owner up so the caller can say so.
        name_alias = _tracks.normalize(source.name)
        owner = None
        if name_alias:
            owner = c.execute(select(_schema.team_aliases.c.team_id).where(
                _schema.team_aliases.c.alias == name_alias)).scalar()
            if owner is None:
                c.execute(insert(_schema.team_aliases).values(
                    team_id=into_id, alias=name_alias))
        c.execute(delete(_schema.teams).where(
            _schema.teams.c.team_id == from_id))
    return MergeResult(entries_moved=moved,
                       name_alias_owner=owner if owner != into_id else None)


@dataclass
class EntryRow:
    """One row of the entries table: which team ran this number in this race."""

    race_id: str
    car_number: str
    team_id: str


def set_entry(race_id, car_number, team_id, conn=None):
    """Record which team ran a car number in a race, replacing any prior answer.

    car_number is trimmed on write; a stray leading space has reached the tag
    layer before and blanked a whole dashboard.

    An absent race raises ValueError rather than a raw IntegrityError from the
    race_id foreign key: `entries propose` sources race ids from Influx lap
    data, which legitimately carries races with no Postgres row (anything
    captured before the cutover), so this is a normal answer and callers need
    to be able to report it and carry on rather than crash mid-loop.

    team_id is not checked here: the caller must ensure the team exists, and an
    unknown one raises a raw IntegrityError. Every caller today pre-checks
    with get_team (entries._handle_set, entries._handle_propose,
    laps.store_entry) because each wants to report it differently.
    """
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert
    stmt = insert(_schema.entries).values(
        race_id=race_id, car_number=str(car_number).strip(), team_id=team_id)
    with connection(conn) as c:
        if c.execute(select(_schema.races.c.race_id).where(
                _schema.races.c.race_id == race_id)).scalar() is None:
            raise ValueError(f"no race row for {race_id!r}")
        c.execute(stmt.on_conflict_do_update(
            index_elements=[_schema.entries.c.race_id,
                            _schema.entries.c.car_number],
            set_={'team_id': stmt.excluded.team_id}))


def get_entry(race_id, car_number, conn=None):
    """Return the EntryRow for one race and car number, or None."""
    from sqlalchemy import select
    with connection(conn) as c:
        row = c.execute(
            select(_schema.entries)
            .where(_schema.entries.c.race_id == race_id,
                   _schema.entries.c.car_number == str(car_number).strip())
        ).first()
    return EntryRow(row.race_id, row.car_number, row.team_id) if row else None


def list_entries(team_id=None, race_id=None, conn=None):
    """Return entries, optionally filtered by team or race, in a stable order."""
    from sqlalchemy import select
    stmt = select(_schema.entries)
    if team_id is not None:
        stmt = stmt.where(_schema.entries.c.team_id == team_id)
    if race_id is not None:
        stmt = stmt.where(_schema.entries.c.race_id == race_id)
    stmt = stmt.order_by(_schema.entries.c.race_id,
                         _schema.entries.c.car_number)
    with connection(conn) as c:
        rows = c.execute(stmt).all()
    return [EntryRow(r.race_id, r.car_number, r.team_id) for r in rows]
