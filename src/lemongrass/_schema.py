"""SQLAlchemy Core table definitions for the lemongrass relational store.

A leaf module: it imports nothing else from lemongrass, so both ``_db`` and
Alembic's ``env.py`` can import it without an import cycle.

Only the non-time-series data lives here. Laps, standings, and telemetry stay in
InfluxDB — see the sub-project 1a design doc for why races and sessions do not.
"""
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    func,
)

# Every constraint and index gets a deterministic name. Without this, Alembic
# emits unnamed constraints that a downgrade cannot drop.
NAMING_CONVENTION = {
    'ix': 'ix_%(table_name)s_%(column_0_N_name)s',
    'uq': 'uq_%(table_name)s_%(column_0_N_name)s',
    'ck': 'ck_%(table_name)s_%(constraint_name)s',
    'fk': 'fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s',
    'pk': 'pk_%(table_name)s',
}

metadata = MetaData(naming_convention=NAMING_CONVENTION)

# race_id is TEXT, not an integer: it is a RaceMonitor identifier we do not mint,
# and ctx.race_id is a str throughout the codebase.
races = Table(
    'races', metadata,
    Column('race_id', Text, primary_key=True),
    # NOT NULL with a default rather than bare NOT NULL: Influx drops
    # empty-string tag values entirely, so a race written from an unsuccessful
    # details fetch comes back from a pivot with the column absent.
    Column('name', Text, nullable=False, server_default=''),
    Column('track_name', Text, nullable=False, server_default=''),
    Column('series_id', Integer),
    Column('series_name', Text),
    # Epochs become real timestamps: Influx fields must be numeric, Postgres has
    # no such restriction, and it removes a seconds-versus-milliseconds trap.
    Column('race_time', DateTime(timezone=True), nullable=False),
    Column('end_time', DateTime(timezone=True)),
    Column('expected_lap_count', Integer),
    Column('session_count', Integer),
    # Named lap_schema_version to avoid colliding with Alembic's own versioning
    # vocabulary; the Influx field it maps from is called schema_version.
    Column('lap_schema_version', Integer),
    Column('updated_at', DateTime(timezone=True), nullable=False,
           server_default=func.now()),
    # Curated identity, resolved by _tracks.resolve and synced from
    # tracks.toml. NULL means "no curated match", which dashboards group by
    # track_name instead; there is deliberately no raw-string fallback.
    Column('venue_id', Text),
    Column('layout_id', Text),
    Column('event_id', Text),
    ForeignKeyConstraint(['venue_id'], ['venues.venue_id'],
                         name='fk_races_venue_id_venues'),
    ForeignKeyConstraint(['event_id'], ['events.event_id'],
                         name='fk_races_event_id_events'),
    ForeignKeyConstraint(['venue_id', 'layout_id'],
                         ['layouts.venue_id', 'layouts.layout_id'],
                         name='fk_races_venue_id_layout_id_layouts'),
    # Load-bearing. PostgreSQL's default MATCH SIMPLE skips the composite
    # foreign key check whenever any referencing column is NULL. That is what
    # makes ('njmp', NULL) legal — but it equally permits (NULL,
    # 'thunderbolt'), a layout orphaned from its venue, silently.
    CheckConstraint('layout_id IS NULL OR venue_id IS NOT NULL',
                    name='layout_needs_venue'),
)

sessions = Table(
    'sessions', metadata,
    Column('session_id', BigInteger, primary_key=True),
    Column('race_id', Text,
           ForeignKey('races.race_id', ondelete='CASCADE'), nullable=False),
    Column('name', Text, nullable=False, server_default=''),
    Column('start_time', DateTime(timezone=True)),
    Column('updated_at', DateTime(timezone=True), nullable=False,
           server_default=func.now()),
)

Index('ix_sessions_race_id', sessions.c.race_id)

venues = Table(
    'venues', metadata,
    Column('venue_id', Text, primary_key=True),
    Column('name', Text, nullable=False),
)

layouts = Table(
    'layouts', metadata,
    Column('venue_id', Text,
           ForeignKey('venues.venue_id', ondelete='CASCADE'), primary_key=True),
    Column('layout_id', Text, primary_key=True),
    Column('name', Text, nullable=False),
)

# event_id is globally unique, not scoped per series, matching its
# single-column primary key; the loader enforces the same rule on the file.
events = Table(
    'events', metadata,
    Column('event_id', Text, primary_key=True),
    Column('series_id', Integer, nullable=False),
    Column('name', Text, nullable=False),
)

teams = Table(
    'teams', metadata,
    Column('team_id', Text, primary_key=True),
    Column('name', Text, nullable=False),
)

# alias is the primary key so one spelling cannot map to two teams.
team_aliases = Table(
    'team_aliases', metadata,
    Column('team_id', Text,
           ForeignKey('teams.team_id', ondelete='CASCADE'), nullable=False),
    Column('alias', Text, primary_key=True),
)

# (race_id, car_number) encodes the real constraint: a number is unique within
# a race. Two of our cars at one event is two rows with the same team_id.
# team_id is NO ACTION: dropping a team goes through `teams merge`.
entries = Table(
    'entries', metadata,
    Column('race_id', Text,
           ForeignKey('races.race_id', ondelete='CASCADE'), primary_key=True),
    Column('car_number', Text, primary_key=True),
    Column('team_id', Text, ForeignKey('teams.team_id'), nullable=False),
)

Index('ix_races_venue_id', races.c.venue_id)
Index('ix_races_event_id', races.c.event_id)
Index('ix_entries_team_id', entries.c.team_id)
