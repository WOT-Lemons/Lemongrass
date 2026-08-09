"""SQLAlchemy Core table definitions for the lemongrass relational store.

A leaf module: it imports nothing else from lemongrass, so both ``_db`` and
Alembic's ``env.py`` can import it without an import cycle.

Only the non-time-series data lives here. Laps, standings, and telemetry stay in
InfluxDB — see the sub-project 1a design doc for why races and sessions do not.
"""
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
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
