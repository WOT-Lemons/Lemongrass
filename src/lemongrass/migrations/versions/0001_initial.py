"""Create races and sessions.

Revision ID: 0001
Revises:
Create Date: 2026-08-09
"""
import sqlalchemy as sa
from alembic import op

revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Create the races and sessions tables."""
    op.create_table(
        'races',
        sa.Column('race_id', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False, server_default=''),
        sa.Column('track_name', sa.Text(), nullable=False, server_default=''),
        sa.Column('series_id', sa.Integer(), nullable=True),
        sa.Column('series_name', sa.Text(), nullable=True),
        sa.Column('race_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('end_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('expected_lap_count', sa.Integer(), nullable=True),
        sa.Column('session_count', sa.Integer(), nullable=True),
        sa.Column('lap_schema_version', sa.Integer(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('race_id', name='pk_races'),
    )
    op.create_table(
        'sessions',
        sa.Column('session_id', sa.BigInteger(), nullable=False),
        sa.Column('race_id', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False, server_default=''),
        sa.Column('start_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('session_id', name='pk_sessions'),
        sa.ForeignKeyConstraint(
            ['race_id'], ['races.race_id'],
            name='fk_sessions_race_id_races', ondelete='CASCADE'),
    )
    op.create_index('ix_sessions_race_id', 'sessions', ['race_id'])


def downgrade():
    """Drop the sessions and races tables."""
    op.drop_index('ix_sessions_race_id', table_name='sessions')
    op.drop_table('sessions')
    op.drop_table('races')
