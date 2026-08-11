"""Create track and team identity tables and tag races with them.

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-10
"""
import sqlalchemy as sa
from alembic import op

revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade():
    """Create the identity tables, then add the three columns to races."""
    op.create_table(
        'venues',
        sa.Column('venue_id', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('venue_id', name='pk_venues'),
    )
    op.create_table(
        'layouts',
        sa.Column('venue_id', sa.Text(), nullable=False),
        sa.Column('layout_id', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('venue_id', 'layout_id', name='pk_layouts'),
        sa.ForeignKeyConstraint(
            ['venue_id'], ['venues.venue_id'],
            name='fk_layouts_venue_id_venues', ondelete='CASCADE'),
    )
    op.create_table(
        'events',
        sa.Column('event_id', sa.Text(), nullable=False),
        sa.Column('series_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('event_id', name='pk_events'),
    )
    op.create_table(
        'teams',
        sa.Column('team_id', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('team_id', name='pk_teams'),
    )
    op.create_table(
        'team_aliases',
        sa.Column('team_id', sa.Text(), nullable=False),
        sa.Column('alias', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('alias', name='pk_team_aliases'),
        sa.ForeignKeyConstraint(
            ['team_id'], ['teams.team_id'],
            name='fk_team_aliases_team_id_teams', ondelete='CASCADE'),
    )
    op.create_table(
        'entries',
        sa.Column('race_id', sa.Text(), nullable=False),
        sa.Column('car_number', sa.Text(), nullable=False),
        sa.Column('team_id', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('race_id', 'car_number', name='pk_entries'),
        sa.ForeignKeyConstraint(
            ['race_id'], ['races.race_id'],
            name='fk_entries_race_id_races', ondelete='CASCADE'),
        sa.ForeignKeyConstraint(
            ['team_id'], ['teams.team_id'], name='fk_entries_team_id_teams'),
    )
    op.create_index('ix_entries_team_id', 'entries', ['team_id'])

    op.add_column('races', sa.Column('venue_id', sa.Text(), nullable=True))
    op.add_column('races', sa.Column('layout_id', sa.Text(), nullable=True))
    op.add_column('races', sa.Column('event_id', sa.Text(), nullable=True))
    op.create_foreign_key('fk_races_venue_id_venues', 'races', 'venues',
                          ['venue_id'], ['venue_id'])
    op.create_foreign_key('fk_races_event_id_events', 'races', 'events',
                          ['event_id'], ['event_id'])
    op.create_foreign_key('fk_races_venue_id_layout_id_layouts', 'races',
                          'layouts', ['venue_id', 'layout_id'],
                          ['venue_id', 'layout_id'])
    # Closes the hole MATCH SIMPLE leaves open: without this, (NULL,
    # 'thunderbolt') passes the composite foreign key unchecked.
    # The BARE name, not 'ck_races_layout_needs_venue'. MigrationContext
    # inherits target_metadata's naming convention from env.py, so the name
    # passed here is run through ck_%(table_name)s_%(constraint_name)s a second
    # time — passing the rendered name would create
    # ck_races_ck_races_layout_needs_venue and silently drift from _schema.py.
    op.create_check_constraint('layout_needs_venue', 'races',
                               'layout_id IS NULL OR venue_id IS NOT NULL')
    op.create_index('ix_races_venue_id', 'races', ['venue_id'])
    op.create_index('ix_races_event_id', 'races', ['event_id'])


def downgrade():
    """Drop the races columns first, then the tables they referenced."""
    op.drop_index('ix_races_event_id', table_name='races')
    op.drop_index('ix_races_venue_id', table_name='races')
    op.drop_constraint('layout_needs_venue', 'races', type_='check')
    op.drop_constraint('fk_races_venue_id_layout_id_layouts', 'races',
                       type_='foreignkey')
    op.drop_constraint('fk_races_event_id_events', 'races', type_='foreignkey')
    op.drop_constraint('fk_races_venue_id_venues', 'races', type_='foreignkey')
    op.drop_column('races', 'event_id')
    op.drop_column('races', 'layout_id')
    op.drop_column('races', 'venue_id')

    op.drop_index('ix_entries_team_id', table_name='entries')
    op.drop_table('entries')
    op.drop_table('team_aliases')
    op.drop_table('teams')
    op.drop_table('events')
    op.drop_table('layouts')
    op.drop_table('venues')
