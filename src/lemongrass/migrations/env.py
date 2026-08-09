"""Alembic environment for the lemongrass relational store.

The URL always arrives from the Config the caller built (see
``_db.alembic_config``); this file never reads a database URL of its own.
"""
from alembic import context
from sqlalchemy import engine_from_config, pool

from lemongrass._schema import metadata

target_metadata = metadata


def run_migrations_offline():
    """Emit SQL to stdout instead of running it, for `alembic upgrade --sql`."""
    context.configure(
        url=context.config.get_main_option('sqlalchemy.url'),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations against a live connection."""
    connectable = engine_from_config(
        context.config.get_section(context.config.config_ini_section, {}),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
