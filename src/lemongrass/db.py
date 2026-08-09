#!/usr/bin/env python
"""lemongrass db subcommand: manage the PostgreSQL schema.

Subcommands: upgrade, current.

Migrations ship inside the installed package, so this runs from a wheel or a
container with no checkout and no alembic.ini present.
"""
import sys

_SUBCOMMANDS = ('upgrade', 'current')


def main():
    """Dispatch to the db subcommand named by the first argument."""
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass db <subcommand>")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        return 1
    subcmd = sys.argv.pop(1)
    return {'upgrade': _handle_upgrade, 'current': _handle_current}[subcmd]()


def _handle_upgrade():
    """Apply every migration that has not yet been applied.

    Alembic reports what it did through the ``alembic.runtime.migration``
    logger, not through print. Config's fileConfig-based logging setup only
    runs inside `alembic.config.CommandLine.main`, which this CLI never
    calls, so without configuring logging here the command would run
    silently whether it applied revision 0001, found nothing to do, or
    reached the wrong database. Configured here, not in
    `_db.alembic_config`, so importing `_db` never mutates global logging
    state.
    """
    import logging

    from alembic import command

    from lemongrass import _db
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('alembic').setLevel(logging.INFO)
    command.upgrade(_db.alembic_config(), 'head')
    return 0


def _handle_current():
    """Print the revision currently applied to the database."""
    from alembic import command

    from lemongrass import _db
    command.current(_db.alembic_config(), verbose=True)
    return 0
