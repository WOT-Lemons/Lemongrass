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
    """Apply every migration that has not yet been applied."""
    from alembic import command

    from lemongrass import _db
    command.upgrade(_db.alembic_config(), 'head')
    return 0


def _handle_current():
    """Print the revision currently applied to the database."""
    from alembic import command

    from lemongrass import _db
    command.current(_db.alembic_config(), verbose=True)
    return 0
