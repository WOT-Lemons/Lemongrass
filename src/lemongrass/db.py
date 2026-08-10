#!/usr/bin/env python
"""lemongrass db subcommand: manage the PostgreSQL schema.

Subcommands: upgrade, current, import-legacy.

Migrations ship inside the installed package, so this runs from a wheel or a
container with no checkout and no alembic.ini present.
"""
import sys

_SUBCOMMANDS = ('upgrade', 'current', 'import-legacy')


def main():
    """Dispatch to the db subcommand named by the first argument."""
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass db <subcommand>")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        return 1
    subcmd = sys.argv.pop(1)
    return {
        'upgrade': _handle_upgrade,
        'current': _handle_current,
        'import-legacy': _handle_import_legacy,
    }[subcmd]()


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


def _handle_import_legacy():
    """Copy legacy Influx races and sessions into Postgres."""
    import argparse
    import logging

    from lemongrass import _influx, _legacy_migration
    parser = argparse.ArgumentParser(
        prog='lemongrass db import-legacy',
        description='Copy race and session data from InfluxDB into PostgreSQL')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Read and report without writing anything')
    parser.add_argument('--only-missing', action='store_true', default=False,
                        help='Insert rows absent from Postgres; never update '
                             'an existing row (use for the post-deploy catch-up)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    with _influx.connect() as client:
        summary = _legacy_migration.import_legacy(
            client.query_api(), dry_run=args.dry_run,
            only_missing=args.only_missing)

    from lemongrass import _db
    print(f"races:    read {summary['races_read']:5d}  "
          f"written {summary['races_written']:5d}")
    print(f"sessions: read {summary['sessions_read']:5d}  "
          f"written {summary['sessions_written']:5d}  "
          f"skipped {summary['sessions_skipped']:5d}")
    if summary['orphan_race_ids']:
        print("orphan sessions belong to race id(s): "
              + ' '.join(summary['orphan_race_ids']))
    print(f"now stored: {len(_db.list_races())} race(s), "
          f"{len(_db.list_sessions())} session(s)")
    return 0
