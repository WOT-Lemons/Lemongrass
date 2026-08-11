#!/usr/bin/env python
"""lemongrass tracks subcommand: keep the curated track data and the tables in step.

Subcommand: sync. `db upgrade` runs the same sync as its final step, and
store_race syncs before it writes, so this exists for the case where a curator
edits tracks.toml and wants to see the effect without running either.
(`db import-legacy` writes race rows without syncing, which is safe only
because legacy rows carry NULL identity columns and so reference nothing.)
"""
import argparse
import sys

_SUBCOMMANDS = ('sync',)


def main():
    """Dispatch to the tracks subcommand named by the first argument."""
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass tracks <subcommand>")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        return 1
    subcmd = sys.argv.pop(1)
    sys.argv[0] = f'lemongrass-tracks-{subcmd}'
    return {'sync': _handle_sync}[subcmd]()


def print_sync_summary(summary, dry_run=False):
    """Print the counts and the orphan report from a sync_tracks summary."""
    for key in ('venues', 'layouts', 'events'):
        print(f"{key:<8}: created {summary[key + '_created']:3d}  "
              f"updated {summary[key + '_updated']:3d}")
    orphans = (summary['orphan_venues'] or summary['orphan_layouts']
               or summary['orphan_events'])
    if orphans:
        print("stored but absent from tracks.toml (left in place):")
        for venue_id in summary['orphan_venues']:
            print(f"  venue {venue_id}")
        for venue_id, layout_id in summary['orphan_layouts']:
            print(f"  layout {venue_id}/{layout_id}")
        for event_id in summary['orphan_events']:
            print(f"  event {event_id}")
    if dry_run:
        print("dry run: nothing written")


def _handle_sync():
    """Copy tracks.toml into the venues, layouts, and events tables."""
    from lemongrass import _db, _tracks
    parser = argparse.ArgumentParser(
        prog='lemongrass tracks sync',
        description='Sync the curated track data into PostgreSQL')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Report what would change without writing')
    args = parser.parse_args()
    print_sync_summary(
        _db.sync_tracks(_tracks.data(), dry_run=args.dry_run),
        dry_run=args.dry_run)
    return 0
