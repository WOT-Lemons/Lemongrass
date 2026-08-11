#!/usr/bin/env python
"""lemongrass teams subcommand: the team side of identity.

Team data is operational, not curated: it grows by one row every time we race,
so it lives in the database rather than in a file that would need a release cut
to record last weekend.

Subcommands: add, list, alias, merge.
"""
import argparse
import sys

from lemongrass import _db

_SUBCOMMANDS = ('add', 'list', 'alias', 'merge')


def main():
    """Dispatch to the teams subcommand named by the first argument."""
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass teams <subcommand>")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        return 1
    subcmd = sys.argv.pop(1)
    sys.argv[0] = f'lemongrass-teams-{subcmd}'
    return {'add': _handle_add, 'list': _handle_list,
            'alias': _handle_alias, 'merge': _handle_merge}[subcmd]()


def _handle_add():
    """Create a team, or rename an existing one."""
    parser = argparse.ArgumentParser(prog='lemongrass-teams-add',
                                     description='Create or rename a team')
    parser.add_argument('team_id')
    parser.add_argument('name')
    args = parser.parse_args()
    try:
        _db.upsert_team(args.team_id, args.name)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    print(f"{args.team_id}  {args.name}")
    return 0


def _handle_list():
    """Print every team with its recorded aliases."""
    argparse.ArgumentParser(prog='lemongrass-teams-list',
                            description='List teams').parse_args()
    aliases = {}
    for team_id, alias in _db.list_team_aliases():
        aliases.setdefault(team_id, []).append(alias)
    print(f"{'TEAM ID':<20} {'NAME':<30} ALIASES")
    print('-' * 80)
    for team in _db.list_teams():
        print(f"{team.team_id:<20} {team.name:<30} "
              f"{', '.join(aliases.get(team.team_id, []))}")
    return 0


def _handle_alias():
    """Record a historical name for a team."""
    parser = argparse.ArgumentParser(
        prog='lemongrass-teams-alias',
        description='Record a historical name for a team')
    parser.add_argument('team_id')
    parser.add_argument('alias')
    args = parser.parse_args()
    try:
        _db.add_team_alias(args.team_id, args.alias)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    print(f"{args.team_id}  <- {args.alias}")
    return 0


def _handle_merge():
    """Fold one team into another, moving its entries and aliases."""
    parser = argparse.ArgumentParser(
        prog='lemongrass-teams-merge',
        description='Fold one team into another')
    parser.add_argument('from_id')
    parser.add_argument('into_id')
    args = parser.parse_args()
    try:
        result = _db.merge_teams(args.from_id, args.into_id)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    moved = result.entries_moved
    print(f"merged {args.from_id} into {args.into_id}: "
          f"{moved} entr{'y' if moved == 1 else 'ies'} moved")
    if result.name_alias_owner:
        print(f"Warning: {args.from_id}'s name is already an alias of team "
              f"{result.name_alias_owner}, so it was not recorded for "
              f"{args.into_id}", file=sys.stderr)
    return 0
