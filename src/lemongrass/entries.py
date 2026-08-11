#!/usr/bin/env python
"""lemongrass entries subcommand: which team ran which car number in which race.

Subcommands: set, list.

Car number is not stable — we normally run 252 but ran 253 at an event where
another entrant took the number first — so "our laps" cannot be derived from
the number or from the team name, only from these rows.
"""
import argparse
import sys

from lemongrass import _config, _db

_SUBCOMMANDS = ('set', 'list')


def main():
    """Dispatch to the entries subcommand named by the first argument."""
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass entries <subcommand>")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        return 1
    subcmd = sys.argv.pop(1)
    sys.argv[0] = f'lemongrass-entries-{subcmd}'
    return {'set': _handle_set, 'list': _handle_list}[subcmd]()


def _resolve_team(explicit):
    """Return the team id to use: the flag, else [team] id from config."""
    return explicit or _config.load_config().team.id


def _handle_set():
    """Record one race/car/team entry."""
    parser = argparse.ArgumentParser(
        prog='lemongrass-entries-set',
        description='Record which team ran a car number in a race')
    parser.add_argument('race_id')
    parser.add_argument('car_number')
    parser.add_argument('--team', default=None,
                        help='team id (default: [team] id from config)')
    args = parser.parse_args()
    team_id = _resolve_team(args.team)
    if not team_id:
        print("Error: no team given and [team] id is not set in the config file",
              file=sys.stderr)
        return 1
    _db.set_entry(args.race_id, args.car_number, team_id)
    print(f"{args.race_id}  {args.car_number.strip()}  {team_id}")
    return 0


def _handle_list():
    """Print stored entries, optionally for one team."""
    parser = argparse.ArgumentParser(prog='lemongrass-entries-list',
                                     description='List stored entries')
    parser.add_argument('--team', default=None, help='only this team id')
    args = parser.parse_args()
    print(f"{'RACE ID':<10} {'CAR':<6} TEAM")
    print('-' * 40)
    for entry in _db.list_entries(team_id=args.team):
        print(f"{entry.race_id:<10} {entry.car_number:<6} {entry.team_id}")
    return 0
