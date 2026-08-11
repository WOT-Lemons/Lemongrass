#!/usr/bin/env python
"""lemongrass entries subcommand: which team ran which car number in which race.

Subcommands: set, list, propose.

Car number is not stable — we normally run 252 but ran 253 at an event where
another entrant took the number first — so "our laps" cannot be derived from
the number or from the team name, only from these rows.
"""
import argparse
import sys

from lemongrass import _config, _db

_SUBCOMMANDS = ('set', 'list', 'propose')


def main():
    """Dispatch to the entries subcommand named by the first argument."""
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCOMMANDS:
        print("Usage: lemongrass entries <subcommand>")
        print(f"Subcommands: {', '.join(_SUBCOMMANDS)}")
        return 1
    subcmd = sys.argv.pop(1)
    sys.argv[0] = f'lemongrass-entries-{subcmd}'
    return {'set': _handle_set, 'list': _handle_list,
             'propose': _handle_propose}[subcmd]()


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


EPOCH_START = '1970-01-01T00:00:00Z'


def _competitor_names(query_api):
    """Yield (race_id, car_number, competitor_name) from stored lap data.

    One record per distinct name per car per race. Cheap: this reads Influx,
    not RaceMonitor, so it is not rate-limited.
    """
    from lemongrass import _influx
    tables = query_api.query(
        f'from(bucket: "{_influx.BUCKET_LAPS}")\n'
        f'  |> range(start: {EPOCH_START})\n'
        f'  |> filter(fn: (r) => r._measurement == "lap"\n'
        f'      and r._field == "competitor_name")\n'
        f'  |> group(columns: ["race_id", "car_number"])\n'
        f'  |> distinct(column: "_value")')
    for table in tables:
        for record in table.records:
            yield (record.values.get('race_id'),
                   str(record.values.get('car_number') or '').strip(),
                   record.get_value())


def propose_entries(query_api, terms, team_id):
    """Propose race/car entries whose stored competitor name matches any term.

    Terms are normalized the same way track names are, so punctuation and case
    do not matter and several terms handle the season the team name differed.
    The team's recorded aliases are searched alongside the given terms — they
    are already-confirmed spellings of this team, so every confirmation makes
    the next run find more without anyone having to remember the old name.
    Nothing is written here — this only proposes.
    """
    from lemongrass import _tracks
    aliases = [alias for _, alias in _db.list_team_aliases(team_id)]
    wanted = [_tracks.normalize(t) for t in [*terms, *aliases]
              if _tracks.normalize(t)]
    proposals, seen = [], set()
    for race_id, car_number, name in _competitor_names(query_api):
        if not race_id or not car_number or not name:
            continue
        normalized = _tracks.normalize(name)
        if not any(term in normalized for term in wanted):
            continue
        key = (race_id, car_number)
        if key in seen:
            continue
        existing = _db.get_entry(race_id, car_number)
        if existing is not None and existing.team_id == team_id:
            continue
        seen.add(key)
        proposals.append({
            'race_id': race_id, 'car_number': car_number,
            'competitor_name': name,
            'existing_team_id': existing.team_id if existing else None,
        })
    return proposals


def confirm_proposals(proposals, team_id):
    """Prompt for each proposal and write the accepted ones. Returns the count.

    Confirming also offers to record the matched spelling as an alias, so the
    term list improves with use instead of having to be remembered.
    """
    written = 0
    for proposal in proposals:
        note = ''
        if proposal['existing_team_id']:
            note = f" (currently {proposal['existing_team_id']})"
        answer = input(
            f"race {proposal['race_id']} car {proposal['car_number']}: "
            f"{proposal['competitor_name']}{note} -> {team_id}? [y/N] ")
        if answer.strip().lower() != 'y':
            continue
        _db.set_entry(proposal['race_id'], proposal['car_number'], team_id)
        written += 1
        alias = input(f"  record {proposal['competitor_name']!r} as an alias "
                      f"of {team_id}? [y/N] ")
        if alias.strip().lower() == 'y':
            _db.add_team_alias(team_id, proposal['competitor_name'])
    return written


def _handle_propose():
    """Scan stored lap data for a team's names and offer to record entries."""
    from lemongrass import _influx
    parser = argparse.ArgumentParser(
        prog='lemongrass-entries-propose',
        description='Propose entries from stored lap data')
    parser.add_argument('--team', default=None,
                        help='team id (default: [team] id from config)')
    parser.add_argument('--term', action='append', required=True, dest='terms',
                        help='name fragment to search for (repeatable)')
    args = parser.parse_args()
    team_id = _resolve_team(args.team)
    if not team_id:
        print("Error: no team given and [team] id is not set in the config file",
              file=sys.stderr)
        return 1

    with _influx.connect() as client:
        proposals = propose_entries(client.query_api(), args.terms, team_id)
    if not proposals:
        print("no unrecorded matches")
        return 0
    print(f"{len(proposals)} proposed entr"
          f"{'y' if len(proposals) == 1 else 'ies'}:")
    written = confirm_proposals(proposals, team_id)
    print(f"recorded {written} entr{'y' if written == 1 else 'ies'}")
    return 0
