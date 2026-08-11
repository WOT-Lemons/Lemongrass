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
    """Return the team id to use — the flag, else [team] id from config.

    Returns None, having already reported it, when neither names a team, so a
    handler's whole response is `if not team_id: return 1`.
    """
    team_id = explicit or _config.load_config().team.id
    if not team_id:
        print("Error: no team given and [team] id is not set in the config file",
              file=sys.stderr)
        return None
    return team_id


def _team_exists(team_id):
    """True if team_id has a row; otherwise report how to create it.

    Both handlers pre-check rather than letting the write raise: set_entry's
    team_id has no ValueError path, so an unknown team surfaces as a raw FK
    IntegrityError. They check at different points, though — _handle_set after
    its car-number guard, _handle_propose before it scans Influx — so this is a
    helper they call, not a step folded into _resolve_team.
    """
    if _db.get_team(team_id) is not None:
        return True
    print(f"Error: no team {team_id!r}; run `lemongrass teams add "
          f"{team_id} <name>` first", file=sys.stderr)
    return False


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
        return 1
    # Mirror store_entry's (laps.py) guards: a blank car number would write a
    # row with an empty-string primary-key component, and an unknown team
    # would raise an unhandled FK IntegrityError instead of a clean message.
    if not args.car_number.strip():
        print("Error: car number is blank", file=sys.stderr)
        return 1
    if not _team_exists(team_id):
        return 1
    try:
        _db.set_entry(args.race_id, args.car_number, team_id)
    except ValueError as e:
        # race_id is a foreign key; a race captured before the cutover has no
        # row until `lemongrass db import-legacy` brings it over.
        print(f"Error: {e}", file=sys.stderr)
        return 1
    print(f"{args.race_id}  {args.car_number.strip()}  {team_id}")
    return 0


def _handle_list():
    """Print stored entries, optionally for one team."""
    parser = argparse.ArgumentParser(prog='lemongrass-entries-list',
                                     description='List stored entries')
    parser.add_argument('--team', default=None, help='only this team id')
    parser.add_argument('--race', default=None, help='only this race id')
    args = parser.parse_args()
    print(f"{'RACE ID':<10} {'CAR':<6} TEAM")
    print('-' * 40)
    for entry in _db.list_entries(team_id=args.team, race_id=args.race):
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
    # One read of the whole table rather than a get_entry per candidate: each
    # of those checks out its own pooled connection (and pool_pre_ping costs a
    # round trip per checkout), and the same car gets re-queried once per
    # distinct spelling of its name.
    existing_teams = {(e.race_id, e.car_number): e.team_id
                      for e in _db.list_entries()}
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
        seen.add(key)
        existing = existing_teams.get(key)
        if existing == team_id:
            continue
        proposals.append({
            'race_id': race_id, 'car_number': car_number,
            'competitor_name': name,
            'existing_team_id': existing,
        })
    return proposals


def _prompt(text):
    """input(), returning None instead of raising on end of input.

    Ctrl-D, or a redirected stdin that runs out, means "no more answers" — the
    caller stops asking and reports what it already wrote, rather than dying
    part-way through a loop whose earlier answers are already stored.
    """
    try:
        return input(text)
    except EOFError:
        print()
        return None


def confirm_proposals(proposals, team_id):
    """Prompt for each proposal and write the accepted ones.

    Returns (written, failed): counts of accepted proposals that were stored
    and that could not be. failed is reported rather than raised so one
    unstorable race does not cost the operator every answer after it, but the
    caller needs it — "every write failed" must not exit 0.

    Confirming also offers to record the matched spelling as an alias, so the
    term list improves with use instead of having to be remembered.
    """
    written = failed = 0
    for proposal in proposals:
        note = ''
        if proposal['existing_team_id']:
            note = f" (currently {proposal['existing_team_id']})"
        answer = _prompt(
            f"race {proposal['race_id']} car {proposal['car_number']}: "
            f"{proposal['competitor_name']}{note} -> {team_id}? [y/N] ")
        if answer is None:
            break
        if answer.strip().lower() != 'y':
            continue
        try:
            _db.set_entry(proposal['race_id'], proposal['car_number'], team_id)
        except ValueError as e:
            # Proposals come from Influx, which carries races with no Postgres
            # row. Aborting here would strand every proposal after this one
            # after the operator has already answered for them.
            print(f"  {e}", file=sys.stderr)
            failed += 1
            continue
        written += 1
        alias = _prompt(f"  record {proposal['competitor_name']!r} as an alias "
                        f"of {team_id}? [y/N] ")
        if alias is None:
            break
        if alias.strip().lower() == 'y':
            try:
                _db.add_team_alias(team_id, proposal['competitor_name'])
            except ValueError as e:
                # A conflicting alias (claimed by a different team) must not
                # abort the loop: the entries already confirmed are good, and
                # remaining proposals still deserve a prompt.
                print(f"  {e}", file=sys.stderr)
    return written, failed


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
        return 1
    # Checked before the scan, not at the first write: without it an unknown
    # team scans Influx, prints proposals, prompts the operator, and only then
    # dies on an unhandled FK IntegrityError inside confirm_proposals at the
    # first accepted proposal, abandoning the rest.
    if not _team_exists(team_id):
        return 1

    with _influx.connect() as client:
        proposals = propose_entries(client.query_api(), args.terms, team_id)
    if not proposals:
        print("no unrecorded matches")
        return 0
    print(f"{len(proposals)} proposed entr"
          f"{'y' if len(proposals) == 1 else 'ies'}:")
    written, failed = confirm_proposals(proposals, team_id)
    print(f"recorded {written} entr{'y' if written == 1 else 'ies'}")
    if failed:
        print(f"{failed} accepted entr{'y' if failed == 1 else 'ies'} could "
              f"not be recorded (see above)", file=sys.stderr)
        return 1
    return 0
