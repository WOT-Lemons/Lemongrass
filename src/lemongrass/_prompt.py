"""Terminal confirmation prompts, with one policy for end of input.

Three commands ask the operator to confirm something — `races prune`,
`entries propose`, and race-backfill's offer to persist changed settings — and
all three have to decide what a closed stdin means. Ctrl-D, a redirect from
/dev/null, and cron all raise EOFError out of input(); left unhandled that is a
traceback, and answered independently in each command it is three policies that
drift apart.

The policy: end of input is never a yes. Whether it is *merely* a no or a
reportable condition is the caller's call, which is why there are two
functions — ask() hands back the distinction, ask_yes() throws it away.
"""


def ask(text):
    """Prompt for a line. Returns None at end of input, never raising.

    The tri-state form, for callers that must tell "the operator said no"
    apart from "there was no operator" — a run that answered nothing usually
    wants a nonzero exit rather than a silent success.
    """
    try:
        return input(text)
    except EOFError:
        # input() echoes no newline when it hits EOF, so without this the
        # shell prompt resumes on the half-written prompt line.
        print()
        return None


def ask_yes(text):
    """One y/N prompt as a bool. End of input counts as no."""
    answer = ask(text)
    return answer is not None and answer.strip().lower() in ('y', 'yes')
