"""Environment variable helpers shared across lemongrass commands."""
import os


def resolve_tokens() -> str | list[str]:
    """Return tokens from RACEMONITOR_TOKENS (comma-separated) or fall back to RACEMONITOR_TOKEN."""
    multi = os.environ.get('RACEMONITOR_TOKENS')
    if multi:
        tokens = [t.strip() for t in multi.split(',') if t.strip()]
        return tokens if len(tokens) > 1 else (tokens[0] if tokens else '')
    return os.environ.get('RACEMONITOR_TOKEN', '')
