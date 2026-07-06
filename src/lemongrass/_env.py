"""Environment variable helpers shared across lemongrass commands."""
import os


def resolve_tokens() -> str | list[str]:
    """Return tokens from the configured pool var (comma-separated) or fall back
    to the legacy singular RACEMONITOR_TOKEN."""
    from lemongrass import _config
    pool_var = _config.load_config().racemonitor.tokens_env
    multi = os.environ.get(pool_var)
    if multi:
        tokens = [t.strip() for t in multi.split(',') if t.strip()]
        if len(tokens) > 1:
            return tokens
        if tokens:
            return tokens[0]
    return os.environ.get('RACEMONITOR_TOKEN', '')
