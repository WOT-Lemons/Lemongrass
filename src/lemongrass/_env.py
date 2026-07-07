"""Environment variable helpers shared across lemongrass commands."""
import os

_LEGACY_TOKEN_VAR = 'RACEMONITOR_TOKEN'


def _pool_var() -> str:
    from lemongrass import _config
    return _config.load_config().racemonitor.tokens_env


def _legacy_applies(pool_var: str) -> bool:
    # The legacy singular var is honored only alongside the default pool var; a
    # deployment that names its own pool var must not pick up a stale leftover.
    from lemongrass import _config
    return pool_var == _config.RaceMonitorConfig().tokens_env


def resolve_tokens() -> str | list[str]:
    """Return tokens from the configured pool var (comma-separated) or fall back
    to the legacy singular RACEMONITOR_TOKEN."""
    pool_var = _pool_var()
    multi = os.environ.get(pool_var)
    if multi:
        tokens = [t.strip() for t in multi.split(',') if t.strip()]
        if len(tokens) > 1:
            return tokens
        if tokens:
            return tokens[0]
    if _legacy_applies(pool_var):
        return os.environ.get(_LEGACY_TOKEN_VAR, '')
    return ''


def tokens_env_hint() -> str:
    """Name the env var(s) consulted by resolve_tokens(), for error messages."""
    pool_var = _pool_var()
    if _legacy_applies(pool_var):
        return f"{pool_var} or {_LEGACY_TOKEN_VAR}"
    return pool_var
