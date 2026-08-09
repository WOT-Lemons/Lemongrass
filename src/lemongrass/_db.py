"""PostgreSQL engine and connection handling for lemongrass.

Mirrors the shape of ``_influx``: settings come from the config layer, the
secret is read from the env var the config names, and nothing connects until a
caller asks. Unlike ``_influx`` the engine is built lazily rather than at import
— every command imports the CLI, and a command that never touches the database
must not require a password to be set.

All SQL statements belong in this module. Keeping that boundary is what would
make a later move to the SQLAlchemy ORM a one-module change.
"""
import logging
import os
import sys
from contextlib import contextmanager

from lemongrass import _config

_engine = None


def database_url():
    """Build the SQLAlchemy URL from config plus the configured password env var.

    Logs an error and exits with status 1 when the password variable is unset,
    matching ``_influx.connect``'s handling of a missing token.
    """
    from sqlalchemy import URL
    cfg = _config.load_config().postgres
    password = os.environ.get(cfg.password_env)
    if not password:
        logging.error("%s environment variable not set", cfg.password_env)
        sys.exit(1)
    return URL.create(
        'postgresql+psycopg',
        username=cfg.user,
        password=password,
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
    )


def engine():
    """Return the process-wide Engine, creating it on first use.

    ``pool_pre_ping`` is on because the monitor runs for hours: an idle pooled
    connection reaped by the server or a NAT timeout becomes a transparent
    reconnect instead of a mid-race exception.
    """
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(database_url(), pool_pre_ping=True)
    return _engine


def reset_engine():
    """Dispose and forget the memoized engine (tests, and config changes)."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


@contextmanager
def connect():
    """Yield a Connection inside a transaction, committing on clean exit.

    Rolls back if the body raises, so a partial write is never left behind.
    """
    with engine().begin() as conn:
        yield conn


def db_password_present():
    """True iff the configured database password env var is set.

    Cheap check (no connection, no sys.exit) so a TUI can validate up front and
    surface a clean error instead of a worker-thread SystemExit.
    """
    return bool(os.environ.get(_config.load_config().postgres.password_env))


def alembic_config(url=None):
    """Build an Alembic Config pointed at the migrations shipped in this package.

    Resolving ``script_location`` through the package directory is what lets
    ``lemongrass db upgrade`` work from an installed wheel or a container, with
    no checkout and no alembic.ini on disk.
    """
    from pathlib import Path

    from alembic.config import Config
    # Config's `stdout` parameter defaults to `sys.stdout` bound once, at the
    # time alembic.config is first imported — not at each call. Pass it
    # explicitly so output goes to whatever sys.stdout currently is (tests
    # rely on this to land in capsys's per-test buffer).
    cfg = Config(stdout=sys.stdout)
    cfg.set_main_option('script_location',
                        str(Path(__file__).parent / 'migrations'))
    if url is None:
        url = database_url()
    # A URL object hides the password in str(); a caller-supplied string is
    # already rendered. Accept either so tests can pass a plain URL string.
    if hasattr(url, 'render_as_string'):
        url = url.render_as_string(hide_password=False)
    cfg.set_main_option('sqlalchemy.url', str(url))
    return cfg
