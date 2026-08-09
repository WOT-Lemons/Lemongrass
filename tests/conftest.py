# obd and race_monitor are real, installed dependencies that import with no
# hardware or network side effects (only OBD/Async construction and
# RaceMonitorClient calls touch the outside world). Tests mock those I/O seams
# individually, so no suite-wide module mock is needed here.
import os

import pytest


@pytest.fixture(scope="session")
def postgres_url():
    """SQLAlchemy URL for a scratch PostgreSQL, or skip if none is configured.

    Set LEMONGRASS_TEST_DATABASE_URL to run the database-backed tests. CI always
    sets it; locally it is optional so the suite stays runnable without Docker.
    """
    url = os.environ.get("LEMONGRASS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("LEMONGRASS_TEST_DATABASE_URL not set")
    return url


@pytest.fixture
def clean_db(postgres_url):
    """An Engine pointed at an empty public schema, torn down after the test.

    Dropping and recreating the schema is faster and more thorough than
    truncating: it also removes the alembic_version table, which the migration
    tests need gone between runs.

    Requires LEMONGRASS_TEST_DB_ALLOW_WIPE=1 as an explicit, conscious opt-in.
    LEMONGRASS_TEST_DATABASE_URL alone is not enough to trust: the URL a
    developer would naturally copy out of .github/workflows/pytest.yml
    (postgresql+psycopg://lemongrass:local-dev-password@localhost:5432/lemongrass)
    is character-for-character identical to the local-testing docker-compose
    stack's Postgres — same host, port, credentials, and database name — so
    pointing the test suite at it without a second, distinct confirmation
    would silently wipe a developer's local races and sessions. A
    name-substring check on the database name would not catch this, since the
    colliding name is literally "lemongrass" in both places.
    """
    if not os.environ.get("LEMONGRASS_TEST_DB_ALLOW_WIPE"):
        pytest.fail(
            "clean_db drops and recreates the public schema on the database "
            "named by LEMONGRASS_TEST_DATABASE_URL. Refusing to run: set "
            "LEMONGRASS_TEST_DB_ALLOW_WIPE=1 to confirm that database is a "
            "disposable test instance, not the local-testing stack. See "
            "CONTRIBUTING.md for how to start a throwaway Postgres for these "
            "tests."
        )
    from sqlalchemy import create_engine, text
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    try:
        yield engine
    finally:
        engine.dispose()
