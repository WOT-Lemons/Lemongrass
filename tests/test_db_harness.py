from sqlalchemy import text


def test_clean_db_is_reachable_and_empty(clean_db):
    with clean_db.begin() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1
        tables = conn.execute(text(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'public'"
        )).scalar()
    assert tables == 0
