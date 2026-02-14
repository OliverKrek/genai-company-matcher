import pytest
import sqlite3
from infrastructure.sqlite_repository import SqliteCompanyRepository

@pytest.fixture
def repo():
    # Use an in memory database to do the test.
    test_db_uri = "file:testdb?mode=memory&cache=shared"

    keep_alive = sqlite3.connect(test_db_uri, uri=True)

    keep_alive.execute(f"""
        CREATE TABLE IF NOT EXISTS isin_lei_map(
            isin TEXT PRIMARY KEY,
            lei TEXT
        )
    """)

    keep_alive.execute(f"""
        CREATE TABLE IF NOT EXISTS lei_metadata (
            lei TEXT PRIMARY KEY,
            registration_status TEXT,
            entity_status TEXT,
            legal_name TEXT,
            city TEXT,
            country TEXT,
            category TEXT,
            description TEXT,
            sector_labels TEXT,
            wikidata_check INTEGER DEFAULT 0,
            timestamp TEXT
        )
    """)

    keep_alive.commit()

    # Let the company repo point to the in memory database
    company_repo = SqliteCompanyRepository(test_db_uri)

    yield company_repo

    keep_alive.close()

    