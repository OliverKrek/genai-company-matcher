# tests/test_base_sqlite_repository.py
import pytest
import sqlite3
from infrastructure.base_sqlite_repository import BaseSqliteRepository

def test_constructor_stores_path():
    path = "test_db.sqlite"
    repo = BaseSqliteRepository(path)
    assert repo.db_path == path

def test_conn_returns_valid_connection():
    # We use an in memory databse to test the connection
    repo = BaseSqliteRepository(":memory:")
    conn = repo._conn()

    assert isinstance(conn, sqlite3.Connection)

    assert conn.row_factory == sqlite3.Row

    assert conn.execute("SELECT 1").fetchone()[0] == 1

    conn.close()