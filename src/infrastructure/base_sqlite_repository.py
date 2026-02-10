# infrastructure/base_sqlite_repository.py
import sqlite3

class BaseSqliteRepository:
    def __init__(self, db_path: str):
        """
        Initialize the repository with a path to the SQLite database file.
        
        Args:
            db_path: Path of the repository
        """
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        """Return a new SQLite connection to the configured database."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn