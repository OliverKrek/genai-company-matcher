# infrastructure/sqlite_wikidata_repository.py
import sqlite3
from typing import List, Dict, Optional, Any
from datetime import datetime

from infrastructure.base_sqlite_repository import BaseSqliteRepository
from infrastructure.utils import query_wikidata
from core.models import Company

class SqliteWikidataRepository(BaseSqliteRepository):

    def get_cached_wikidata(self, lei: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            cache_row = conn.execute(
                "SELECT wikidata_id, description FROM wikidata_cache WHERE lei = ?",
                (lei,)
            ).fetchone()

            if cache_row is None:
                return None
            
            sector_rows = conn.execute(
                "SELECT sector_label, sector_qid FROM company_sectors WHERE lei = ?",
                (lei,)
            ).fetchall()

        return {
            "wikidata_id": cache_row[0],
            "description": cache_row[1],
            "sectors": [{"label": row[0], "qid": row[1]} for row in sector_rows]
        }

    def save_wikidata_information(self, lei: str, wikidata_id:str, description: str, sectors: List[Dict[str, str]]) -> None:
        with self._conn() as conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO wikidata_cache (lei, wikidata_id, description, last_updated)
                VALUES (?, ?, ?, ?)
                """,
                (lei, wikidata_id, description, datetime.now().isoformat())
            )

            conn.execute("DELETE FROM company_sectors WHERE lei = ?", (lei,))
            if sectors:
                conn.executemany(
                    "INSERT INTO company_sectors (lei, sector_label, sector_qid) VALUES (?, ?, ?)",
                    [(lei, s['label'], s['qid']) for s in sectors]
                )
    
    def set_wikidata_info(self, company: Company) -> None:
        wikidata_dict = self.get_cached_wikidata(company.lei)
        if not wikidata_dict:
            wikidata_dict = query_wikidata(company.lei)
            self.save_wikidata_information(company.lei,
                                           wikidata_dict['wikidata_id'],
                                           wikidata_dict['description'],
                                           wikidata_dict['sectors'])
        sectors = wikidata_dict.get("sectors") or []
        company.sector_labels = [sector['label'] for sector in sectors]
        company.sector_qids = [sector['qid'] for sector in sectors]


    # ---------------- DB initalization ----------------
    @classmethod
    def init_db(cls, db_path: str, *, recreate: bool = False) -> None:
        """
        Initialize the SQLite schema; optionally drop and recreate tables.

        Args:
            db_path: path where the DB should be initialized
            recreate: If True, drop existing tables and recreate them.
        """
        with sqlite3.connect(db_path) as conn:

            # Optimization for DB inserts
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = OFF")

            cursor = conn.cursor()
            wikidata_cache = 'wikidata_cache'
            company_sectors = 'company_sectors'

            if recreate:
                cursor.execute(f"DROP TABLE IF EXISTS {wikidata_cache}")
                cursor.execute(f"DROP TABLE IF EXISTS {company_sectors}") 

            # Create the two tables using static methods for clean separation
            cls._create_wikidata_cache(cursor, wikidata_cache)
            cls._create_company_sectors(cursor, company_sectors)
            print(f"Create wikidata cache and company sectors table.")
    
    @staticmethod
    def _create_wikidata_cache(cursor: sqlite3.Cursor, table_name: str) -> None:
        """Create the empty tables for the wikidata that stores metadata on companies"""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                lei TEXT PRIMARY KEY,
                wikidata_id TEXT, 
                description TEXT,
                last_updated DATETIME
            )
            """
        )

    @staticmethod
    def _create_company_sectors(cursor: sqlite3.Cursor, table_name: str) -> None:
        """Create the table that stores sector information per company."""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                lei TEXT,
                sector_label TEXT,
                sector_qid TEXT,
                FOREIGN KEY(lei) REFERENCES wikidata_cache(lei)
            )
            """
        )