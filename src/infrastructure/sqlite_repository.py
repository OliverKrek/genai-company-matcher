# infrastructure/sqlite_repository.py
import sqlite3
import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Optional
from core.models import Company
from core.interfaces import CompanyRepository
from infrastructure.base_sqlite_repository import BaseSqliteRepository

# SQL query constants
COMPANY_COLUMNS = """
    m.lei AS lei,
    m.registration_status AS registration_status,
    m.entity_status AS entity_status,
    m.legal_name AS legal_name,
    m.city AS city,
    m.country AS country,
    m.category AS category,
    m.description AS description,
    m.sector_labels AS sector_labels,
    m.wikidata_check AS wikidata_check
"""

class SqliteCompanyRepository(BaseSqliteRepository, CompanyRepository):
    """SQLite-backed implementation of the CompanyRepository interface."""

    def __init__(self, db_path):
        BaseSqliteRepository.__init__(self, db_path)

   
    def get_by_isin(self, isin: str) -> Optional[Company]:
        """
        Return the company associated with the given ISIN.
        
        Args:
            isin: ISIN for which to search
        
        Returns:
            Company: metadata structure of the company
        """
        with self._conn() as conn:
            row = conn.execute(
                f"""
                SELECT {COMPANY_COLUMNS}
                FROM isin_lei_map im
                JOIN lei_metadata m ON im.lei = m.lei
                WHERE im.isin = ?
                """,
                (isin,)
            ).fetchone()

        return Company.from_row(row) if row else None
    
    def get_by_isins(self, isins: List[str]) -> List[Company | None]:
        """
        Return a list of comapnies that match the provided list of isins.
        
        Args:
            isins: List of isins to lookup
        
        Returns:
            List[Company]: returns a list of mateches otherwise none
        """
        if not isins:
            return []
        
        isin_placeholders = ", ".join(["?"] * len(isins))
        with self._conn() as conn:            
            company_rows = conn.execute(f"""
                SELECT {COMPANY_COLUMNS}
                FROM lei_metadata m
                JOIN isin_lei_map im ON m.lei = im.lei
                WHERE im.isin IN ({isin_placeholders})
                """,
                isins,
            ).fetchall()

        return [Company.from_row(row) for row in company_rows]
    
    def get_by_lei(self, lei: str) -> Optional[Company]:
        """
        Return the company associated with the given LEI.
        
        Args:
            lei: LEI of the company to lookup
        
        Returns:
            Company: Metadata structure of the matched company
        """
        with self._conn() as conn:
            row = conn.execute(
                f"SELECT {COMPANY_COLUMNS} FROM lei_metadata m WHERE m.lei = ?",
                (lei,)
            ).fetchone()

        return Company.from_row(row) if row else None
    
    def enrich_company(self, lei: str, description: str, labels: List[str]) -> None:
        """
        Savely inserts the wikidata enrichment into the sql table

        Args:
            lei: LEI of the company
            description: Description of the company
            labels: sector labels from wikidata
        """
        sector_labels = json.dumps(labels)
        with self._conn() as conn:
            conn.execute(f"""
                UPDATE lei_metadata
                SET description = ?,
                    sector_labels = ?,
                    wikidata_check = 1,
                    timestamp = ?
                WHERE lei = ?
                """,
                (description, sector_labels, datetime.now().isoformat(), lei)
            )
    
    def list_all(self, limit: int = 100) -> List[Company]:
        """Return all companies stored in the database."""
        with self._conn() as conn:
            company_data = conn.execute(f"""
                SELECT {COMPANY_COLUMNS}
                FROM lei_metadata m
                LIMIT ?
                """,
                (limit,)
            ).fetchall()

        return [Company.from_row(row) for row in company_data]
  