# infrastructure/sqlite_repository.py
import sqlite3
import os
import pandas as pd
from typing import List, Optional, Any, Dict
from core.models import Company
from core.interfaces import CompanyRepository
from infrastructure.utils import import_csv_to_sqlite
from infrastructure.base_sqlite_repository import BaseSqliteRepository

# SQL query constants
COMPANY_COLUMNS = """
    m.lei AS lei,
    m.registration_status AS registration_status,
    m.entity_status AS entity_status,
    m.legal_name AS legal_name,
    m.city AS city,
    m.country AS country,
    m.category AS category
"""

class SqliteCompanyRepository(BaseSqliteRepository, CompanyRepository):
    """SQLite-backed implementation of the CompanyRepository interface."""

    def __init__(self, db_path):
        BaseSqliteRepository.__init__(self, db_path)
   
    def get_by_isin(self, isin: str) -> Company:
        """
        Return the company associated with the given ISIN.
        
        Args:
            isin: ISIN for which to search
        
        Returns:
            Company: metadata structure of the company

        Raises:
            ValueError if no matching value found.
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

            if row is None:
                raise ValueError(f"No company metadata found for ISIN {isin}")
            
            return Company.from_row(row)
    
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
    
    def get_by_lei(self, lei: str) -> Company:
        """
        Return the company associated with the given LEI.
        
        Args:
            lei: LEI of the comapny to lookup
        
        Returns:
            Company: Metadata structure of the matched company

        Raises:
            ValueError if no matching value is found.
        """
        with self._conn() as conn:
            company_data = conn.execute(
                f"SELECT {COMPANY_COLUMNS} FROM lei_metadata WHERE lei = ?",
                (lei,)
            ).fetchone()

            if company_data is None:
                raise ValueError(f"Found no company data matching to LEI: {lei}")
           
        return Company.from_row(company_data)
    
    def list_all(self):
        """Return all companies stored in the database."""
        raise NotImplementedError
   
   # ---------------- DB initalization ----------------
    @classmethod
    def init_db(cls, db_path: str, *, recreate: bool = False) -> None:
        """
        Initialize the SQLite schema; optionally drop and recreate tables.

        Args:
            dp_path: path where the bp should be initalized
            recreate: Flag whether data should be dropped and tables are freshly created.
        """
        conn = sqlite3.connect(db_path)
        try:
            # Optimization for DB inserts
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = OFF")

            cursor = conn.cursor()
            map_table = 'isin_lei_map'
            metadata_table = 'lei_metadata'

            if recreate:
                cursor.execute(f"DROP TABLE IF EXISTS {map_table}")
                cursor.execute(f"DROP TABLE IF EXISTS {metadata_table}")

            # Create the two tables using static methods for clean separation
            cls._create_mapping_table(cursor, map_table)
            print(f"Created mapping table.")
            cls._create_metadata_table(cursor, metadata_table)
            print(f"Created metadata table.")
        finally:
            conn.close()
        
   
    @staticmethod
    def _create_mapping_table(cursor: sqlite3.Cursor, table_name: str) -> None:
        """
        Create the ISIN–LEI mapping table and optionally import data from CSV.
        
        Args:
            cursor: Cursor object that can execute queries.
            table_name: Table on which to execute queries.
        """
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                isin TEXT PRIMARY KEY,
                lei TEXT
            )
        """)
        cursor.connection.commit()
        column_map = {'ISIN': 'isin', 'LEI': 'lei'}
        csv_path = os.getenv("LEI_ISIN_PATH")
        if csv_path:            
            import_csv_to_sqlite(csv_path, table_name, column_map, cursor.connection)
    
    @staticmethod
    def _create_metadata_table(cursor: sqlite3.Cursor, table_name: str) -> None:
        """
        Create the LEI metadata table and populate it from the LEI CSV file.
        
        Args:
            cursor: Cursor object that can execute queries.
            table_name: Table on which to execute queries.
        """
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                lei TEXT PRIMARY KEY,
                registration_status TEXT,
                entity_status TEXT,
                legal_name TEXT,
                city TEXT,
                country TEXT,
                category TEXT
            )
        """)
        conn = cursor.connection
        conn.commit()

        cols_to_import = [
            'LEI',
            'Entity.EntityStatus',
            'Entity.LegalName',
            'Entity.LegalAddress.City',
            'Entity.LegalAddress.Country',
            'Entity.EntityCategory',
            'Registration.RegistrationStatus',
        ]

        print(f"Starting metadata import... this may take a few minutes.")

        dtype_settings = {
            'LEI': str,
            'Entity.EntityStatus': str,
            'Entity.LegalName': str,
            'Entity.LegalAddress.City': str,
            'Entity.LegalAddress.Country': str,
            'Entity.EntityCategory': str,
            'Registration.RegistrationStatus': str,
        }

        csv_path = os.getenv("LEI_PATH")
        if not csv_path or not os.path.exists(csv_path):
            print("Metadata CSV missing or invalid.")
            
        reader = pd.read_csv(csv_path, chunksize=100000, usecols=cols_to_import, dtype=dtype_settings)

        for _, chunk in enumerate(reader):
            print(f"Header names: {chunk.columns}")
            chunk = chunk.rename(columns={
                'LEI': 'lei',
                'Entity.EntityStatus': 'entity_status',
                'Entity.LegalName': 'legal_name',
                'Entity.LegalAddress.City': 'city',
                'Entity.LegalAddress.Country': 'country',
                'Entity.EntityCategory': 'category',
                'Registration.RegistrationStatus': 'registration_status',
            })

            chunk = chunk.dropna(subset=["lei"])
            chunk = chunk.replace({"": None})
            chunk = chunk[
                (chunk['registration_status'] == 'ISSUED') &
                (chunk['entity_status'] == 'ACTIVE')
                ]

            with conn:
                chunk.to_sql('temp_staging', conn, if_exists='replace', index=False)
                columns = "lei, registration_status, entity_status, legal_name, city, country, category"
                conn.execute(f"""
                    INSERT OR IGNORE INTO {table_name} ({columns})
                    SELECT {columns} FROM temp_staging
                """)

        conn.execute("DROP TABLE IF EXISTS temp_staging")
        conn.commit()