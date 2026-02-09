# infrastructure/sqlite_repository.py
import sqlite3
import os
import pandas as pd
from typing import List, Optional, Any, Dict
from datetime import datetime
from core.models import Company
from core.interfaces import CompanyRepository
from infrastructure.utils import import_csv_to_sqlite
from infrastructure.utils import query_wikidata

# TODO: do proper aliasing for the table in the queries
# TODO: Split the repo into proper tasks. Base sqlite repo and one class for company repo and one for wikidata.

# SQL query constants
COMPANY_COLUMNS = """
    lei,
    registration_status,
    entity_status,
    legal_name,
    city,
    country,
    category
"""

SELECT_COMPANY_BY_LEI = f"""
    SELECT {COMPANY_COLUMNS}
    FROM lei_metadata
    WHERE lei = ?
"""

class SqliteCompanyRepository(CompanyRepository):
    """SQLite-backed implementation of the CompanyRepository interface."""

    def __init__(self, db_path: str):
        """
        Initialize the repository with a path to the SQLite database file.
        
        Args:
            db_path: Path of the repository
        """
        super().__init__()
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        """Return a new SQLite connection to the configured database."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
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

        with self._conn() as conn:
            isin_placeholders = ", ".join(["?"] * len(isins))
            company_rows = conn.execute(f"""
                SELECT {COMPANY_COLUMNS}
                FROM lei_metadata m
                JOIN isin_lei_map im ON m.lei = im.lei
                WHERE isin IN ({isin_placeholders})
                """,
                isins,
            ).fetchall()

            companies = [Company.from_row(row) for row in company_rows]
            return companies
    
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
    
    def set_wikidata_info(self, company: Company) -> None:
        wikidata_dict = self.get_cached_wikidata(company.lei)
        if not wikidata_dict:
            wikidata_dict = query_wikidata(company.lei)
            self.save_wikidata_information(company.lei,
                                           wikidata_dict['wikidata_id'],
                                           wikidata_dict['description'],
                                           wikidata_dict['sectors'])
            company.sector_labels = [sector['label'] for sector in (wikidata_dict['sectors'] or [])]
            company.sector_qids = [sector['qid'] for sector in (wikidata_dict['sectors'] or [])]
    
    def get_cached_wikidata(self, lei: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            cache_row = conn.execute(
                "SELECT wikidata_id, description FROM wikidata_cache WHERE lei = ?",
                (lei,)
            ).fetchone()

            if not cache_row:
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

    def save_wikidata_information(self, lei: str, wikidata_id:str, description: str, sectors: List[Dict[str, str]]):
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
            conn.commit()

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
            wikidata_cache = 'wikidata_cache'
            company_sectors = 'company_sectors'

            if recreate:
                cursor.execute(f"DROP TABLE IF EXISTS {map_table}")
                cursor.execute(f"DROP TABLE IF EXISTS {metadata_table}")
                cursor.execute(f"DROP TABLE IF EXISTS {wikidata_cache}")
                cursor.execute(f"DROP TABLE IF EXISTS {company_sectors}")

            # Create the two tables using static methods for clean separation
            cls._create_mapping_table(cursor, map_table)
            print(f"Created mapping table.")
            cls._create_metadata_table(cursor, metadata_table)
            print(f"Created metadata table.")
            cls._create_wikidata_cache(cursor, wikidata_cache)
            print(f"Create wikidata cache table.")
            cls._create_company_sector(cursor, company_sectors)
            print(f"Created company sectors mapping table.")
        finally:
            conn.close()
        
    @staticmethod
    def _create_wikidata_cache(cursor: sqlite3.Cursor, table_name: str) -> None:
        """Create the empty tables for the wikidata that stores metadata on companies"""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                lei TEXT PRIMARY KEY,
                wikidata_id TEXT, --
                description TEXT,
                last_updated DATETIME
            )
        """)
        cursor.connection.commit()

    def _create_company_sector(cursor: sqlite3.Cursor, table_name: str) -> None:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                lei TEXT,
                sector_label TEXT,
                sector_qid TEXT,
                FOREIGN KEY(lei) REFERENCES wikidata_cache(lei)
            )
        """)
        cursor.connection.commit()
    
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