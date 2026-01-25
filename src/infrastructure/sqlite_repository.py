# infrastructure/sqlite_repository.py
import sqlite3
import os
import pandas as pd
from typing import List
from core.models import Company
from core.interfaces import CompanyRepository
from infrastructure.utils import import_csv_to_sqlite

class SqliteCompanyRepository(CompanyRepository):
    def __init__(self, db_path: str):
        super().__init__()
        self.db_path = db_path

    def _conn(self):
        return sqlite3.connect(self.db_path)
    
    def get_by_isin(self, isin):
        with self._conn() as conn:
            row = conn.execute(
                "SELECT lei FROM isin_lei_map WHERE isin = ?",
                (isin,)
            ).fetchall()

            company_data = conn.execute(
                "SELECT lei, registration_status, entity_status, legal_name, city, country, category FROM lei_metadata WHERE lei = ?",
                (row[0][0],)
            ).fetchall()
            company_data = company_data[0]
            company = Company(company_data[0], company_data[1], company_data[2],
                              company_data[3], company_data[4], company_data[5], company_data[6])
        return company
    
    def get_by_lei(self, lei: str) -> Company:
        with self._conn() as conn:
            company_data = conn.execute(
                "SELECT lei, registration_status, entity_status, legal_name, city, country, category FROM lei_metadata WHERE lei = ?",
                (lei,)
            ).fetchall()
            company_data = company_data[0]
            company = Company(company_data[0], company_data[1], company_data[2],
                              company_data[3], company_data[4], company_data[5], company_data[6])
        return company
    
    def list_all(self):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT isin, name FROM companies"
            ).fetchall()
        return NotImplementedError

    # ---------------- DB initalization ----------------
    @classmethod
    def init_db(cls, db_path: str, *, recreate: bool = False) -> None:
        """
        Initializes the SQLite database: create tables, indexes, etc.
        Idempotent by default: set recreate = True to drop and recreate.
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

        for i, chunk in enumerate(reader):
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