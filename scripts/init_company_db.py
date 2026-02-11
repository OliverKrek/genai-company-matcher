# scripts/init_company_db.py
import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv


def initialize_database(recreate: bool = False):
    load_dotenv()
    db_path = os.getenv("DB_PATH")

    if not db_path:
        print("Error: DB_PATH not found in environment variables.")
        return

    conn = sqlite3.connect(db_path)
    try:
        # Optimization for DB inserts
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        cursor = conn.cursor()
        map_table = 'isin_lei_map'
        metadata_table = 'lei_metadata'

        if recreate:
            print("Recreating tables...")
            cursor.execute(f"DROP TABLE IF EXISTS {map_table}")
            cursor.execute(f"DROP TABLE IF EXISTS {metadata_table}")

        # Create the two tables using static methods for clean separation
        create_mapping_table(cursor, map_table)
        create_metadata_table(cursor, metadata_table)

        cursor.execute(f"SELECT COUNT(*) FROM {map_table}")
        if cursor.fetchone()[0] == 0 or recreate:
            populate_mapping_table(cursor, map_table)
            populate_metadata_table(cursor, metadata_table)

        conn.commit()
        print(f"Finished initalizing SQL database.")
    finally:
        conn.close()
    
def create_mapping_table(cursor: sqlite3.Cursor, table_name: str) -> None:
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

def create_metadata_table(cursor: sqlite3.Cursor, table_name: str) -> None:
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
            category TEXT,
            description TEXT,
            sector_labels TEXT,
            wikidata_check INTEGER DEFAULT 0,
            timestamp TEXT
        )
    """)

def populate_mapping_table(cursor: sqlite3.Cursor, table_name: str, chunk_size: int = 10000) -> None:
    csv_path = os.getenv("LEI_ISIN_PATH")
    if not csv_path or not os.path.exists(csv_path):
        print(f"Error: File {csv_path} not found.")
        return
    
    column_map = {'ISIN': 'isin', 'LEI': 'lei'}
    reader = pd.read_csv(csv_path, chunksize=chunk_size, usecols=column_map.keys())

    for chunk in reader:
        chunk = chunk.rename(columns=column_map)
        chunk.to_sql('temp_staging', cursor.connection, if_exists='replace', index=False)
        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name} (isin, lei)
            SELECT isin, lei FROM temp_staging
        """)
    
    cursor.execute("DROP TABLE IF EXISTS temp_staging")

def populate_metadata_table(cursor: sqlite3.Cursor, table_name: str):
    csv_path = os.getenv("LEI_PATH")
    if not csv_path or not os.path.exists(csv_path):
        print("Metadata CSV missing or invalid.")
        return
    
    cols_to_import = {
        'LEI': 'lei',
        'Entity.EntityStatus': 'entity_status',
        'Entity.LegalName': 'legal_name',
        'Entity.LegalAddress.City': 'city',
        'Entity.LegalAddress.Country': 'country',
        'Entity.EntityCategory': 'category',
        'Registration.RegistrationStatus': 'registration_status',
    }

    print(f"Starting metadata import...") 

    dtype_settings = {
        'LEI': str,
        'Entity.EntityStatus': str,
        'Entity.LegalName': str,
        'Entity.LegalAddress.City': str,
        'Entity.LegalAddress.Country': str,
        'Entity.EntityCategory': str,
        'Registration.RegistrationStatus': str,
    }
    reader = pd.read_csv(csv_path, chunksize=100000, usecols=cols_to_import, dtype=dtype_settings)

    for _, chunk in enumerate(reader):
        chunk = chunk.rename(columns=cols_to_import)
        chunk = chunk.dropna(subset=["lei"])
        chunk = chunk[
            (chunk['registration_status'] == 'ISSUED') &
            (chunk['entity_status'] == 'ACTIVE')
            ]
        
        chunk.to_sql('temp_meta_staging', cursor.connection, if_exists='replace', index=False)
        cols_str = ", ".join(cols_to_import.values())
        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name} ({cols_str})
            SELECT {cols_str} FROM temp_meta_staging
        """)

    cursor.execute("DROP TABLE IF EXISTS temp_staging")

if __name__ == "__main__":
    initialize_database(recreate=True)