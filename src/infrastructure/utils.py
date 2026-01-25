# infrastructure/utils.py
import os
from typing import Dict, List
import sqlite3
import pandas as pd

def import_csv_to_sqlite(csv_path: str, table_name: str, columns_map: Dict[str, str],
                          db: sqlite3.Connection, chunk_size: int =10000) -> None:
    if not os.path.exists(csv_path):
        print(f"Error: File {csv_path} not found.")
        return
    
    chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size, usecols=columns_map.keys())

    for chunk in chunk_iter:
        chunk = chunk.rename(columns=columns_map)
        chunk.to_sql('temp_staging', db, if_exists='replace', index=False)
        db.execute(f"""
            INSERT OR IGNORE INTO {table_name} (isin, lei)
            SELECT isin, lei FROM temp_staging
        """)
        db.commit()
    
    db.execute("DROP TABLE IF EXISTS temp_staging")


