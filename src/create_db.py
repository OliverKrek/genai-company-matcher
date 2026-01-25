"""
This file takes an exisiting GLEI file and stores the data in a json?
"""
import sqlite3
import pandas as pd
import os
from typing import Dict

# functions 
def import_csv_to_sqlite(csv_path: str, table_name: str, columns_map: Dict[str, str], db: sqlite3.Connection) -> None:
    if not os.path.exists(csv_path):
        print(f"Error: File {csv_path} not found.")
        return
    
    chunk_iter = pd.read_csv(csv_path, chunksize=10000, usecols=columns_map.keys())

    for chunk in chunk_iter:
        chunk = chunk.rename(columns=columns_map)
        chunk.to_sql('temp_staging', db, if_exists='replace', index=False)
        db.execute("""
            INSERT OR IGNORE INTO {table_name} (isin, lei)
            SELECT isin, lei FROM temp_staging
        """)
        db.commit()

# Path and names
db_folder = './databases'
db_path = os.path.join(db_folder, 'gleif_data.db')
csv_path = './data/lei-isin-20260124T081546.csv'
table_name = 'isin_lei_map'

if not os.path.exists(db_folder):
    os.makedirs(db_folder)

try:
    db = sqlite3.connect(db_path)
    cursor_obj = db.cursor()

    column_map = {'ISIN': 'isin', 'LEI': 'lei'}

    # creat table
    cursor_obj.execute("""
        CREATE TABLE IF NOT EXISTS {table_name} (
            isin TEXT PRIMARY KEY
            lei TEXT
        )
    """)

    # Run import 
    import_csv_to_sqlite(csv_path, table_name, column_map, db)

    query = f"SELECT lei from {table_name} WHERE isin = ?"
    ubs_isin = 'CH0244767585'
    cursor_obj.execute(query, (ubs_isin,))

    result = cursor_obj.fetchall()
    print(f"Result for {ubs_isin}: {result}")

except sqlite3.Error as e:
    print(f"Database error: {e}")
finally:
    if 'db' in locals():
        db.close()


