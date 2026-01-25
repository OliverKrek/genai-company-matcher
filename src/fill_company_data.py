import sqlite3
import os
import pandas as pd
from typing import Dict

# paths
db_path = './databases/gleif_data.db'
csv_path = './data/golden-copy.csv'

# check if stuff exists
if not os.path.isfile(csv_path):
    print(f"CSV file doesnt exist. Download LEI data first.")
    exit

if not os.path.exists(db_path):
    print("SQL database doesnt exist. Create it first.")
    exit

db = sqlite3.connect(db_path)
db.execute("DROP TABLE IF EXISTS lei_metadata")
db.commit()

db.execute("""
CREATE TABLE IF NOT EXISTS lei_metadata (
    lei TEXT PRIMARY KEY,
    registration_status TEXT,
    entity_status TEXT,
    legal_name TEXT,
    city TEXT,
    country TEXT,
    category TEXT
)
""")
db.commit()

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

reader = pd.read_csv(csv_path, chunksize=100000, usecols=cols_to_import, dtype=dtype_settings)


processed = 0

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

    with db:
        chunk.to_sql('lei_metadata', db, if_exists='append', index=False)

    processed += len(chunk)
    print(f"Processed ~{processed:,} LEIs")

db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_lei_metadata ON lei_metadata(lei)")
db.commit()

isin = 'CH0244767585'
query = """
SELECT m.legal_name, m.city, m.country
FROM isin_lei_map map
JOIN lei_metadata m ON map.lei = m.lei
WHERE map.isin = ?
"""
df = pd.read_sql_query(query, db, params=(isin,))
print(df)

db.close()

print("Metadata import complete")
