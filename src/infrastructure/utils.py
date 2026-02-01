# infrastructure/utils.py
import os
from typing import Dict, List, Any
import sqlite3
import pandas as pd
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def import_csv_to_sqlite(csv_path: str, table_name: str, columns_map: Dict[str, str],
                          db: sqlite3.Connection, chunk_size: int =10000) -> None:
    """
    Import ISIN/LEI data from a CSV file into a SQLite table in chunks.
    The target table is expected to have columns `isin` and `lei`.

    Args:
        csv_path: Path to the input CSV file.
        table_name: Name of the target SQLite table.
        columns_map: Mapping from CSV column names to target column names
            (e.g. `{"csv_isin": "isin", "csv_lei": "lei"}`).
        db: Open SQLite database connection.
        chunk_size: Number of rows to process per chunk.
    """
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

def _get_wikidata_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        'User-Agent': 'GenAIMatchin/1.0 (test@email.com)',
        'Accept': 'application/sparql-results+json'
    })

    return session

def query_wikidata(lei: str, max_retries: int = 3) -> Dict[str, Any]:
    """
    Queries wikidata for industry and description using LEI.

    Args:
        lei: Company identifier

    Returns:
        dict: {
            'wikidata_id': str or None,
            'description': str or None,
            'sector': [{'label': str, 'qid': str},...]
        }
    """
    url = "https://query.wikidata.org/sparql"

    query = f"""
        SELECT ?item ?itemDescription ?industry ?industryLabel WHERE {{
            ?item wdt:P1278 "{lei}".
            OPTIONAL {{ ?item wdt:P452 ?industry. }}
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        """
    result_dict = {
        "wikidata_id": None,
        "description": None,
        "sectors": []
    }

    try:
        with _get_wikidata_session() as session:
            response = session.get(url, params={'query': query}, timeout=15)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", {}).get("bindings", [])

            if not results:
                return result_dict
            
            first = results[0]

            if "item" in first:
                result_dict["wikidata_id"] = first["item"]["value"].split("/")[-1]
            
            if "itemDescription" in first:
                result_dict["description"] = first["itemDescription"]["value"]

            # Aggregate sectors (handle duplicates caused by multiple industries)
            unique_sector_qids = set()
            sectors = []
            
            for row in results:
                if "industry" in row and "industryLabel" in row:
                    qid = row["industry"]["value"].split("/")[-1]
                    if qid not in unique_sector_qids:
                        sectors.append({
                            "label": row["industryLabel"]["value"],
                            "qid": qid
                        })
                        unique_sector_qids.add(qid)
            
            result_dict["sectors"] = sectors
            return result_dict

    except requests.exceptions.RequestException as e:
        return result_dict

