# infrastructure/utils.py
import os
from typing import List

def validate_db_files(sql_path: str, vector_path: str) -> bool:
    """Fast check if database exists or not"""
    sql_exists = os.path.exists(sql_path)
    vector_exists = os.path.exists(vector_path)

    if not sql_exists:
        print(f"SQL database does not exist at path: {sql_path}")
    if not vector_exists:
        print(f"Vector database does not exist at path: {vector_path}")

    return sql_exists and vector_exists