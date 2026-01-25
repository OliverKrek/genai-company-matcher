# infrastructure/vector_repository.py
import os
import pandas as pd
import chromadb
from typing import List, Tuple
from core.interfaces import VectorIndex
from core.models import Company

class ChromaVectorRepository(VectorIndex):
    def __init__(self, db_path: str):
        super().__init__()
        self.client = chromadb.PersistentClient(db_path)
        self.collection = self.client.get_or_create_collection(name="companies")
        print(f"Loaded collection with {self.collection.count()} items.") 
    
    def upsert_embedding(self, items: List[Company]) -> None:
        self.collection.upsert(
            ids=[item.lei for item in items],
            documents=[item.embedding_text() for item in items],
        )
    
    def retrieve_matches(self, item: Company, k: int) -> Tuple[List[str], List[float]]:
        results = self.collection.query(
            query_texts=[item.embedding_text()],
            n_results=k,
            include=["distances"]
        )
        return results["ids"][0], results["distances"][0]
    
    @classmethod
    def init_db(cls, db_path: str, *, recreate: bool = False) -> None:
        """
        Initializes the vector db using the path provided in db_path.
        """
        client = chromadb.PersistentClient(path=db_path)

        if recreate:
            try:
                client.delete_collection(name="companies")
                print(f"Deleting existing companies database.")
            except Exception:
                print("No exisiting collection to delete. Moving on...")
        
        client.get_or_create_collection(name="companies")
        print(f"Finished initializing vector databse at {db_path}")