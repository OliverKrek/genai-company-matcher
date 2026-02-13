# infrastructure/vector_repository.py
import os
import pandas as pd
import chromadb
from chromadb.utils import embedding_functions
from typing import List, Tuple
from core.interfaces import VectorIndex
from core.models import Company

os.environ["TOKENIZERS_PARALLELISM"] = "false"

class ChromaVectorRepository(VectorIndex):
    """Vector index implementation backed by a persistent ChromaDB collection."""

    def __init__(self, db_path: str, model: str = "all-MiniLM-L6-v2", collection_name: str = "companies", distance: str = "cosine"):
        """
        Initialize the repository with a ChromaDB client at the given path.
        
        Args:
            dp_path: Path of the vector database.
            model: name of the SentenceTransformer model which generates the embeddings.
            collection_name: Name of the vector embedding collection.
            distance: Distance metric to measure similarity between embeddings.
        """
        super().__init__()
        self.client = chromadb.PersistentClient(db_path)
        self.collection_name = collection_name
        embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=model)     
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            embedding_function=embedding_function,
            metadata={"embedding_model": model,
                      "hnsw:space": distance},
        )

        existing_metadata = self.collection.metadata or {}
        stored_model = existing_metadata.get("embedding_model")

        if stored_model is not None and stored_model != model:
            raise ValueError(f"Embedding model: {model} inconsistent with existing collection: {stored_model}. Change collection name or model.")
        
        print(f"Loaded collection with {self.collection.count()} items.") 

    def upsert_embedding(self, items: List[Company]) -> None:
        """
        Insert or update embeddings for the given companies in the collection.

        Each company is stored using its LEI as the ID and the text returned
        by `Company.embedding_text()` as the document.

        Args:
            items: List of companies for which to generate embeddings
        """
        self.collection.upsert(
            ids=[item.lei for item in items],
            documents=[item.embedding_text() for item in items],
        )
    
    def retrieve_matches(self, item: Company, k: int) -> Tuple[List[str], List[float]]:
        """
        Return up to k closest matches for the given company.

        The method queries the collection using the company's embedding text and
        returns the matched item IDs (LEIs) and their distances.

        Args:
            item: Company used as the query reference.
            k: Maximum number of matches to return.

        Returns:
            A tuple containing:
                - A list of matched company IDs (LEIs).
                - A list of the corresponding distances.
        """
        results = self.collection.query(
            query_texts=[item.embedding_text()],
            n_results=k,
            include=["distances"]
        )
        return results["ids"][0], results["distances"][0]
   