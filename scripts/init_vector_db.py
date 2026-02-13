# scripts/init_vector_db.py
import chromadb
import os
from dotenv import load_dotenv
from chromadb.utils import embedding_functions


def initialize_vectordb(recreate: bool = False, model: str = "all-MiniLM-L6-v2", distance: str = "cosine") -> None:
    """
    Initialize the vector database at the given path.

    Create the `companies` collection if it does not exist. Optionally
    delete and recreate the collection when `recreate` is True.

    Args:
        db_path: Path to the ChromaDB storage.
        recreate: If True, delete any existing `companies` collection
            and create a new one.
    """
    load_dotenv()
    db_path = os.getenv("VECTOR_DB_PATH")

    if not db_path:
        print("Error: VECTOR_DB_PATH not found in environment variables.")

    client = chromadb.PersistentClient(db_path)

    if recreate:
        try:
            client.delete_collection(name="companies")
            print(f"Deleting existing companies database.")
        except Exception:
            print("No exisiting collection to delete. Moving on...")
    
    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=model)
    client.get_or_create_collection(
        name="companies",
        embedding_function=embedding_function,
        metadata={"embedding_model": model,
                    "hnsw:space": distance},
    )   
    print(f"Finished initializing vector databse at {db_path}")

if __name__ == "__main__":
    initialize_vectordb(recreate=True)
     