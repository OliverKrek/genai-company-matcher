from typing import Protocol, List, Tuple
from core.models import Company

class CompanyRepository(Protocol):
    """Protocol for repositories of company metadata."""
    def get_by_isin(self, isin: str) -> Company:
        """Return the company with the given ISIN, or None if not found."""
        ...

    def get_by_isins(self, isins: List[str]) -> List[Company]:
        """Return a list of companies with the given ISINs"""
        ...

    def get_by_lei(self, lei: str) -> Company:
        """Return the company with the given Lei, or None if not found"""
        ...

    def enrich_company(self, lei: str, description: str, labels: List[str]) -> None:
        """Stores data on sector and industry in the repository."""
        ...
    
    def list_all(self, limit: int = 100) -> List[Company]:
        """Return a list of all companies in the repository."""
        ...

class VectorIndex(Protocol):
    """Protocol for interfacing with a vector database of embedded company data."""
    def upsert_embedding(self, items: List[Company]) -> None:
        """Insert or update embeddings for the given companies."""
        ...
    
    def retrieve_matches(self, item: Company, k: int) -> Tuple[List[str], List[float]]:
        """Return up to k matching items and their similarity scores."""
        ...