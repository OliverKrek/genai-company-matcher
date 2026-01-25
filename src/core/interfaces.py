from typing import Protocol, List, Tuple
from core.models import Company

class CompanyRepository(Protocol):
    def get_by_isin(self, isin: str) -> Company | None:
        ...
    
    def list_all(self) -> List[Company]:
        ...

class VectorIndex(Protocol):
    def upsert_embedding(self, items: List[Company]) -> None:
        ...
    
    def retrieve_matches(self, item: Company, k: int) -> Tuple[List[str], List[float]]:
        ...