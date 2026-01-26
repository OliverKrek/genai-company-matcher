# src/core/matching_service.py
from typing import List, Tuple
import re
import unicodedata
from core.models import Company
from core.interfaces import CompanyRepository, VectorIndex
from infrastructure.sqlite_repository import SqliteCompanyRepository
from infrastructure.vector_repository import ChromaVectorRepository

ISIN_REGEX = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')

class MatchingService:
    def __init__(self, company_repo: CompanyRepository, vector_repo: VectorIndex):
        self.company_repo = company_repo
        self.vector_repo = vector_repo
    
    def find_isin(self, isin: str) -> Company:
        return self.company_repo.get_by_isin(isin)

    def find_matches(self, isin: str, k: int) -> Tuple[List[Company], List[float]]:
        isin = self._validate_normalize_isin(isin)
        company = self.find_isin(isin=isin)
        leis, weights = self.vector_repo.retrieve_matches(company, k)
        companies = [self.company_repo.get_by_lei(lei) for lei in leis]
        return companies, weights
    
    def insert_embedding(self, isin: str) -> None:
        isin = self._validate_normalize_isin(isin)
        companies = [self.find_isin(isin)]
        self.vector_repo.upsert_embedding(companies)
        print(f"Succesfully stored embedding of: {isin}")
    
    def _validate_normalize_isin(self, isin: str) -> str:
        if not isin:
            raise ValueError("ISIN is None.")
        
        isin = unicodedata.normalize("NFKC", isin)
        isin = isin.strip()

        if not isin:
            raise ValueError("ISIN is empty after trimming.")
        
        isin = "".join(ch for ch in isin if not ch.isspace() and ch != "-")
        isin = isin.upper()

        if len(isin) != 12:
            raise ValueError(f"ISIN length mismatch, expected 12, got {len(isin)}.")
        
        if not ISIN_REGEX.match(isin):
            raise ValueError(
                "ISIN format not consistent with expected format: "
                "2 letters, 9 alphanumeric characters, and a final digit."
            )
        
        return isin
    
    @staticmethod
    def init_sqlite(db_path: str, recreate: bool = False):
        SqliteCompanyRepository.init_db(db_path, recreate=recreate)

    @staticmethod
    def init_vector_db(db_path: str, recreate: bool = False):
        ChromaVectorRepository.init_db(db_path, recreate=recreate)