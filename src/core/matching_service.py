# core/matching_service.py
from typing import List, Tuple
from core.models import Company
from core.interfaces import CompanyRepository, VectorIndex
from infrastructure.sqlite_repository import SqliteCompanyRepository
from infrastructure.vector_repository import ChromaVectorRepository


class MatchingService:
    def __init__(self, company_repo: CompanyRepository, vector_repo: VectorIndex):
        self.company_repo = company_repo
        self.vector_repo = vector_repo
    
    def find_isin(self, isin: str) -> Company:
        return self.company_repo.get_by_isin(isin)

    def find_matches(self, isin: str, k: int) -> Tuple[List[Company], List[float]]:
        company = self.find_isin(isin=isin)
        leis, weights = self.vector_repo.retrieve_matches(company, k)
        companies = [self.company_repo.get_by_lei(lei) for lei in leis]
        return companies, weights
    
    def insert_embedding(self, isin: str) -> None:
        companies = [self.find_isin(isin)]
        self.vector_repo.upsert_embedding(companies)
        print(f"Succesfully stored embedding of: {isin}")
    
    @staticmethod
    def init_sqlite(db_path: str, recreate: bool = False):
        SqliteCompanyRepository.init_db(db_path, recreate=recreate)

    @staticmethod
    def init_vector_db(db_path: str, recreate: bool = False):
        ChromaVectorRepository.init_db(db_path, recreate=recreate)