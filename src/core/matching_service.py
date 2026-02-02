# src/core/matching_service.py
from typing import List, Tuple, Union
import re
import unicodedata
from core.models import Company
from core.interfaces import CompanyRepository, VectorIndex
from infrastructure.sqlite_repository import SqliteCompanyRepository
from infrastructure.vector_repository import ChromaVectorRepository
from infrastructure.utils import query_wikidata

ISIN_REGEX = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')

class MatchingService:
    """Provide ISIN-based company lookup and vector-similarity matching."""

    def __init__(self, company_repo: CompanyRepository, vector_repo: VectorIndex):
        """
        Initialize the service with company and vector repositories.

        Args:
            company_repo: instance of a database to lookup company metadata.
            vector_repo: instance of a vector db to lookup embeddings.

        """
        self.company_repo = company_repo
        self.vector_repo = vector_repo
    
    def find_by_isin(self, isin: str) -> Company:
        """
        Return the company associated with the given ISIN.

        Args:
            isin: The company ISIN to lookup metadata

        Returns:
            Company: datastructure that contains the metadata.
        """
        return self.company_repo.get_by_isin(isin)
    
    def find_by_lei(self, lei: str) -> Company:
        """
        Return the company associated with the given ISIN.

        Args:
            lei: The company lei to lookup metadata.

        Return:
            Company: data structure that contains the metadata
        """
        return self.company_repo.get_by_lei(lei)

    def find_matches(self, isin: str, k: int) -> Tuple[List[Company], List[float]]:
        """
        Return up to k companies most similar to the company with the given ISIN.

        Args:
            isin: ISIN of the company to find matches for
            k: Number of matches to return
        
        Returns:
            Tuple:
                - List[Company]: List of company metadata corresponding to the matches.
                - List[float]: List of weights correspoding to the mathes.
        """
        isin = self._validate_normalize_isin(isin)
        company = self.find_isin(isin=isin)
        leis, weights = self.vector_repo.retrieve_matches(company, k)
        companies = [self.company_repo.get_by_lei(lei) for lei in leis]
        return companies, weights
    
    def insert_embedding(self, isins: Union[List[str], str] ) -> None:
        """
        Insert or update the vector embedding for the company with the given ISIN.

        Args:
            isins: list or single isin to embed in the vector DB
        """
        # Calls are delegated to internal functions
        if isinstance(isins, list):
            self._insert_embeddings(isins)
        else:
            self._insert_embedding(isins)
    

    def _insert_embedding(self, isin: str) -> None:
        isin = self._validate_normalize_isin(isin)
        company= self.find_isin(isin)

        if not company.validate():
            raise NotImplementedError
        
        self.vector_repo.upsert_embedding(company)
        '''
        # get the company corresponding to the isin. If there is 
        if not companies[0].sector_labels:
            print(f"Fetching metadata from wikidata for: {isin}")
            # Call function dict
            wikidata_response = query_wikidata(companies[0].lei)
            print(wikidata_response)
            self.company_repo.save_wikidata_information(companies[0].lei, wikidata_response['wikidata_id'],
                                                        wikidata_response['description'],
                                                        wikidata_response['sectors'])
            companies[0].sector_labels = wikidata_response['sectors'][0]['label']
            companies[0].sector_qids = wikidata_response['sectors'][0]['qid']

        self.vector_repo.upsert_embedding(companies)
        '''
        print(f"Succesfully stored embedding of: {isin}")

    def _insert_embeddings(self, isins: List[str]) -> None:
        raise NotImplementedError
    
    def _validate_normalize_isin(self, isin: str) -> str:
        """
        Validate and normalize an ISIN string.

        Args:
            isin: ISIN to validate and normalize

        Raise:
            ValueError: If the ISIN is missing, becomes empty after cleaning,
                has an invalid length, or does not match the expected format.
        """
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
        """
        Initialize the SQLite-backed company repository database.

        Args:
            db_path: Path to the SQLite database file.
            recreate: If True, drop and recreate the database schema.
        """
        SqliteCompanyRepository.init_db(db_path, recreate=recreate)

    @staticmethod
    def init_vector_db(db_path: str, recreate: bool = False):
        """
        Initialize the vector database used for company embeddings.

        Args:
            db_path: Path to the vector database storage (e.g., directory).
            recreate: If True, drop and recreate the vector index.
        """
        ChromaVectorRepository.init_db(db_path, recreate=recreate)