# src/core/matching_service.py
from typing import List, Tuple, Union
import re
import unicodedata
from core.models import Company
from core.interfaces import VectorIndex
from core.enrichment_service import EnrichmentService

ISIN_REGEX = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
"""
The matching service provides ISIN/LEI based lookup of company data using a vector-embedding approach.

The class contains a company repository (company_repo) and a vector repository (vector_repo).

There is a minimal public interface that exposes the core functionality:
    - find_by_isin()
    - find_by_lei()
    - insert_embedding()
    - find_matches()

"""

class MatchingService:
    """Provide ISIN-based company lookup and vector-similarity matching."""

    def __init__(self, enrichment_service: EnrichmentService, vector_repo: VectorIndex):
        """
        Initialize the service with company and vector repositories.

        Args:
            company_repo: instance of a database to lookup company metadata.
            vector_repo: instance of a vector db to lookup embeddings.

        """
        self.enrichment_service = enrichment_service
        self.vector_repo = vector_repo

    # -------------------- Public Interface -------------------- 
    def find_by_isin(self, isin: List[str] | str) -> Company | List[Company]:
        """
        Return the company (or companies) associated with the given ISINS(s).

        Args:
            isin: The company ISIN to lookup metadata

        Returns:
            Company: datastructure that contains the metadata.
        """
        if isinstance(isin, list):
            return self.enrichment_service.get_enriched_companies_by_isin(isin)
        else:
            return self.enrichment_service.get_enriched_company_by_isin(isin)
    
    def find_by_lei(self, lei: str) -> Company:
        """
        Return the company associated with the given the LEI.

        Args:
            lei: The company lei to lookup metadata.

        Return:
            Company: data structure that contains the metadata
        """
        return self.enrichment_service.get_enriched_company_by_lei(lei)

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
        company = self.enrichment_service.get_enriched_company_by_isin(isin)
        leis, weights = self.vector_repo.retrieve_matches(company, k)
        companies = [self.enrichment_service.get_enriched_company_by_lei(lei) for lei in leis]
        return companies, weights
    
    def insert_embedding(self, isins: List[str] | str ) -> None:
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
    
    # -------------------- Internal Functions -------------------- 
    def _insert_embedding(self, isin: str) -> None:
        isin = self._validate_normalize_isin(isin)
        company = self.enrichment_service.get_enriched_company_by_isin(isin)
        self.vector_repo.upsert_embedding([company])
        print(f"Succesfully stored embedding of: {company.legal_name}")

    def _insert_embeddings(self, isins: List[str]) -> None:
        isins = [self._validate_normalize_isin(isin) for isin in isins]
        companies = self.enrichment_service.get_enriched_companies_by_isin(isins)
        for company in companies:
            self.vector_repo.upsert_embedding([company])
            print(f"Successfully stored embedding of : {company.legal_name}")
    
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
