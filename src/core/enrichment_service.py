# core/enrichment_service.py
from typing import Dict, Any, Optional, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from core.models import Company
from core.interfaces import CompanyRepository

class EnrichmentService:
    def __init__(self, company_repo: CompanyRepository):
        self.company_repo = company_repo
        self.session = self._init_wikidata_session()

    def _init_wikidata_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'GenAIMatching/1.0 (test@email.com)',
            'Accept': 'application/sparql-results+json'
        })
        return session

    # ---------------- Implementation/Interface to sql repo -----------------
    def get_enriched_company_by_isin(self, isin: str) -> Optional[Company]:
        return self._ensure_enriched_company_by_isin(isin)
    
    def get_enriched_companies_by_isin(self, isins: List[str]) -> List[Company | None]:
        return self._ensure_enriched_company_by_isins(isins)
    
    def get_enriched_company_by_lei(self, lei: str) -> Optional[Company]:
        return self._ensure_enriched_company_by_lei(lei)

    def _ensure_enriched_company_by_isin(self, isin: str) -> Optional[Company]:
        company = self.company_repo.get_by_isin(isin)

        if not company:
            return None

        if company.has_sector_data:
            return company

        wikidata_data = self._query_wikidata(company.lei)

        if wikidata_data.get("wikidata_id"):
            description = wikidata_data["description"] or ""
            labels = [s["label"] for s in wikidata_data["sectors"]]

            company.enrich(labels, description)
            self.company_repo.enrich_company(company.lei, description, labels)

        return company
    
    def _ensure_enriched_company_by_isins(self, isins: List[str]) -> List[Company | None]:
        companies = self.company_repo.get_by_isins(isins)

        enriched_companies = []

        for company in companies:
            if not company:
                enriched_companies.append(None)
                continue

            if company.has_sector_data:
                enriched_companies.append(company)
                continue
            
            wikidata_data = self._query_wikidata(company.lei)

            if wikidata_data.get("wikidata_id"):
                description = wikidata_data["description"] or ""
                labels = [s["label"] for s in wikidata_data["sectors"]]

                company.enrich(labels, description)
                self.company_repo.enrich_company(company.lei, description, labels)
            enriched_companies.append(company)
        return enriched_companies
    
    def _ensure_enriched_company_by_lei(self, lei: str) -> Optional[Company]:
        company = self.company_repo.get_by_lei(lei)

        if not company:
            return None

        if company.has_sector_data:
            return company

        wikidata_data = self._query_wikidata(company.lei)

        if wikidata_data.get("wikidata_id"):
            description = wikidata_data["description"] or ""
            labels = [s["label"] for s in wikidata_data["sectors"]]

            company.enrich(labels, description)
            self.company_repo.enrich_company(company.lei, description, labels)

        return company

    def _query_wikidata(self, lei: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Queries wikidata for industry and description using LEI.

        Args:
            lei: Company identifier

        Returns:
            dict: {
                'wikidata_id': str or None,
                'description': str or None,
                'sector': [{'label': str, 'qid': str},...]
            }
        """
        url = "https://query.wikidata.org/sparql"

        query = f"""
            SELECT ?item ?itemDescription ?industry ?industryLabel WHERE {{
                ?item wdt:P1278 "{lei}".
                OPTIONAL {{ ?item wdt:P452 ?industry. }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
            }}
            """

        try:
            response = self.session.get(url, params={'query': query}, timeout=15)
            response.raise_for_status()
            data = response.json()
            bindings = data.get("results", {}).get("bindings", [])

            if not bindings:
                return {"wikidata_id": None, "description": None, "sectors": []}

            # Map the response
            return {
                "wikidata_id": bindings[0]["item"]["value"].split("/")[-1],
                "description": bindings[0].get("itemDescription", {}).get("value"),
                "sectors": [
                    {"label": b["industryLabel"]["value"]} 
                    for b in bindings if "industryLabel" in b
                ]
            }

        except (requests.RequestException, KeyError):
            return {"wikidata_id": None, "description": None, "sectors": []}