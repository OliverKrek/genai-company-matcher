# core/enrichment_service.py
from typing import Dict, Any, Optional, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from core.models import Company
from core.interfaces import CompanyRepository


class EnrichmentService:
    def __init__(self, company_repo: CompanyRepository, batch_size: int = 30):
        self.company_repo = company_repo
        self.batch_size = batch_size
        self.session = self._init_wikidata_session()

    def _init_wikidata_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "GenAIMatching/1.0 (test@email.com)",
                "Accept": "application/sparql-results+json",
            }
        )
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

    def _ensure_enriched_company_by_isins(
        self, isins: List[str]
    ) -> List[Company | None]:
        companies = self.company_repo.get_by_isins(isins)

        enriched_companies = []
        companies_needing_enrichment = []
        companies_needing_enrichment_indices = []

        for idx, company in enumerate(companies):
            if not company:
                enriched_companies.append(None)
                continue

            if company.has_sector_data:
                enriched_companies.append(company)
            else:
                enriched_companies.append(company)
                companies_needing_enrichment.append(company)
                companies_needing_enrichment_indices.append(idx)

        if companies_needing_enrichment:
            leis_to_query = [c.lei for c in companies_needing_enrichment]
            wikidata_results = self._chunked_query_wikidata(
                leis_to_query, self.batch_size
            )

            for idx, company in zip(
                companies_needing_enrichment_indices, companies_needing_enrichment
            ):
                wikidata_data = wikidata_results.get(company.lei, {})

                if wikidata_data.get("wikidata_id"):
                    description = wikidata_data["description"] or ""
                    labels = [s["label"] for s in wikidata_data["sectors"]]

                    company.enrich(labels, description)
                    self.company_repo.enrich_company(company.lei, description, labels)

                enriched_companies[idx] = company

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
            response = self.session.get(url, params={"query": query}, timeout=15)
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
                    for b in bindings
                    if "industryLabel" in b
                ],
            }

        except (requests.RequestException, KeyError):
            return {"wikidata_id": None, "description": None, "sectors": []}

    def _chunked_query_wikidata(
        self, leis: List[str], chunk_size: int
    ) -> Dict[str, Dict[str, Any]]:
        results = {}

        for i in range(0, len(leis), chunk_size):
            chunk = leis[i : i + chunk_size]
            chunk_results = self._query_wikidata_batch(chunk)
            results.update(chunk_results)

        return results

    def _query_wikidata_batch(self, leis: List[str]) -> Dict[str, Dict[str, Any]]:
        if not leis:
            return {}

        url = "https://query.wikidata.org/sparql"

        values_clause = " ".join([f'("{lei}")' for lei in leis])

        query = f"""
            SELECT ?item ?itemDescription ?industry ?industryLabel ?lei WHERE {{
                VALUES (?lei) {{ {values_clause} }}
                ?item wdt:P1278 ?lei.
                OPTIONAL {{ ?item wdt:P452 ?industry. }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
            }}
            """

        try:
            response = self.session.get(url, params={"query": query}, timeout=60)
            response.raise_for_status()
            data = response.json()
            bindings = data.get("results", {}).get("bindings", [])

            results = {}
            for binding in bindings:
                lei = binding.get("lei", {}).get("value")
                if not lei:
                    continue

                if lei not in results:
                    results[lei] = {
                        "wikidata_id": binding["item"]["value"].split("/")[-1]
                        if "item" in binding
                        else None,
                        "description": binding.get("itemDescription", {}).get("value"),
                        "sectors": [],
                    }

                if "industryLabel" in binding:
                    results[lei]["sectors"].append(
                        {"label": binding["industryLabel"]["value"]}
                    )

            for lei in leis:
                if lei not in results:
                    results[lei] = {
                        "wikidata_id": None,
                        "description": None,
                        "sectors": [],
                    }

            return results

        except (requests.RequestException, KeyError):
            return {
                lei: {"wikidata_id": None, "description": None, "sectors": []}
                for lei in leis
            }
