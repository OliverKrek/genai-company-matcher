# src/main.py
import os
import sys
import argparse
from dotenv import load_dotenv
from core.matching_service import MatchingService
from infrastructure.sqlite_repository import SqliteCompanyRepository
from infrastructure.vector_repository import ChromaVectorRepository
from core.enrichment_service import EnrichmentService
from infrastructure.utils import validate_db_files

load_dotenv()

def main():
    # Instantiation of the parser
    parser = argparse.ArgumentParser(prog="genai-matcher", description="GenAI company matcher")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # TODO: add function from util that checks if tables and other names exist
    sql_path = os.getenv("DB_PATH")
    vector_path = os.getenv("VECTOR_DB_PATH")
    db_state = validate_db_files(sql_path, vector_path)

    if not db_state:
        sys.exit(1)
        print(f"Database not initialized. Please run the setup scripts in: 'scripts/init_*.py'")

    search_cmd = subparsers.add_parser("search")
    search_cmd.add_argument("--isin", required=True)
    search_cmd.add_argument("--top-k", type=int, default=5)

    vectordb_cmd = subparsers.add_parser("vectordb")
    vectordb_cmd.add_argument("--isin", required=True)

    args = parser.parse_args()

    # Create db objects for the service
    company_repo = SqliteCompanyRepository(sql_path)
    vector_repo = ChromaVectorRepository(vector_path)
    enrichment_service = EnrichmentService(company_repo)
    service = MatchingService(enrichment_service, vector_repo)
    print(f"Databases schemas validated successfully.")
    print(f"Matching service created successfully!")

    if args.command == "search":
        companies, weights = service.find_matches(args.isin, args.top_k)
        if isinstance(companies, list):
            for company, weight in zip(companies, weights):
                print(f"Company: {company}, Weight: {weight}")

    if args.command == "vectordb":
        service.insert_embedding(args.isin)

if __name__ == "__main__":
    main()