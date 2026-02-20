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
    parser = argparse.ArgumentParser(
        prog="genai-matcher", description="GenAI company matcher"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sql_path = os.getenv("DB_PATH")
    vector_path = os.getenv("VECTOR_DB_PATH")
    batch_size = int(os.getenv("BATCH_SIZE", "30"))
    db_state = validate_db_files(sql_path, vector_path)

    if not db_state:
        sys.exit(1)
        print(
            f"Database not initialized. Please run the setup scripts in: 'scripts/init_*.py'"
        )

    search_cmd = subparsers.add_parser("search")
    search_cmd.add_argument("--isin", required=False)
    search_cmd.add_argument("--isins", required=False)
    search_cmd.add_argument("--top-k", type=int, default=5)
    search_cmd.add_argument("--batch-size", type=int, default=batch_size)

    vectordb_cmd = subparsers.add_parser("vectordb")
    vectordb_cmd.add_argument("--isin", required=False)
    vectordb_cmd.add_argument("--isins", required=False)
    vectordb_cmd.add_argument("--batch-size", type=int, default=batch_size)

    args = parser.parse_args()

    if args.isin and args.isins:
        print("Error: Cannot use both --isin and --isins at the same time.")
        sys.exit(1)

    if not args.isin and not args.isins:
        print("Error: Either --isin or --isins must be provided.")
        sys.exit(1)

    company_repo = SqliteCompanyRepository(sql_path)
    vector_repo = ChromaVectorRepository(vector_path)
    enrichment_service = EnrichmentService(company_repo, batch_size=args.batch_size)
    service = MatchingService(enrichment_service, vector_repo)
    print(f"Databases schemas validated successfully.")
    print(f"Matching service created successfully!")

    if args.command == "search":
        if args.isin:
            companies, weights = service.find_matches(args.isin, args.top_k)
            for company, weight in zip(companies, weights):
                print(f"Company: {company}, Weight: {weight}")
        else:
            isins = args.isins.split()
            for isin in isins:
                companies, weights = service.find_matches(isin, args.top_k)
                for company, weight in zip(companies, weights):
                    print(f"ISIN: {isin}, Company: {company}, Weight: {weight}")

    if args.command == "vectordb":
        if args.isin:
            service.insert_embedding(args.isin)
        else:
            isins = args.isins.split()
            service.insert_embedding(isins)


if __name__ == "__main__":
    main()
