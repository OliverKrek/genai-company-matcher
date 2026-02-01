# src/main.py
import os
import argparse
from dotenv import load_dotenv
from core.matching_service import MatchingService
from infrastructure.sqlite_repository import SqliteCompanyRepository
from infrastructure.vector_repository import ChromaVectorRepository

load_dotenv()

def create_service() -> MatchingService:
    company_repo = SqliteCompanyRepository(db_path=os.getenv("DB_PATH"))
    vector_repo = ChromaVectorRepository(db_path=os.getenv("VECTOR_DB_PATH"))
    return MatchingService(company_repo, vector_repo)


def main():
    # Instantiation of the parser
    parser = argparse.ArgumentParser(prog="genai-matcher", description="GenAI company matcher")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # The subparser provides and interface to the specific modules like search, initiation, etc.
    init_cmd = subparsers.add_parser("init")
    init_cmd.add_argument("--target", choices=["sqlite", "vectordb", "all"])
    init_cmd.add_argument("--recreate", action="store_true")

    search_cmd = subparsers.add_parser("search")
    search_cmd.add_argument("--isin", required=True)
    search_cmd.add_argument("--top-k", type=int, default=5)

    vectordb_cmd = subparsers.add_parser("vectordb")
    vectordb_cmd.add_argument("--isin", required=True)

    args = parser.parse_args()
    sql_db_path = os.getenv("DB_PATH")
    vector_db_path = os.getenv("VECTOR_DB_PATH")

    if args.command == "init":
        if args.target in ("sqlite", "all"):
            MatchingService.init_sqlite(sql_db_path, args.recreate)
            print(f"Successfully created SQL databse under: {sql_db_path}")
        if args.target in ("vectordb", "all"):
            MatchingService.init_vector_db(vector_db_path, args.recreate)
            print(f"Successfully created a ChromaDB under: {vector_db_path}")
        return
    
    # Create db objects for the service
    service = create_service()
    print(f"Created Matching Service!")

    if args.command == "search":
        companies, weights = service.find_matches(args.isin, args.top_k)
        if isinstance(companies, list):
            for company, weight in zip(companies, weights):
                print(f"Company: {company}, Weight: {weight}")

    if args.command == "vectordb":
        service.insert_embedding(args.isin)

if __name__ == "__main__":
    main()