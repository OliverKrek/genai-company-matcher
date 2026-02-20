# cli/wikidata_test.py
import os
from dotenv import load_dotenv
from core.enrichment_service import EnrichmentService
from infrastructure.sqlite_repository import SqliteCompanyRepository

load_dotenv()


def test_batch_query():
    db_path = os.getenv("DB_PATH")

    company_repo = SqliteCompanyRepository(db_path)
    enrichment_service = EnrichmentService(company_repo, batch_size=3)

    test_leis = [
        "549300S4KLFTLO7GSQ80",  # Apple
        "INR2EJN1ERAN0W5ZP974",  # Microsoft
        "ZXTILKJKG63JELOEG630",  # Amazon
    ]

    print(f"Testing Batch Query for {len(test_leis)} LEIs")
    print("-" * 50)

    results = enrichment_service._query_wikidata_batch(test_leis)

    for lei, data in results.items():
        print(f"\nLEI: {lei}")
        if data["wikidata_id"]:
            print(f"  Wikidata ID: {data['wikidata_id']}")
            print(f"  Description: {data['description']}")
            print(f"  Sectors: {[s['label'] for s in data['sectors']]}")
        else:
            print("  ⚠️ No data found")


if __name__ == "__main__":
    print("Starting batch Wikidata test.\n")
    test_batch_query()
