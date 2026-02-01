# cli/test_utils.py
from infrastructure.utils import query_wikidata

def test_single_query():
    test_lei = "549300S4KLFTLO7GSQ80"

    print(f"Testing Single Query for {test_lei}")
    data = query_wikidata(test_lei)
    print(data)

    assert isinstance(data, dict)
    if data['wikidata_id']:
        print("✅ Found wikidata ID: ", data["wikidata_id"])
        print("✅ Description: ", data["description"])
        print("✅ Sectors: ", [s['label'] for s in data["sectors"]])
    else:
        print("⚠️ No data found (Check if LEI exists in Wikidata)")

if __name__ == "__main__":
    print("Starting test.")
    test_single_query()