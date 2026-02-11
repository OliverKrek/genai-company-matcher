import pytest
from core.models import Company

def test_has_sector_data_is_false_initially():
    company = Company(lei="123", registration_status="active", entity_status="active",
                     legal_name="Test Co", city="Zurich", country="CH", category="Finance")
    assert company.has_sector_data() is False

def test_enrich_updates_state():
    company = Company(lei="123", registration_status="active", entity_status="active",
                     legal_name="Test Co", city="Zurich", country="CH", category="Finance")
    company.enrich(labels=["Banking"], description="Global Bank")

    assert company.has_sector_data is True
    assert "Banking" in company.sector_labels
    assert company.description == "Global Bank"

def test_embedding_fallback():
    company = Company(lei="123", registration_status="active", entity_status="active",
                     legal_name="Test Co", city="Zurich", country="CH", category="Finance")
    test_embedding = company.embedding_text()

    assert "Risk characteristics" in test_embedding
    assert "Finance" in test_embedding