from dataclasses import dataclass, field
from typing import List
import sqlite3
import json

@dataclass
class Company():
    """Class that holds the company metadata for future consumption"""

    lei: str
    registration_status: str
    entity_status: str
    legal_name: str
    city: str
    country: str
    category: str
    description: str = ""
    sector_labels: List[str] = field(default_factory=list)

    @property
    def has_sector_data(self) -> bool:
        """Check if enrichment with Wikidata has occurred."""
        return bool(self.sector_labels or self.description)
   
    def enrich(self, labels: List[str], description: str) -> None:
        """Enriches instance of company with sector information"""
        self.sector_labels = labels
        self.description = description

    def embedding_text(self) -> str:
        """Returns the prompt used to embed a company in a vector DB."""
        location = f"located in {self.city}, {self.country}"
        label_string = " ,".join(label for label in self.sector_labels) if self.sector_labels else ""

        if self.sector_labels and self.description:
            return f"{self.legal_name} is a {self.description}, {location}. It belongs in {label_string}."
        if self.description:
            return f"{self.legal_name} is a {self.description}, {location}." 
        if self.sector_labels:
            return f"Company {self.legal_name}, {location}. It belongs in {label_string}."
        
        # We need a fallback embedding text if no data can be pulled from wikidata
        return f"Risk characteristics for company {self.legal_name}. Located in {self.city}, {self.country}. Category: {self.category}."

    def __str__(self) -> str:
        """Returns the string Representation of a company"""
        return f"Name: {self.legal_name}, LEI: {self.lei}, Country: {self.country}, Category: {self.category}"
    
    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Company":
        raw_labels = row["sector_labels"]

        # Need to handle the json encoding of the sector labels
        try:
            labels = json.loads(raw_labels) if isinstance(raw_labels, str) else (raw_labels or [])
        except json.JSONDecodeError:
            labels = []

        return cls(
            lei=row["lei"],
            registration_status=row["registration_status"],
            entity_status=row["entity_status"],
            legal_name=row["legal_name"],
            city=row["city"],
            country=row["country"],
            category=row["category"],
            description=row["description"],
            sector_labels=labels,
        )