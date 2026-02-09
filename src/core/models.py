from dataclasses import dataclass, field
from typing import Optional, List
import sqlite3

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
    sector_labels: List[str] = field(default_factory=list)
    sector_qids: List[str] = field(default_factory=list)

    def validate(self) -> bool:
        """Validates whether the class contains sector level information."""
        return True if (self.sector_labels or self.sector_qids) else False

    def embedding_text(self) -> str:
        """Returns the prompt used to embed a company in a vector DB."""
        location = f"located in {self.city}, {self.country}"

        if self.sector_labels and self.sector_qids:
            return f"{self.legal_name} is a {self.sector_labels[0]}, {location}. It belongs in {self.sector_qids[0]}."
        if self.sector_labels:
            return f"{self.legal_name} is a {self.sector_labels[0]}, {location}. It belongs in {self.sector_qids[0]}." 
        if self.sector_qids:
            return f"Company {self.legal_name}, {location}. It belongs in {self.sector_qids[0]}."
        
        return f"""
            Risk characteristics for company {self.legal_name}. Located in {self.city}, {self.country}. Category: {self.category}.
        """

    def __str__(self) -> str:
        """Returns the string Representation of a company"""
        return f"Name: {self.legal_name}, LEI: {self.lei}, Country: {self.country}, Category: {self.category}"
    
    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Company":
        return cls(
            lei=row["lei"],
            registration_status=row["registration_status"],
            entity_status=row["entity_status"],
            legal_name=row["legal_name"],
            city=row["city"],
            country=row["country"],
            category=row["category"],
        )