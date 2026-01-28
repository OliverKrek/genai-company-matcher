from dataclasses import dataclass

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

    def embedding_text(self) -> str:
        """Returns the prompt used to embed a company in a vector DB."""
        return f"""Risk characteristics for company {self.legal_name}. Located in {self.city}, {self.country}. Category: {self.category}."""

    def __str__(self):
        """Returns the string Representation of a company"""
        return f"Name: {self.legal_name}, LEI: {self.lei}, Country: {self.country}, Category: {self.category}"