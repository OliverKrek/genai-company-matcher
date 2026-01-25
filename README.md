# GenAI LEI & ISIN Matching System
This project provides a semantic matching pipeline to link company names and ISINs to their official Legal Entity Identifiers (LEI) using a combination of relational data and vector embeddings.

## Core Features
SQLite Repository: Stores and manages millions of GLEIF metadata records and ISIN-to-LEI mappings.

ChromaDB Vector Index: Enables semantic search to match "messy" or incomplete company names to clean LEI records.

Risk-Based Embedding: Uses a custom text template to prioritize industry, location, and entity status during matching.

CLI Tool: Command-line interface to search for specific entities and find their closest semantic neighbors.

## Quick Start
1. Environment Setup
Create a .env file based on .env.example and set your data paths:

LEI_PATH: Path to the full LEI metadata CSV.

LEI_ISIN_PATH: Path to the ISIN-LEI mapping CSV.

Both csv files can be downloaded using curl from the official API.

2. Initialize Databases
Run the initialization scripts to build your local SQLite and Vector stores:

Bash

python src/cli/main.py init --target all --recreate

3. Search an Entity
To find a company and its closest matches via ISIN:

Bash

python src/cli/main.py search --isin [YOUR_ISIN] --top-k 3

## Project Structure
core/: Domain models (Company) and repository interfaces.

infrastructure/: Implementations for SQLite and ChromaDB.

data/: (Ignored) Raw CSV source files.

databases/: (Ignored) Local persistence for SQLite and Vector indices.