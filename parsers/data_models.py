"""
parsers/data_models.py
-----------------------
Simplified data models for the dynamic LLM-powered system.

MAJOR SIMPLIFICATION from original:
- NO SituationProfile class (removed 70+ metadata fields)
- NO TextClause classification (removed clause_type, variable_entities)
- Sections store ONLY structure + full text (the template)
- All intelligence moved to LLM layer

This makes the system:
1. Scalable to ANY dossier format (no hardcoded section types)
2. Maintainable (600 fewer lines of regex logic)
3. Adaptable (works with future regulations automatically)
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class TableData:
    """
    A table extracted from the PDF.
    Kept simple - just the data.
    """
    caption: str
    headers: List[str]
    rows: List[List[str]]
    page_number: int


@dataclass
class ParsedSection:
    """
    One section from a dossier - SIMPLIFIED for dynamic system.
    
    REMOVED from original:
    - situation: SituationProfile (700 lines of regex logic)
    - clauses: list[TextClause] (clause classification logic)
    - source_db_tables: list[str] (complex mapping logic)
    
    KEPT (essentials only):
    - Hierarchical structure (section_number, parent_number)
    - Full text (THE TEMPLATE for generation)
    - Format metadata (for choosing generation strategy)
    - Embedding (for semantic search)
    """
    section_number: str      # "2.2.2.1"
    title: str               # "Presence of allergens"
    parent_number: str       # "2.2.2" for hierarchical queries
    full_text: str           # THE TEMPLATE - most important field!
    
    # Format metadata (helps LLM choose generation strategy)
    content_format: str      # "table" | "bullets" | "paragraphs" | "mixed"
    has_table: bool = False
    has_bullets: bool = False
    
    # Semantic search
    embedding: List[float] = field(default_factory=list)
    
    # Optional: preserve tables if needed for validation
    tables: List[TableData] = field(default_factory=list)


@dataclass
class ParsedDossier:
    """Complete parsed representation of one product dossier."""
    product_code: str
    product_name: str
    version_code: str
    regqual_code: str
    issue_date: str
    sections: List[ParsedSection] = field(default_factory=list)


@dataclass
class PageContent:
    """
    Content extracted from one PDF page.
    (Unchanged - works fine as is)
    """
    page_number: int
    text: str
    tables: List[TableData] = field(default_factory=list)
