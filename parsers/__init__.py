"""
parsers module
--------------
Simplified parsing for LLM-powered dynamic system.
"""
from parsers.pdf_extractor import extract_pdf, PageContent, TableData
from parsers.dossier_parser import parse_dossier_v2
from parsers.data_models import ParsedDossier, ParsedSection

__all__ = [
    "extract_pdf",
    "PageContent",
    "TableData",
    "parse_dossier_v2",
    "ParsedDossier",
    "ParsedSection",
]
