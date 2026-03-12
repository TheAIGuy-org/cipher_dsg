"""
parsers/pdf_extractor.py
------------------------
Responsibility: Extract raw text from dossier PDFs, page by page,
preserving structure signals (headings, tables, bullets).

Design decisions:
  - Uses pdfplumber for accurate text + table detection.
  - Returns a list of PageContent objects so the section parser can
    work with page-aware content (needed for cross-page sections).
  - Table data is extracted separately from prose so we can mark
    sections as has_table=True with full table content stored.
  - Does NOT do any section parsing — that is the section_parser's job.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pdfplumber
from utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class TableData:
    """A table extracted from a PDF page."""
    caption: str           # text immediately preceding the table (the table title)
    headers: list[str]     # first non-empty row treated as headers
    rows: list[list[str]]  # all subsequent rows
    raw_text: str          # flattened representation for embedding / search


@dataclass
class PageContent:
    """All content from one PDF page."""
    page_number: int
    raw_text: str           # full page text (used by section parser)
    tables: list[TableData] = field(default_factory=list)


def extract_pdf(pdf_path: Path) -> list[PageContent]:
    """
    Extract all pages from a dossier PDF.

    Returns a list of PageContent — one per page.
    Skips signature pages (they contain no dossier content).
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"Dossier PDF not found: {pdf_path}")

    pages: list[PageContent] = []

    with pdfplumber.open(pdf_path) as pdf:
        log.info(f"Extracting {len(pdf.pages)} pages from: {pdf_path.name}")

        for i, page in enumerate(pdf.pages, start=1):
            raw_text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""

            # Skip signature pages — they have no regulatory content
            if _is_signature_page(raw_text):
                log.debug(f"  Page {i}: signature page — skipped")
                continue

            # Skip table-of-contents pages — guarded by page number + line-length check
            if _is_toc_page(raw_text, page_number=i):
                log.debug(f"  Page {i}: table of contents page — skipped")
                continue

            # Extract tables from this page
            tables = _extract_tables(page, raw_text)

            pages.append(PageContent(
                page_number=i,
                raw_text=raw_text,
                tables=tables,
            ))
            log.debug(f"  Page {i}: {len(raw_text)} chars, {len(tables)} tables")

    log.info(f"Extracted {len(pages)} content pages from {pdf_path.name}")
    return pages


# ── Private helpers ──────────────────────────────────────────────────────────

def _is_signature_page(text: str) -> bool:
    """Signature pages have no dossier sections — skip them."""
    sig_markers = ["Signature Page for", "Review and Provide Annotations", "Approval Task"]
    matches = sum(1 for m in sig_markers if m in text)
    return matches >= 2


def _is_toc_page(text: str, page_number: int = 999) -> bool:
    """
    Detect a pure table-of-contents page that should be skipped.

    Three conditions must ALL be true — any one false → not a TOC:
      1. The text contains the literal heading "Table of contents"
      2. The page number is early (≤ 2) — dossier content never starts on page 1
      3. At least 3 lines match the TOC entry pattern:
           section_number  .......  page_number
         e.g. "2.2.2.1 Presence of allergens ................. 3"
         TOC entries ALWAYS end in a page number after the dot leaders.
         Body sentences NEVER end in a bare integer after dots — they end in
         words, punctuation, or alphanumeric values.

    Why three guards instead of the original one regex?
      The original r'\\d+\\.\\d+.*?\\.{4,}' matches body content too: batch
      result tables and cross-references like "2.2.3.........pass" satisfy it.
      Silently dropping a real content page causes missing sections in the graph
      with no error — the worst kind of failure.
    """
    # Guard 1: must have the explicit TOC header
    if "Table of contents" not in text:
        return False

    # Guard 2: TOC pages are always at the very start of a PDF
    if page_number > 2:
        return False

    # Guard 3: require actual TOC entry lines (section + dot leaders + page number)
    # This pattern only matches lines that end in a digit (page number) after dots —
    # which body text never does.
    toc_entry_pattern = re.compile(r'^\d+\.\d+.*\.{4,}\s*\d+\s*$')
    lines = text.splitlines()
    toc_entry_count = sum(1 for line in lines if toc_entry_pattern.match(line.strip()))

    return toc_entry_count >= 3


def _extract_tables(page, full_text: str) -> list[TableData]:
    """Extract structured tables from a pdfplumber page object."""
    tables: list[TableData] = []

    for table in page.extract_tables():
        if not table:
            continue

        # Clean all cells
        cleaned = [
            [_clean_cell(cell) for cell in row]
            for row in table
        ]

        # Find first fully non-empty row as header
        headers: list[str] = []
        data_rows: list[list[str]] = []
        found_header = False

        for row in cleaned:
            non_empty = [c for c in row if c]
            if not found_header and len(non_empty) >= 2:
                headers = row
                found_header = True
            elif found_header:
                data_rows.append(row)

        if not headers:
            continue

        # Build flat text representation for embedding
        flat_parts = [" | ".join(h for h in headers if h)]
        for row in data_rows:
            row_text = " | ".join(c for c in row if c)
            if row_text.strip():
                flat_parts.append(row_text)
        raw_text = "\n".join(flat_parts)

        # Try to find the caption (text before the table on this page)
        caption = _find_table_caption(full_text)

        tables.append(TableData(
            caption=caption,
            headers=headers,
            rows=data_rows,
            raw_text=raw_text,
        ))

    return tables


def _clean_cell(cell: Optional[str]) -> str:
    """Normalize whitespace in a table cell."""
    if cell is None:
        return ""
    return re.sub(r'\s+', ' ', str(cell)).strip()


def _find_table_caption(page_text: str) -> str:
    """
    Extract the most recent 'Table N:' or 'Figure N:' title from page text.
    These captions appear immediately above tables in Cipher dossiers.
    """
    matches = re.findall(r'(?:Table|Figure)\s+\d+[:\.]?\s+[^\n]+', page_text)
    return matches[-1].strip() if matches else ""
