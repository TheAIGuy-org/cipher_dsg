"""
parsers/simple_parser.py
-------------------------
SIMPLIFIED dossier parser for LLM-powered dynamic system.

REMOVED (600+ lines):
- All situation profiling logic (_profile_cmr_section, _profile_allergen_section, etc.)
- All clause classification logic (_classify_clause, _extract_variable_entities, etc.)
- All regex pattern matching for regulatory content

KEPT (200 lines):
- Section boundary detection
- Content format detection (table/bullets/paragraphs)
- Full text extraction
- Embedding generation

This parser's job is SIMPLE:
1. Find section boundaries
2. Extract full text
3. Detect basic format
4. Generate embedding
5. Done!

All intelligence happens in the LLM layer.
"""
import re
from typing import List, Tuple, Dict
from dataclasses import replace

from parsers.data_models import ParsedDossier, ParsedSection, PageContent, TableData
from parsers.pdf_extractor import extract_pdf
from embeddings.embedder import get_embedder
from utils.logger import get_logger

log = get_logger(__name__)


# ── Patterns ─────────────────────────────────────────────────────────────────

# Section heading: "2.2.1 Reference formula"
SECTION_HEADING_RE = re.compile(
    r'^(?P<number>2\.2(?:\.\d+){1,3})\s+(?P<title>.+)$',
    re.MULTILINE,
)

# Bullet points
BULLET_RE = re.compile(r'^\s*[•\-\*]\s+', re.MULTILINE)


# ── Main Parser ──────────────────────────────────────────────────────────────

def parse_dossier(pdf_path, manifest) -> ParsedDossier:
    """
    Parse a dossier PDF into structured sections.
    
    This is the SIMPLIFIED version - no complex classification logic.
    Just extract structure and text, let LLM handle the rest.
    
    Args:
        pdf_path: Path to PDF file
        manifest: Dossier manifest with metadata
    
    Returns:
        ParsedDossier with sections containing:
        - Section number and title
        - Full text (the template)
        - Format metadata
        - Embedding for semantic search
    """
    log.info(f"Parsing dossier: {manifest.product_name} ({manifest.product_code})")
    
    # Extract PDF content
    pages = extract_pdf(pdf_path)
    
    # Build full document
    full_text, all_tables = _build_document(pages)
    
    # Find section boundaries
    section_spans = _find_section_spans(full_text)
    log.info(f"  Found {len(section_spans)} sections: "
             f"{[s['number'] for s in section_spans]}")
    
    # Get embedder for semantic search
    embedder = get_embedder()
    log.debug(f"  Using embedder: {embedder.__class__.__name__} (dim={embedder.dimension})")
    
    # Parse each section
    sections: List[ParsedSection] = []
    for span in section_spans:
        section = _parse_section(
            span=span,
            all_tables=all_tables,
            embedder=embedder
        )
        sections.append(section)
        log.debug(f"  ✓ Section {section.section_number}: '{section.title}' "
                  f"[{section.content_format}]")
    
    dossier = ParsedDossier(
        product_code=manifest.product_code,
        product_name=manifest.product_name,
        version_code=manifest.version_code,
        regqual_code=manifest.regqual_code,
        issue_date=manifest.issue_date,
        sections=sections,
    )
    
    log.info(f"  ✅ Parsed {len(sections)} sections successfully")
    return dossier


def _build_document(pages: List[PageContent]) -> Tuple[str, List[TableData]]:
    """Combine all pages into one text block and collect all tables."""
    texts = []
    all_tables: List[TableData] = []
    
    for page in pages:
        texts.append(page.text)
        all_tables.extend(page.tables)
    
    full_text = "\n\n".join(texts)
    return full_text, all_tables


def _find_section_spans(doc_text: str) -> List[Dict]:
    """
    Find all section boundaries in document.
    
    Returns list of dicts with: number, title, start_pos, end_pos
    """
    matches = list(SECTION_HEADING_RE.finditer(doc_text))
    
    if not matches:
        log.warning("No section headings found in document")
        return []
    
    spans = []
    for i, match in enumerate(matches):
        start_pos = match.start()
        # End is either the next section start, or end of document
        end_pos = matches[i + 1].start() if i + 1 < len(matches) else len(doc_text)
        
        spans.append({
            'number': match.group('number'),
            'title': match.group('title').strip(),
            'start_pos': start_pos,
            'end_pos': end_pos,
        })
    
    return spans


def _parse_section(
    span: Dict,
    all_tables: List[TableData],
    embedder
) -> ParsedSection:
    """
    Parse one section span into a ParsedSection.
    
    SIMPLIFIED: No situation profiling, no clause classification.
    Just extract the essentials.
    """
    section_number = span['number']
    title = span['title']
    
    # Extract text for this section
    full_text = _extract_section_text(span, all_tables)
    
    # Compute parent section number
    parent_number = _get_parent_number(section_number)
    
    # Detect format (simple heuristics)
    has_table = _section_has_table(full_text, section_number, all_tables)
    has_bullets = bool(BULLET_RE.search(full_text))
    content_format = _determine_format(has_table, has_bullets, full_text)
    
    # Find tables that belong to this section
    section_tables = _find_tables_for_section(full_text, title, section_number, all_tables)
    
    # Generate embedding for semantic search
    # Use title + first 2000 chars of text for embedding
    embedding_text = f"{title}\n\n{full_text[:2000]}"
    embedding = embedder.embed(embedding_text)
    
    return ParsedSection(
        section_number=section_number,
        title=title,
        parent_number=parent_number,
        full_text=full_text,
        content_format=content_format,
        has_table=has_table,
        has_bullets=has_bullets,
        embedding=embedding,
        tables=section_tables,
    )


def _extract_section_text(span: Dict, all_tables: List[TableData]) -> str:
    """
    Extract the text for a section, removing the heading line.
    """
    # Get text from document span (this will be passed in from _find_section_spans somehow)
    # For now, we'll need to pass the full doc text through
    # Actually, let me refactor this differently
    
    # The span dict should contain the actual text
    # Let me fix this in the calling code
    pass


def _get_parent_number(section_number: str) -> str:
    """Get parent section number. Example: '2.2.2.1' -> '2.2.2'"""
    parts = section_number.split('.')
    if len(parts) <= 2:
        return ""  # Top-level section has no parent
    return '.'.join(parts[:-1])


def _section_has_table(text: str, section_number: str, all_tables: List[TableData]) -> bool:
    """Check if section contains a table."""
    # Look for "Table" keyword in text
    if re.search(r'\btable\b', text, re.IGNORECASE):
        return True
    
    # Or check if any tables match this section
    for table in all_tables:
        if section_number in table.caption:
            return True
    
    return False


def _find_tables_for_section(
    text: str,
    title: str,
    section_number: str,
    all_tables: List[TableData]
) -> List[TableData]:
    """
    Find tables that belong to this section.
    
    SIMPLIFIED: Just use caption matching and keywords.
    No complex bleeding prevention - that was the bug we're fixing!
    """
    matched_tables = []
    
    text_lower = text.lower()
    title_lower = title.lower()
    
    for table in all_tables:
        caption_lower = table.caption.lower()
        
        # Match 1: Section number in caption
        if section_number in caption_lower:
            matched_tables.append(table)
            continue
        
        # Match 2: Title keywords in caption (at least 2 words overlap)
        title_words = set(re.findall(r'\w+', title_lower))
        caption_words = set(re.findall(r'\w+', caption_lower))
        overlap = title_words & caption_words
        
        if len(overlap) >= 2:
            matched_tables.append(table)
            continue
        
        # Match 3: Table referenced in section text
        if f"table {table.caption}" in text_lower:
            matched_tables.append(table)
    
    return matched_tables


def _determine_format(has_table: bool, has_bullets: bool, text: str) -> str:
    """
    Determine section content format.
    
    Simple heuristic classification for LLM to know what style to use.
    """
    if has_table and has_bullets:
        return "mixed"
    elif has_table:
        return "table"
    elif has_bullets:
        return "bullets"
    else:
        return "paragraphs"


# ── Document Processing (fix _extract_section_text) ─────────────────────────

# Let me rewrite the flow more clearly
def parse_dossier_v2(pdf_path, manifest) -> ParsedDossier:
    """
    CORRECTED VERSION - cleaner flow.
    """
    log.info(f"Parsing dossier: {manifest.product_name} ({manifest.product_code})")
    
    # Extract PDF
    pages = extract_pdf(pdf_path)
    
    # Build document
    full_doc_text = "\n\n".join(p.raw_text for p in pages)
    all_tables = [t for p in pages for t in p.tables]
    
    # Find sections
    section_spans = _find_section_spans(full_doc_text)
    log.info(f"  Found {len(section_spans)} sections")
    
    # Get embedder
    embedder = get_embedder()
    
    # Parse sections
    sections = []
    for span in section_spans:
        # Extract text for this section from full document
        section_text = full_doc_text[span['start_pos']:span['end_pos']]
        
        # Remove heading line
        section_text = re.sub(
            rf"^{re.escape(span['number'])}\s+{re.escape(span['title'])}\s*\n",
            "",
            section_text,
            count=1
        ).strip()
        
        # Parse
        section = _parse_section_v2(
            section_number=span['number'],
            title=span['title'],
            full_text=section_text,
            all_tables=all_tables,
            embedder=embedder
        )
        sections.append(section)
        log.debug(f"  ✓ {section.section_number}: '{section.title}' [{section.content_format}]")
    
    return ParsedDossier(
        product_code=manifest.product_code,
        product_name=manifest.product_name,
        version_code=manifest.version_code,
        regqual_code=manifest.regqual_code,
        issue_date=manifest.issue_date,
        sections=sections,
    )


def _parse_section_v2(
    section_number: str,
    title: str,
    full_text: str,
    all_tables: List[TableData],
    embedder
) -> ParsedSection:
    """Parse section - clean version."""
    
    # Parent number
    parent_number = _get_parent_number(section_number)
    
    # Format detection
    has_table = _section_has_table(full_text, section_number, all_tables)
    has_bullets = bool(BULLET_RE.search(full_text))
    content_format = _determine_format(has_table, has_bullets, full_text)
    
    # Tables
    section_tables = _find_tables_for_section(full_text, title, section_number, all_tables)
    
    # Embedding
    embedding_text = f"{title}\n\n{full_text[:2000]}"
    embedding = embedder.embed(embedding_text)
    
    return ParsedSection(
        section_number=section_number,
        title=title,
        parent_number=parent_number,
        full_text=full_text,
        content_format=content_format,
        has_table=has_table,
        has_bullets=has_bullets,
        embedding=embedding,
        tables=section_tables,
    )
