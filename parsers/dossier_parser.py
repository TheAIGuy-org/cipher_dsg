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
from typing import List, Tuple, Dict, Optional
import os
from openai import AzureOpenAI
from dotenv import load_dotenv
from dataclasses import replace

from parsers.data_models import ParsedDossier, ParsedSection, TableData
from parsers.pdf_extractor import extract_pdf, PageContent
from parsers.section_profiler import SectionProfiler
from embeddings.embedder import get_embedder
from utils.logger import get_logger

log = get_logger(__name__)
load_dotenv()

_client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
)

_model = os.getenv("AZURE_OPENAI_MODEL")


# ── Patterns ─────────────────────────────────────────────────────────────────

# Section heading: "2.2.1 Reference formula"
SECTION_HEADING_RE = re.compile(
    r'^(?P<number>2\.2(?:\.\d+){1,3})\s+(?P<title>.+)$',
    re.MULTILINE,
)

# Bullet points
BULLET_RE = re.compile(r'^\s*[•\-\*]\s+', re.MULTILINE)


# ── Main Parser ──────────────────────────────────────────────────────────────

def parse_dossier(pdf_path, manifest, profiler: Optional[SectionProfiler] = None) -> ParsedDossier:
    """
    Parse a dossier PDF into structured sections with semantic metadata.
    
    Phase 1 Enhancement: Now includes semantic profiling using LLM.
    
    Args:
        pdf_path: Path to PDF file
        manifest: Dossier manifest with metadata
        profiler: Optional SectionProfiler for semantic metadata generation
    
    Returns:
        ParsedDossier with sections containing:
        - Section number and title
        - Full text (the template)
        - Format metadata
        - Embedding for semantic search
        - Semantic profile (if profiler provided)
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
    
    if profiler:
        log.info("  Semantic profiling enabled (Phase 1)")
    
    # Parse each section
    sections: List[ParsedSection] = []
    for span in section_spans:
        section = _parse_section(
            span=span,
            all_tables=all_tables,
            embedder=embedder,
            profiler=profiler
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
        texts.append(page.raw_text)
        all_tables.extend(page.tables)
    
    full_text = "\n\n".join(texts)
    return full_text, all_tables


def _find_section_spans(doc_text: str) -> List[Dict]:
    """
    Find all section boundaries in document.
    
    Returns list of dicts with: number, title, start_pos, end_pos, text
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
        
        # Extract the section text (excluding the heading line)
        section_text = doc_text[start_pos:end_pos]
        # Remove the heading line (first line)
        lines = section_text.split('\n')
        section_content = '\n'.join(lines[1:]) if len(lines) > 1 else ""
        
        spans.append({
            'number': match.group('number'),
            'title': match.group('title').strip(),
            'start_pos': start_pos,
            'end_pos': end_pos,
            'text': section_content.strip(),
        })
    
    return spans


def _parse_section(
    span: Dict,
    all_tables: List[TableData],
    embedder,
    profiler: Optional[SectionProfiler] = None
) -> ParsedSection:
    """
    Parse one section span into a ParsedSection.
    
    Phase 1: Now includes semantic profiling for situation-based matching.
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
    
    # Phase 1: Generate semantic profile if profiler provided
    semantic_description = ""
    semantic_embedding = []
    semantic_characteristics = {}
    domain_concepts = []
    
    if profiler and full_text.strip():
        try:
            # Generate semantic profile (situation description)
            semantic_profile = profiler.generate_semantic_profile(
                section_title=title,
                section_text=full_text
            )
            semantic_description = semantic_profile.situation_description
            semantic_embedding = semantic_profile.situation_embedding
            semantic_characteristics = semantic_profile.characteristics
            
            # Extract domain concepts
            domain_concepts = profiler.extract_domain_concepts(
                section_title=title,
                section_text=full_text
            )
            
            log.debug(f"    ✓ Profiled: {semantic_description[:60]}... | Concepts: {domain_concepts}")
        except Exception as e:
            log.warning(f"    Failed to profile section {section_number}: {e}")
            # Continue without semantic profile if profiling fails
    
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
        semantic_description=semantic_description,
        semantic_embedding=semantic_embedding,
        semantic_characteristics=semantic_characteristics,
        domain_concepts=domain_concepts,
    )


def _extract_section_text(span: Dict, all_tables: List[TableData]) -> str:
    """
    Extract the text for a section from the span dict.
    The span already contains the text with heading removed.
    """
    return span.get('text', '')


def _get_parent_number(section_number: str) -> str:
    """Get parent section number. Example: '2.2.2.1' -> '2.2.2'"""
    parts = section_number.split('.')
    if len(parts) <= 2:
        return ""  # Top-level section has no parent
    return '.'.join(parts[:-1])

def _llm_detect_table(text: str) -> bool:
    """
    Detect if section contains a table using FULL text (no truncation)
    """

    # 🔥 CHANGED: full section text passed (no slicing)
    prompt = f"""
You are a strict classifier.

Determine if the following section contains a table ANYWHERE
(even if the table appears on the next page).

Return ONLY:
true
or
false

Section:
{text}
"""

    try:
        response = _client.chat.completions.create(
            model=_model,
            messages=[
                {"role": "system", "content": "You detect tables in documents."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=5
        )

        answer = response.choices[0].message.content.strip().lower()
        return answer == "true"

    except Exception as e:
        log.warning(f"LLM table detection failed: {e}")
        return False


def _section_has_table(text: str, section_number: str, all_tables: List[TableData]) -> bool:
    """
    Hybrid detection (LLM first):
    1. LLM detects table from FULL section
    2. Fallback to pdfplumber-based section mapping
    """

    # 🔥 CHANGED: LLM is PRIMARY detector
    if _llm_detect_table(text):
        return True

    # 🔥 fallback to pdfplumber mapping
    section_tables = _find_tables_for_section(
        text=text,
        title="",
        section_number=section_number,
        all_tables=all_tables
    )

    return len(section_tables) > 0


def _find_tables_for_section(
    text: str,
    title: str,
    section_number: str,
    all_tables: List[TableData]
) -> List[TableData]:
    """
    Find tables that belong to this section.

    CLEAN VERSION:
    - No last-table hacks
    - Only logical matching
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

        # Match 2: Title keyword overlap
        title_words = set(re.findall(r'\w+', title_lower))
        caption_words = set(re.findall(r'\w+', caption_lower))

        if len(title_words & caption_words) >= 2:
            matched_tables.append(table)
            continue

        # Match 3: Explicit reference in section text
        if f"table {table.caption}" in text_lower:
            matched_tables.append(table)
            continue

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
