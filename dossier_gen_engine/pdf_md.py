"""
dossier_gen_engine/pdf_md.py
-----------------------------
PDF → Markdown pipeline.

Changes from the original finals/pdf_md.py:
  1. process_pdf() now accepts two new optional parameters:
       - output_md_path   : explicit output path (instead of always writing to OUTPUT_FOLDER)
       - manifest_metadata: dict with canonical header values from the registry
                            If provided, these override what was parsed from the PDF.
                            Keys: product, doc_code, reg_code, issue_date
  2. All other logic is UNCHANGED.

Direct usage (unchanged test interface still works):
    python pdf_md.py
"""

import fitz
import re
import os
from pathlib import Path
from openai import AzureOpenAI
from dossier_gen_engine.section_update import apply_section_updates

# ============================================
# CONFIG
# ============================================

INPUT_FOLDER  = r"data\dossiers"
OUTPUT_FOLDER = r"data\markdown_output"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
)

DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

TOKEN_LIMIT        = 55000
AVG_CHARS_PER_TOKEN = 4


# ============================================
# STEP 1: HEADER EXTRACTION
# ============================================

def extract_header(full_text):
    lines = full_text.split("\n")
    header_lines = []
    header_end_idx = 0

    header_pattern = re.compile(
        r"(C\.\d+\.\d+-\d+|VV-REGQUAL-\d+|Issue date|page \d+ of \d+|Additional Information)",
        re.IGNORECASE
    )

    for i, line in enumerate(lines[:20]):
        stripped = line.strip()
        if not stripped:
            continue
        if header_pattern.search(stripped) or (i < 8 and stripped):
            header_lines.append(stripped)
            header_end_idx = i
        elif i > header_end_idx + 3:
            break

    header_text   = "\n".join(header_lines)
    cleaned_lines = lines[header_end_idx + 1:]
    cleaned_text  = "\n".join(cleaned_lines)

    return header_text, cleaned_text


# ============================================
# STEP 2: TOC EXTRACTION
# ============================================

TOC_SECTION_PATTERN  = re.compile(r"^\d+(\.\d+)+$")
TOC_DOTLEADER_PATTERN = re.compile(r"\.{4,}")
TOC_PAGE_NUMBER_PATTERN = re.compile(r"\b\d{1,3}\s*$")


def extract_toc(full_text):
    lines     = full_text.split("\n")
    toc_start = None
    toc_end   = None

    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.search(r"table of contents", stripped, re.IGNORECASE):
            toc_start = i
        if toc_start is not None and TOC_DOTLEADER_PATTERN.search(stripped):
            toc_end = i

    if toc_start is None or toc_end is None:
        return None, full_text

    toc_block   = lines[toc_start: toc_end + 1]
    toc_entries = []

    for line in toc_block:
        stripped = line.strip()
        if not stripped:
            continue
        if TOC_SECTION_PATTERN.match(stripped):
            toc_entries.append(stripped)
        elif any(c.isalpha() for c in stripped) and TOC_DOTLEADER_PATTERN.search(stripped):
            toc_entries.append(stripped)

    cleaned_lines = lines[:toc_start] + lines[toc_end + 1:]
    cleaned_text  = "\n".join(cleaned_lines)

    return toc_entries if toc_entries else None, cleaned_text


# ============================================
# STEP 3: SECTION DETECTION
# ============================================

SECTION_PATTERN = re.compile(r"^\d+(\.\d+)+$")


def parse_section(section_str):
    return [int(x) for x in section_str.split(".")]


def is_valid_transition(prev, curr):
    if prev is None:
        return True
    if curr[:len(prev)] == prev and len(curr) == len(prev) + 1 and curr[-1] == 1:
        return True
    if len(curr) == len(prev) and curr[:-1] == prev[:-1] and curr[-1] == prev[-1] + 1:
        return True
    min_len = min(len(prev), len(curr))
    for i in range(min_len):
        if prev[i] != curr[i]:
            return curr[i] == prev[i] + 1
    return False


def is_valid_title(line):
    if not any(c.isalpha() for c in line):
        return False
    if "...." in line:
        return False
    return True


def extract_sections(text, prev_section):
    lines    = [l.strip() for l in text.split("\n") if l.strip()]
    sections = []
    i        = 0

    while i < len(lines):
        line = lines[i]
        if SECTION_PATTERN.match(line):
            curr_section = parse_section(line)
            if not is_valid_transition(prev_section, curr_section):
                i += 1
                continue
            if i + 1 < len(lines):
                title = lines[i + 1]
                if not is_valid_title(title):
                    i += 1
                    continue
                sections.append({"section": line, "title": title})
                prev_section = curr_section
                i += 2
                continue
        i += 1

    return sections, prev_section


# ============================================
# STEP 4: SECTION CONTENT EXTRACTION
# ============================================

_SIGNATURE_MARKERS = [
    "Signature Page for",
    "Review and Provide Annotations",
    "Approval Task",
]

def _is_signature_page(text: str) -> bool:
    """Skip signature/approval pages — they have no dossier content."""
    matches = sum(1 for m in _SIGNATURE_MARKERS if m in text)
    return matches >= 2


def extract_full_text(doc):
    pages = []
    for page_num, page in enumerate(doc):
        text = page.get_text("text")
        if _is_signature_page(text):
            print(f"  [+] Page {page_num + 1}: signature page — skipped")
            continue
        pages.append({"page": page_num + 1, "text": text})
    return pages


def _merge_table_continuation_lines(lines: list[str]) -> list[str]:
    """
    Fix 1: Merge continuation lines inside Markdown table cells.

    When fitz extracts a PDF table where one cell spans multiple lines
    (e.g. a long INCI name like POLYGLYCERYL-6 DISTEARATE, JOJOBA ESTERS,
    POLYGLYCERYL-3 BEESWAX, CETYL ALCOHOL), each line comes out separately.
    The LLM then treats each line as a new row.

    Strategy: if we are inside a table block (a run of lines starting with |)
    and the current line does NOT start with | but the previous line DID,
    treat the current line as a continuation of the previous cell and append it.
    """
    merged = []
    in_table = False

    for line in lines:
        stripped = line.rstrip()
        is_pipe_line = stripped.lstrip().startswith("|")

        if is_pipe_line:
            in_table = True
            merged.append(stripped)
        elif in_table and stripped and not is_pipe_line:
            # Continuation of the previous table cell — append to last row
            if merged:
                merged[-1] = merged[-1].rstrip("|").rstrip() + " " + stripped.strip()
            else:
                merged.append(stripped)
        else:
            if not is_pipe_line:
                in_table = False
            merged.append(stripped)

    return merged


def build_section_content(pages, sections):
    full_lines = []
    for p in pages:
        for line in p["text"].split("\n"):
            full_lines.append(line)

    section_starts = []
    line_idx       = 0
    sec_idx        = 0
    sections_copy  = list(sections)

    while line_idx < len(full_lines) and sec_idx < len(sections_copy):
        line = full_lines[line_idx].strip()
        sec  = sections_copy[sec_idx]

        if line == sec["section"]:
            for j in range(line_idx + 1, min(line_idx + 5, len(full_lines))):
                if full_lines[j].strip() == sec["title"]:
                    section_starts.append({
                        "section":    sec["section"],
                        "title":      sec["title"],
                        "start_line": line_idx
                    })
                    sec_idx += 1
                    break
        line_idx += 1

    result = []
    for i, sec_info in enumerate(section_starts):
        content_start = sec_info["start_line"] + 2
        content_end   = section_starts[i + 1]["start_line"] if i + 1 < len(section_starts) else len(full_lines)

        raw_content_lines = full_lines[content_start:content_end]

        header_noise = re.compile(
            r"(VV-REGQUAL-\d+|C\.\d+\.\d+-\d+|Issue date|page \d+ of \d+|Signature Page)",
            re.IGNORECASE
        )
        cleaned_lines = [
            l for l in raw_content_lines
            if not header_noise.search(l.strip()) or l.strip() == ""
        ]

        # Fix 1: merge table continuation lines before passing to LLM
        cleaned_lines = _merge_table_continuation_lines(cleaned_lines)

        content = "\n".join(cleaned_lines).strip()

        result.append({
            "section": sec_info["section"],
            "title":   sec_info["title"],
            "content": content
        })

    return result


# ============================================
# STEP 5: TOKEN-AWARE BATCHING
# ============================================

def estimate_tokens(text):
    return len(text) // AVG_CHARS_PER_TOKEN


def build_batches(sections_with_content):
    batches       = []
    current_batch = []
    current_tokens = 0

    for sec in sections_with_content:
        block  = f"SECTION: {sec['section']}\nTITLE: {sec['title']}\nCONTENT:\n{sec['content']}"
        tokens = estimate_tokens(block)

        if current_tokens + tokens > TOKEN_LIMIT and current_batch:
            batches.append(current_batch)
            current_batch  = [sec]
            current_tokens = tokens
        else:
            current_batch.append(sec)
            current_tokens += tokens

    if current_batch:
        batches.append(current_batch)

    return batches


# ============================================
# STEP 6: LLM CALL
# ============================================

SYSTEM_PROMPT = """You are a lossless document formatter.

Your ONLY job is to convert raw extracted PDF text into clean, structured Markdown.

STRICT RULES:
1. DO NOT drop any content — every word, number, and value must appear in output.
2. DO NOT summarize, paraphrase, or interpret.
3. DO NOT hallucinate or add any information not present in the input.
4. DO NOT change any numeric values, chemical names, ppm values, or regulatory references.
5. PRESERVE exact meaning at all times.
6. DO NOT add Summary sections, bullet reformattings, closing statements, or any structural
   elements that are not present verbatim in the raw input. If the input ends with a plain
   sentence or a number row, the output must end exactly the same way — no additions.
7. DO NOT reformat the ending of a section. The last line of the input is the last line of
   the output. Never append a "Summary:", "Note:", "Total:", or any synthesised conclusion.

FORMATTING RULES:
- Each section must start with: ## <section_number> <section_title>
- Paragraphs: continuous prose, merge broken lines into full sentences where obvious.
- Bullet lists: use "- item" format; merge broken bullet lines; infer list structure from context.
- Tables: reconstruct as Markdown tables when structure is clear.
  - If reconstruction is ambiguous, preserve as structured plain text — DO NOT drop data.
  - NEVER invent missing columns or values.
  - Table rows that continue across multiple lines in the raw text must be merged into a
    single row. A continuation line is one that does not start with | but follows a | line.
- Preserve all scientific values, ppm numbers, percentages exactly as-is.

This is a structural transformation task, not a creative task.
Behave like a deterministic formatter with structural intelligence."""


def format_batch_with_llm(batch):
    user_content = ""
    for sec in batch:
        user_content += f"\nSECTION: {sec['section']}\nTITLE: {sec['title']}\nCONTENT:\n{sec['content']}\n\n---\n"

    response = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_content.strip()}
        ],
        temperature=0,
        max_tokens=4096,
    )

    return response.choices[0].message.content.strip()


# ============================================
# STEP 7: STITCH FINAL DOCUMENT
# ============================================

def build_header_md(header_text):
    if not header_text:
        return ""
    lines = [l for l in header_text.split("\n") if l.strip()]
    md    = "---\n"
    for line in lines:
        md += f"{line}\n"
    md += "---\n\n"
    return md


def build_header_md_from_manifest(metadata: dict) -> str:
    """
    Build the front-matter block from canonical registry metadata
    instead of what was parsed from the PDF.

    Args:
        metadata: dict with keys: product, doc_code, reg_code, issue_date

    Returns:
        Front-matter string in the exact format parse_positional_header() expects:
        ---
        <product>
        <doc_code>
        <reg_code>
        <issue_date>
        ---
    """
    lines = [
        metadata.get("product",    ""),
        metadata.get("doc_code",   ""),
        metadata.get("reg_code",   ""),
        metadata.get("issue_date", ""),
    ]
    md = "---\n"
    for line in lines:
        if line:
            md += f"{line}\n"
    md += "---\n\n"
    return md


def build_toc_md(toc_entries):
    if not toc_entries:
        return ""
    md = "## Table of Contents\n\n"
    for entry in toc_entries:
        md += f"- {entry}\n"
    md += "\n"
    return md


def stitch_document(header_md, toc_md, body_chunks):
    parts = []
    if header_md:
        parts.append(header_md)
    if toc_md:
        parts.append(toc_md)
    parts.extend(body_chunks)
    return "\n\n".join(parts)


# ============================================
# MAIN CALLABLE: process_pdf
# ============================================

def process_pdf(
    pdf_path:           str,
    updated_sections:   list[dict] | None = None,
    output_md_path:     str | None = None,
    manifest_metadata:  dict | None = None,
) -> str:
    """
    Full pipeline: PDF → Markdown.

    Args:
        pdf_path          : path to the PDF file
        updated_sections  : optional list of dicts, each with keys: section, title, content
                            All updates are applied sequentially before LLM formatting.
                            Conflicts are flagged per section (see section_update.py).
        output_md_path    : explicit output path for the .md file.
                            If None, falls back to OUTPUT_FOLDER/<pdf_stem>.md
        manifest_metadata : optional dict with canonical header values from the registry.
                            Keys: product, doc_code, reg_code, issue_date.
                            If provided, these REPLACE what was parsed from the PDF header,
                            guaranteeing the front-matter matches the registry ground truth.

    Returns:
        output_path (str) — path to the saved .md file
    """
    filename = os.path.basename(pdf_path)
    print(f"\n{'='*60}")
    print(f"Processing: {filename}")
    print(f"{'='*60}")

    doc = fitz.open(pdf_path)

    # --- Extract all raw text ---
    pages     = extract_full_text(doc)
    full_text = "\n".join(p["text"] for p in pages)

    # --- Step 1: Extract and strip header ---
    header_text, text_no_header = extract_header(full_text)
    print(f"  [+] Header extracted")

    # --- Step 2: Extract and strip TOC ---
    toc_entries, text_no_toc = extract_toc(text_no_header)
    if toc_entries:
        print(f"  [+] TOC found ({len(toc_entries)} entries)")
    else:
        print(f"  [+] No TOC found")

    # --- Step 3: Detect sections ---
    prev_section = None
    all_sections = []

    for page_num, page in enumerate(doc):
        page_text            = page.get_text("text")
        sections, prev_section = extract_sections(page_text, prev_section)
        for sec in sections:
            sec["page"] = page_num + 1
        all_sections.extend(sections)

    print(f"  [+] Detected {len(all_sections)} sections")

    # --- Step 4: Extract content per section ---
    sections_with_content = build_section_content(pages, all_sections)
    print(f"  [+] Content extracted for {len(sections_with_content)} sections")

    # --- NEW STEP: Apply all section updates (if provided) ---
    if updated_sections:
        print(f"\n  [UPDATE] Applying {len(updated_sections)} section update(s)...")
        sections_with_content = apply_section_updates(
            sections_with_content,
            updated_sections,
            client,
            DEPLOYMENT,
        )
        print(f"  [+] All section updates applied")

    # --- Step 5: Build token-aware batches ---
    batches = build_batches(sections_with_content)
    print(f"  [+] Batched into {len(batches)} LLM call(s)")

    # --- Step 6: Format each batch with LLM ---
    body_chunks = []
    for i, batch in enumerate(batches):
        print(f"  [+] LLM formatting batch {i+1}/{len(batches)}...")
        formatted = format_batch_with_llm(batch)
        body_chunks.append(formatted)

    # --- Step 7: Build header MD ---
    # Prefer registry metadata (canonical ground truth) over PDF-parsed header.
    if manifest_metadata:
        header_md = build_header_md_from_manifest(manifest_metadata)
        print(f"  [+] Header built from manifest metadata (registry override)")
    else:
        header_md = build_header_md(header_text)
        print(f"  [+] Header built from PDF-parsed text")

    toc_md   = build_toc_md(toc_entries)
    final_md = stitch_document(header_md, toc_md, body_chunks)

    # --- Determine output path ---
    if output_md_path:
        out_path = output_md_path
    else:
        stem     = Path(pdf_path).stem
        out_path = os.path.join(OUTPUT_FOLDER, f"{stem}.md")

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(final_md)

    print(f"  ✅ Saved: {out_path}")
    return out_path