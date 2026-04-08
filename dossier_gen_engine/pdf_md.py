"""
dossier_gen_engine/pdf_md.py
-----------------------------
Markdown assembler — no LLM involved.

Flow:
  pre-processed .md  ->  parse sections  ->  replace/insert updated sections  ->  write .md

content_generator.py has already produced the final section text upstream.
This module's only job is to splice that text into the correct position in the
source markdown and write the result to disk.
"""

import re
import os
from pathlib import Path
from dossier_gen_engine.section_update import apply_section_updates

# ============================================
# CONFIG
# ============================================

OUTPUT_FOLDER = r"data\markdown_output"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# ============================================
# HEADER BUILDER
# ============================================

def build_header_md_from_manifest(metadata: dict) -> str:
    """
    Build the front-matter block from canonical registry metadata.

    Args:
        metadata: dict with keys: product, doc_code, reg_code, issue_date

    Returns:
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


# ============================================
# MARKDOWN SECTION PARSER / REBUILDER
# ============================================

_SECTION_MD_PATTERN = re.compile(r"^## (\d+(?:\.\d+)+)\s+(.+)$", re.MULTILINE)


def parse_md_sections(body: str) -> tuple[list[dict], str]:
    """
    Parse a clean markdown body into a sections list.

    Returns:
        sections : list of {"section": "2.2.1", "title": "...", "content": "..."}
        preamble : any text before the first numbered section header
    """
    matches = list(_SECTION_MD_PATTERN.finditer(body))
    if not matches:
        return [], body

    preamble = body[:matches[0].start()].strip()
    sections = []

    for i, m in enumerate(matches):
        section_num   = m.group(1)
        title         = m.group(2).strip()
        content_start = m.end()
        content_end   = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        content       = body[content_start:content_end].strip()
        sections.append({"section": section_num, "title": title, "content": content})

    return sections, preamble


def rebuild_md_body(sections: list[dict], preamble: str = "") -> str:
    """Reconstruct a markdown body string from a sections list."""
    parts = []
    if preamble:
        parts.append(preamble)
    for sec in sections:
        parts.append(f"## {sec['section']} {sec['title']}\n\n{sec['content']}")
    return "\n\n".join(parts)


# ============================================
# BULLET NORMALIZER
# ============================================

# Matches lines that start with a bullet-like character (not standard markdown `- `)
# Covers: •  ●  –  —  * (single asterisk, not **bold**)
_BULLET_PATTERN = re.compile(
    r"^(?P<indent>[ \t]*)(?P<bullet>[•●\-–—]|\*(?!\*))[ \t]+(?P<text>.+)$",
    re.MULTILINE,
)


def normalize_bullets(md_text: str) -> str:
    """
    Normalize bullet characters to standard markdown `- ` list syntax, ensure
    a blank line precedes every bullet group, and loosen bullet groups so each
    item renders with proper vertical spacing in the PDF.

    Converts: •  –  —  *  -  to  -
    Inserts:  blank line before first bullet item when preceding line is non-blank.
    Loosens:  blank line between consecutive bullet items (loose list → <p> wrapper
              inside each <li>, giving natural paragraph spacing).
    """
    # Step 1 — normalise bullet characters
    text = _BULLET_PATTERN.sub(r"\g<indent>- \g<text>", md_text)

    # Step 2 — insert blank line before bullet groups that lack one
    # Matches: a non-empty, non-bullet line immediately followed by a bullet line
    text = re.sub(r"(?m)^(?![ \t]*- )(.+)\n([ \t]*- )", r"\1\n\n\2", text)

    # Step 3 — loosen bullet groups: blank line between consecutive bullet items
    # This converts tight lists to loose lists so each <li> gets a <p> wrapper,
    # giving the same vertical spacing as paragraph text in the PDF.
    text = re.sub(r"(?m)^([ \t]*- .+)\n([ \t]*- )", r"\1\n\n\2", text)

    return text


# ============================================
# TABLE BOLD STRIPPER
# ============================================

def strip_table_bold(md_text: str) -> str:
    """
    Strip **bold** markers from markdown table cells.

    Table cells are already rendered uppercase via CSS (text-transform: uppercase).
    Bold markers inside cells produce unwanted <strong> tags that make individual
    cells visually heavier than others, breaking visual consistency.

    Only lines that start with `|` (table rows and separator rows) are processed.
    """
    lines = md_text.split("\n")
    result = []
    for line in lines:
        if line.startswith("|"):
            line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
        result.append(line)
    return "\n".join(result)


# ============================================
# TABLE ROW MERGER
# ============================================

_SEPARATOR_PATTERN = re.compile(r"^\|[\s:|-]+\|$")


def merge_continuation_rows(md_text: str) -> str:
    """
    Merge table continuation rows back into their parent row.

    The LLM sometimes splits a long cell value across two markdown table rows,
    leaving the second row's remaining cells empty.  For example:

        | POLYGLYCERYL-6 DISTEARATE, JOJOBA ESTERS,   | EMULIUM MELLIFERA MB | GATTEFOSSE |
        | POLYGLYCERYL-3 BEESWAX, CETYL ALCOHOL       |                      |            |

    This function detects such continuation rows (all cells except the first are
    blank) and appends the first cell's text to the previous row's first cell.
    """
    lines = md_text.split("\n")
    result = []

    for line in lines:
        stripped = line.strip()

        # Only look at table data rows (start with |, not a separator row)
        if stripped.startswith("|") and not _SEPARATOR_PATTERN.match(stripped):
            cells = [c.strip() for c in stripped.split("|")]
            # split("|") produces empty strings at edges: ['', 'A', 'B', '', ...]
            # Remove leading/trailing empties from the pipe boundaries
            if cells and cells[0] == "":
                cells = cells[1:]
            if cells and cells[-1] == "":
                cells = cells[:-1]

            # A continuation row: first cell has text, all others are blank
            if (
                len(cells) > 1
                and cells[0]
                and all(c == "" or c == "-" for c in cells[1:])
                and result
            ):
                # Find the previous table data row to merge into
                for i in range(len(result) - 1, -1, -1):
                    prev = result[i].strip()
                    if prev.startswith("|") and not _SEPARATOR_PATTERN.match(prev):
                        prev_cells = [c.strip() for c in prev.split("|")]
                        if prev_cells and prev_cells[0] == "":
                            prev_cells = prev_cells[1:]
                        if prev_cells and prev_cells[-1] == "":
                            prev_cells = prev_cells[:-1]

                        # Append continuation text to first cell
                        prev_cells[0] = prev_cells[0] + " " + cells[0]

                        result[i] = "| " + " | ".join(prev_cells) + " |"
                        break
                continue  # skip adding the continuation row

        result.append(line)

    return "\n".join(result)


# ============================================
# MAIN CALLABLE: process_md
# ============================================

def process_md(
    md_source_path:    str,
    updated_sections:  list[dict] | None = None,
    output_md_path:    str | None = None,
    manifest_metadata: dict | None = None,
) -> str:
    """
    Splice updated sections into the source markdown and write the result.

    No LLM is called. The content from content_generator is injected as-is.

    Args:
        md_source_path    : path to the pre-processed .md (data/dossiers/<name>.md)
        updated_sections  : list of {"section", "title", "content"} dicts
        output_md_path    : explicit output path for the result .md
        manifest_metadata : optional dict with canonical header overrides
                            Keys: product, doc_code, reg_code, issue_date

    Returns:
        output_path (str)
    """
    filename = os.path.basename(md_source_path)
    print(f"\n{'='*60}")
    print(f"Assembling MD: {filename}")
    print(f"{'='*60}")

    with open(md_source_path, encoding="utf-8") as f:
        md_text = f.read()

    # Split front-matter from body
    fm_pattern = re.compile(r"^---\n([\s\S]*?)\n---\n?", re.MULTILINE)
    m = fm_pattern.match(md_text.lstrip())
    if m:
        header_block = md_text[: m.end()]
        body         = md_text[m.end():]
    else:
        header_block = ""
        body         = md_text

    # Override header from registry if provided
    if manifest_metadata:
        header_block = build_header_md_from_manifest(manifest_metadata)
        print("  [+] Header set from manifest metadata")
    else:
        print("  [+] Using existing markdown header")

    # Parse sections
    sections, preamble = parse_md_sections(body)
    print(f"  [+] Parsed {len(sections)} sections")

    # Splice in updated sections (REPLACE / INSERT / CONFLICT)
    if updated_sections:
        print(f"  [UPDATE] Splicing {len(updated_sections)} section(s)...")
        sections = apply_section_updates(sections, updated_sections)
        print("  [+] Done")

    # Rebuild and write
    updated_body = rebuild_md_body(sections, preamble)
    final_md     = header_block.rstrip("\n") + "\n\n" + updated_body

    # Normalize bullet characters to standard markdown list syntax + loosen spacing
    final_md = normalize_bullets(final_md)

    # Merge table continuation rows (LLM sometimes splits long cells across rows)
    final_md = merge_continuation_rows(final_md)

    # Strip bold markers from table cells (CSS handles uppercase; bold is redundant)
    final_md = strip_table_bold(final_md)

    if output_md_path:
        out_path = output_md_path
    else:
        stem     = Path(md_source_path).stem
        out_path = os.path.join(OUTPUT_FOLDER, f"{stem}_updated.md")

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(final_md)

    print(f"  [+] Saved: {out_path}")
    return out_path
