from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from config.settings import settings
from llm.azure_client import get_llm_client
from updater.pdf_struct import pdf_to_struct
from updater.model import PromptOutput
from updater.prompt import PdfUpdatePrompts
from updater.struct_docx import struct_to_docx
from updater.table_parser import table_to_excel_llm, split_table_content
from updater.table_router import tableRouter

_LLM_FIELDS = {"Path", "Page", "Text", "attributes", "filePaths"}

# Characters LLMs commonly mis-escape in JSON — replace before sending
_ESCAPE_MAP = {
    "\u00b0": "°",  "\u2013": "–",  "\u2014": "—",  "\u2022": "•",
    "\u00e9": "é",  "\u00e8": "è",  "\u00ea": "ê",  "\u00e0": "à",
    "\u2019": "'",  "\u201c": '"',  "\u201d": '"',
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_pdf_path(product_code: str) -> Path:
    matches = list(settings.DOSSIER_DIR.glob(f"*{product_code}*.pdf"))
    if not matches:
        raise FileNotFoundError(f"No PDF found for product_code='{product_code}'")
    if len(matches) > 1:
        raise FileNotFoundError(f"Ambiguous PDFs for product_code='{product_code}'")
    return matches[0]


def _extract_section_elements(elements: list, section_number: str) -> list:
    """Return elements from the matching heading up to the next heading."""
    heading_idx = None
    for i, el in enumerate(elements):
        path = el.get("Path", "")
        text = (el.get("Text") or "").strip()
        if re.search(r"/H\d+(\[\d+\])?$", path) and text.startswith(f"{section_number} "):
            heading_idx = i
            break
    if heading_idx is None:
        return []
    section_els = [elements[heading_idx]]
    for el in elements[heading_idx + 1:]:
        if re.search(r"/H[12](\[\d+\])?$", el.get("Path", "")):
            break
        section_els.append(el)
    return section_els


def _slim(elements: list) -> list:
    """Strip rendering-only fields — LLM only needs Path, Page, Text, filePaths."""
    return [
        {k: v for k, v in el.items() if k in _LLM_FIELDS and v is not None}
        for el in elements
    ]


def _sanitize(text: str) -> str:
    """Strip markdown syntax and XML-incompatible control chars from LLM output."""
    if not text:
        return text
    text = str(text)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*(.+?)\*",     r"\1", text)
    text = re.sub(r"__(.+?)__",     r"\1", text)
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)


def _escape_safe(text: str) -> str:
    """Replace characters LLMs commonly mis-escape in JSON output."""
    for char, replacement in _ESCAPE_MAP.items():
        text = text.replace(char, replacement)
    return text


def _is_real_prose(text: str, min_line_len: int = 60) -> bool:
    """Return True if text contains at least one sentence-length line."""
    if not text.strip():
        return False
    return any(len(l.strip()) > min_line_len for l in text.split("\n") if l.strip())


# ── Path format detection ──────────────────────────────────────────────────────

def _detect_path_format(all_elements: list) -> str:
    for el in all_elements:
        path = el.get("Path", "")
        if re.match(r"//Document/H[123]", path): return "flat"
        if re.match(r"//Document/Sect", path):   return "nested"
    return "flat"


def _last_section_end(all_elements: list) -> int:
    last_idx, last_page = -1, -1
    for i, el in enumerate(all_elements):
        path = el.get("Path", "")
        text = (el.get("Text") or "").strip()
        if re.search(r"/H[12](\[\d+\])?$", path) and re.match(r"\d+\.\d+", text):
            last_idx, last_page = i, el.get("Page", 0)
    if last_idx == -1:
        return len(all_elements)
    insert_idx = last_idx + 1
    for i in range(last_idx + 1, len(all_elements)):
        el   = all_elements[i]
        text = (el.get("Text") or "").strip()
        page = el.get("Page", 0)
        if "Signature" in text or "Signature" in el.get("Path", ""):
            break
        if page > last_page + 1:
            break
        insert_idx = i + 1
    return insert_idx


def _section_depth(section_number: str) -> int:
    return max(1, min(len(section_number.strip().split(".")) - 2, 3))


def _find_parent_section_path(all_elements: list, section_number: str) -> str | None:
    parent_number = ".".join(section_number.split(".")[:-1])
    for el in all_elements:
        path = el.get("Path", "")
        text = (el.get("Text") or "").strip()
        if re.search(r"/H[123](\[\d+\])?$", path) and text.startswith(f"{parent_number} "):
            return re.sub(r"/H[123](\[\d+\])?$", "", path)
    return None


# ── ADD operation ──────────────────────────────────────────────────────────────

def _build_add_elements(
    all_elements: list, section_number: str, section_title: str, generated_text: str
) -> tuple[list, int]:
    fmt   = _detect_path_format(all_elements)
    depth = _section_depth(section_number)
    h_tag = f"H{depth}"

    if depth == 1:
        insert_idx = _last_section_end(all_elements)
    else:
        parent_number = ".".join(section_number.split(".")[:-1])
        parent_idx = next(
            (i for i, el in enumerate(all_elements)
             if re.search(r"/H[123](\[\d+\])?$", el.get("Path", ""))
             and (el.get("Text") or "").strip().startswith(f"{parent_number} ")),
            None,
        )
        if parent_idx is not None:
            insert_idx = parent_idx + 1
            for i in range(parent_idx + 1, len(all_elements)):
                nxt_path = all_elements[i].get("Path", "")
                if re.search(r"/H1(\[\d+\])?$", nxt_path):
                    break
                if re.search(r"/H[23](\[\d+\])?$", nxt_path):
                    nxt_text   = (all_elements[i].get("Text") or "").strip()
                    nxt_parent = ".".join(nxt_text.split(" ")[0].split(".")[:-1])
                    if nxt_parent != parent_number:
                        break
                insert_idx = i + 1
        else:
            insert_idx = _last_section_end(all_elements)

    page = all_elements[insert_idx - 1].get("Page", 0) if insert_idx > 0 else 0

    if fmt == "flat":
        existing     = [el.get("Path", "") for el in all_elements
                        if re.match(rf"//Document/{h_tag}(\[\d+\])?$", el.get("Path", ""))]
        next_idx     = len(existing) + 1
        heading_path = f"//Document/{h_tag}" if next_idx == 1 else f"//Document/{h_tag}[{next_idx}]"
        p_base       = "//Document/P"
    else:
        if depth == 1:
            sect_indices = []
            for el in all_elements:
                m = re.match(r"//Document/Sect(\[(\d+)\])?/", el.get("Path", ""))
                if m: sect_indices.append(int(m.group(2)) if m.group(2) else 1)
            next_n       = (max(sect_indices) + 1) if sect_indices else 2
            heading_path = f"//Document/Sect[{next_n}]/H1"
            p_base       = f"//Document/Sect[{next_n}]/P"
        else:
            container = _find_parent_section_path(all_elements, section_number)
            if container is None:
                sect_indices = []
                for el in all_elements:
                    m = re.match(r"//Document/Sect(\[(\d+)\])?/", el.get("Path", ""))
                    if m: sect_indices.append(int(m.group(2)) if m.group(2) else 1)
                container = f"//Document/Sect[{max(sect_indices) if sect_indices else 1}]"
            existing_siblings = [
                el.get("Path", "") for el in all_elements
                if el.get("Path", "").startswith(container)
                and re.search(rf"/{h_tag}(\[\d+\])?$", el.get("Path", ""))
            ]
            next_m        = len(existing_siblings) + 1
            sub_container = f"{container}/Sect" if next_m == 1 else f"{container}/Sect[{next_m}]"
            heading_path  = f"{sub_container}/{h_tag}"
            p_base        = f"{sub_container}/P"

    elements: list[dict[str, Any]] = [
        {"Path": heading_path, "Page": page, "Text": f"{section_number} {section_title} "}
    ]
    for i, chunk in enumerate(c.strip() for c in generated_text.split("\n\n") if c.strip()):
        p_path = p_base if i == 0 else f"{p_base}[{i + 1}]"
        elements.append({"Path": p_path, "Page": page, "Text": chunk})

    return elements, insert_idx


# ── Rendering field helpers ────────────────────────────────────────────────────

def _restore_rendering_fields(llm_elements: list, original_elements: list) -> list:
    original_by_path = {el.get("Path"): el for el in original_elements}
    restored = []
    for llm_el in llm_elements:
        path = llm_el.get("Path")
        if path in original_by_path:
            full_el = dict(original_by_path[path])
            if "Text" in llm_el:
                full_el["Text"] = _sanitize(llm_el["Text"])
            restored.append(full_el)
        else:
            llm_el = dict(llm_el)
            if "Text" in llm_el:
                llm_el["Text"] = _sanitize(llm_el["Text"])
            restored.append(llm_el)
    return restored


def _apply_donor_formatting(llm_elements: list, all_elements: list) -> list:
    _RENDERING = {"Bounds", "ClipBounds", "Font", "HasClip", "Lang",
                  "ObjectID", "TextSize", "attributes"}

    def _donor(path: str) -> dict | None:
        for pattern in (r"/H1(\[\d+\])?$", r"/H2(\[\d+\])?$"):
            if re.search(pattern, path):
                return next((e for e in all_elements if re.search(pattern, e.get("Path", ""))), None)
        if re.search(r"/P(\[\d+\])?$", path) and "/Table/" not in path:
            return next((e for e in all_elements
                         if re.search(r"/P(\[\d+\])?$", e.get("Path", ""))
                         and "/Table/" not in e.get("Path", "")), None)
        return None

    result = []
    for el in llm_elements:
        donor = _donor(el.get("Path", ""))
        if donor:
            merged = {k: v for k, v in donor.items() if k in _RENDERING}
            merged.update(el)
            result.append(merged)
        else:
            result.append(el)
    return result


# ── LLM batch call ─────────────────────────────────────────────────────────────

def _batch_llm_call(all_elements: list, llm_changes: list) -> dict[str, list]:
    sections_json: dict = {}
    instructions:  list = []

    for change in llm_changes:
        sn = change["section_number"]
        sections_json[sn] = _slim(_extract_section_elements(all_elements, sn))
        instructions.append({
            "section_number": sn,
            "section_title":  change["section_title"],
            "operation":      change["operation"],
            "generated_text": _escape_safe(change["generated_text"]),
        })

    user_prompt = PdfUpdatePrompts.USER_TEMPLATE.format(
        sections_json     = json.dumps(sections_json, indent=2),
        instructions_json = json.dumps(instructions,  indent=2),
    )

    response = get_llm_client().ask(
        prompt          = user_prompt,
        system_prompt   = PdfUpdatePrompts.SYSTEM_PROMPT,
        temperature     = 0,
        max_tokens      = 16000,
        response_format = "json_object",
    )

    if not response.success:
        raw = getattr(response, "raw_content", None) or getattr(response, "raw", None)
        if raw and ("Invalid" in str(response.error) or "escape" in str(response.error).lower()):
            print("  ⚠️  JSON parse error — attempting escape cleanup...")
            try:
                cleaned      = re.sub(r"\\u(?![0-9a-fA-F]{4})", r"\\\\u", raw)
                sections_out = json.loads(cleaned).get("sections", {})
                return {sn: data.get("elements", []) for sn, data in sections_out.items()}
            except Exception as e2:
                raise Exception(f"Batch LLM call failed (cleanup also failed): {e2}")
        raise Exception(f"Batch LLM call failed: {response.error}")

    print(f"  Batch tokens used: {response.tokens_used}")
    sections_out = response.content.get("sections", {})
    return {sn: data.get("elements", []) for sn, data in sections_out.items()}


def _splice_section(
    all_elements: list,
    section_number: str,
    new_section_elements: list,
    original_section_elements: list,
) -> list:
    heading_idx = next(
        (i for i, el in enumerate(all_elements)
         if re.search(r"/H[12](\[\d+\])?$", el.get("Path", ""))
         and (el.get("Text") or "").strip().startswith(f"{section_number} ")),
        None,
    )
    if heading_idx is None:
        return all_elements

    end_idx = heading_idx + 1
    while end_idx < len(all_elements):
        if re.search(r"/H[12](\[\d+\])?$", all_elements[end_idx].get("Path", "")):
            break
        t = (all_elements[end_idx].get("Text") or "").strip()
        if "Signature" in t or "Signature" in all_elements[end_idx].get("Path", ""):
            break
        end_idx += 1

    restored = _restore_rendering_fields(new_section_elements, original_section_elements)
    restored = _apply_donor_formatting(restored, all_elements)
    return all_elements[:heading_idx] + restored + all_elements[end_idx:]


# ── Public entry point ─────────────────────────────────────────────────────────

def run_prompt(approved_contents: list[dict]) -> PromptOutput:
    """
    Apply all approved changes for one product in a single pass.
      - TABLE  → Excel updated, prose_before → LLM UPDATE, prose_after → injected after table
      - ADD    → built in Python, no LLM
      - UPDATE/DELETE → one batched LLM call
    Saves the DOCX once at the end.
    """
    if not approved_contents:
        raise ValueError("No approved contents to process")

    product_code = approved_contents[0]["product_code"]
    pdf_path     = _get_pdf_path(product_code)
    struct_dict  = pdf_to_struct(pdf_path)
    tables_path  = settings.PROJECT_ROOT / "data" / "extracted" / pdf_path.stem / "tables"
    all_elements = struct_dict.get("elements", [])

    print(f"\n{'═'*60}")
    print(f"Batch update: {len(approved_contents)} change(s) for product {product_code}")
    print(f"{'═'*60}")

    add_changes:      list[dict] = []
    llm_changes:      list[dict] = []
    after_injections: list[dict] = []

    for content in approved_contents:
        sn = content["section_number"]
        st = content["section_title"]
        gt = content["generated_text"]

        # ── TABLE ─────────────────────────────────────────────────────────────
        if tableRouter(gt) == 1:
            print(f"  [TABLE] {sn} → updating Excel")
            _, pipe_rows, _ = split_table_content(gt)

            if not pipe_rows:
                print(f"  [TABLE] No pipe rows in generated_text — skipping {sn}")
                continue

            prose_before, prose_after = table_to_excel_llm(gt, str(tables_path))

            if _is_real_prose(prose_before) and not any(c["section_number"] == sn for c in llm_changes):
                print(f"  [TABLE PROSE BEFORE] Queuing UPDATE for {sn}")
                llm_changes.append({
                    "section_number": sn,
                    "section_title":  st,
                    "generated_text": prose_before.strip(),
                    "operation":      "UPDATE",
                })

            if prose_after.strip():
                after_injections.append({"section_number": sn, "prose_after": prose_after.strip()})
            continue

        # ── ADD ───────────────────────────────────────────────────────────────
        if not _extract_section_elements(all_elements, sn) and gt.strip():
            add_changes.append({"section_number": sn, "section_title": st, "generated_text": gt})
            continue

        # ── UPDATE / DELETE ───────────────────────────────────────────────────
        llm_changes.append({
            "section_number": sn,
            "section_title":  st,
            "generated_text": gt,
            "operation":      "DELETE" if not gt.strip() else "UPDATE",
        })

    # ── One LLM call for all UPDATE/DELETE ────────────────────────────────────
    if llm_changes:
        print(f"\n  Sending {len(llm_changes)} UPDATE/DELETE change(s) to LLM...")
        original_sections = {
            c["section_number"]: _extract_section_elements(all_elements, c["section_number"])
            for c in llm_changes
        }
        llm_results = _batch_llm_call(all_elements, llm_changes)
        for change in llm_changes:
            sn = change["section_number"]
            if sn in llm_results:
                print(f"  [SPLICE] {sn}")
                all_elements = _splice_section(
                    all_elements, sn, llm_results[sn], original_sections[sn],
                )

    # ── ADD in Python ─────────────────────────────────────────────────────────
    for change in add_changes:
        sn = change["section_number"]
        print(f"  [ADD] {sn}")
        new_elements, insert_idx = _build_add_elements(
            all_elements, sn, change["section_title"], change["generated_text"]
        )
        restored     = _apply_donor_formatting(new_elements, all_elements)
        all_elements = all_elements[:insert_idx] + restored + all_elements[insert_idx:]

    # ── Inject prose_after after table (done last so LLM splice doesn't wipe it) ──
    for inj in after_injections:
        sn_inj       = inj["section_number"]
        after_chunks = [_sanitize(c.strip()) for c in inj["prose_after"].split("\n\n") if c.strip()]

        heading_idx = next(
            (i for i, el in enumerate(all_elements)
             if re.search(r"/H\d+(\[\d+\])?$", el.get("Path", ""))
             and (el.get("Text") or "").strip().startswith(f"{sn_inj} ")),
            None,
        )
        if heading_idx is None:
            print(f"  [TABLE PROSE AFTER] Heading not found for {sn_inj} — skipping")
            continue

        # Find last filePaths element in section — insert after it
        insert_after = heading_idx
        section_end  = heading_idx + 1
        for j in range(heading_idx + 1, len(all_elements)):
            el_j = all_elements[j]
            t_j  = (el_j.get("Text") or "").strip()
            if re.search(r"/H[12](\[\d+\])?$", el_j.get("Path", "")):
                section_end = j; break
            if "Signature" in t_j or "Signature" in el_j.get("Path", ""):
                section_end = j; break
            if el_j.get("filePaths"):
                insert_after = j
            section_end = j + 1

        # Remove stale summary sentence (old value from original PDF)
        kept = [
            el for el in all_elements[insert_after + 1:section_end]
            if not re.match(r"^The\s+percentage", (el.get("Text") or "").strip(), re.IGNORECASE)
        ]
        page    = all_elements[insert_after].get("Page", 0)
        new_els = [
            {"Path": "//Document/P", "Page": page, "Text": chunk, "_injected": True}
            for chunk in after_chunks
        ]
        all_elements = (
            all_elements[:insert_after + 1]
            + new_els
            + kept
            + all_elements[section_end:]
        )
        print(f"  [TABLE PROSE AFTER] Injected {len(new_els)} line(s) after table for {sn_inj}")

    # ── Save ──────────────────────────────────────────────────────────────────
    full_output = PromptOutput(elements=all_elements)
    struct_to_docx({"elements": full_output.elements}, pdf_path.stem,product_code)
    print(f"\n✅ All changes applied. DOCX saved.")
    return full_output