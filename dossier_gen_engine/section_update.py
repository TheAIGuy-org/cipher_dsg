"""
dossier_gen_engine/section_update.py
--------------------------------------
Drop-in module for pdf_md.py pipeline.
THIS FILE IS UNCHANGED from the original finals/section_update.py.

Plugs in between Step 4 and Step 5:

    sections_with_content = build_section_content(...)

    # NEW STEP
    sections_with_content = apply_section_update(
        sections_with_content,
        updated_section,
        client,
        DEPLOYMENT
    )

    batches = build_batches(...)
"""

# ============================================
# CONTRACTS
# ============================================
#
# updated_section (input):
# {
#     "section": "2.2.3",          # section number string
#     "title":   "Nanomaterials",  # section title
#     "content": "RAW TEXT..."     # full raw content (NOT markdown)
# }
#
# Result cases:
#   REPLACE  — number exists, LLM says titles are SAME/similar
#              → swap content in-place, keep original title
#   INSERT   — number does not exist
#              → insert in sorted order
#   CONFLICT — number exists, LLM says titles are clearly DIFFERENT
#              → keep original, append incoming with * marker,
#                both go to LLM for formatting, visible in final MD
#
# ============================================

import re
from openai import AzureOpenAI


# ============================================
# HELPERS
# ============================================

def parse_section(section_str: str) -> list[int]:
    """Convert '2.2.3' → [2, 2, 3] for sorting."""
    return [int(x) for x in section_str.split(".")]


# ============================================
# TITLE SIMILARITY — LLM ALWAYS
# ============================================

TITLE_CHECK_PROMPT = """You are a strict document section comparator.

Given two section titles, decide if they refer to the SAME section or DIFFERENT sections.

Rules:
- Minor differences in casing, spacing, punctuation, or abbreviation = SAME
- Clearly different topics = DIFFERENT
- When in doubt, say DIFFERENT

Respond with EXACTLY one word: SAME or DIFFERENT. Nothing else."""


def titles_are_same(
    title_a:    str,
    title_b:    str,
    client:     AzureOpenAI,
    deployment: str
) -> bool:
    """
    Uses LLM to decide if two section titles refer to the same section.
    Returns True if SAME, False if DIFFERENT.
    """
    user_msg = f'Title A: "{title_a}"\nTitle B: "{title_b}"'

    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": TITLE_CHECK_PROMPT},
            {"role": "user",   "content": user_msg}
        ],
        temperature=0,
        max_tokens=5,
    )

    answer = response.choices[0].message.content.strip().upper()
    return answer == "SAME"


# ============================================
# CONFLICT SECTION BUILDER
# ============================================

def build_conflict_pair(original: dict, incoming: dict) -> list[dict]:
    """
    When a conflict is detected, returns TWO section dicts:
      1. Original — untouched, passes through to LLM as-is
      2. Incoming — title flagged with *, content prefixed with conflict notice

    Both will appear in the final markdown output for human review.
    """
    original_copy = dict(original)

    conflict_copy = {
        "section": incoming["section"],
        "title":   f"{incoming['title']} *[CONFLICT — REVIEW REQUIRED]*",
        "content": (
            f"⚠️ CONFLICT: This section was submitted as an update but conflicts with "
            f"an existing section carrying the same number ({incoming['section']}).\n"
            f"Original title : \"{original['title']}\"\n"
            f"Incoming title : \"{incoming['title']}\"\n\n"
            f"--- INCOMING CONTENT ---\n"
            f"{incoming['content']}"
        )
    }

    return [original_copy, conflict_copy]


# ============================================
# CORE: apply_section_update
# ============================================

def apply_section_update(
    sections_with_content: list[dict],
    updated_section:       dict,
    client:                AzureOpenAI,
    deployment:            str
) -> list[dict]:
    """
    Applies a single section update to the sections list BEFORE LLM formatting.

    Args:
        sections_with_content : output of build_section_content()
        updated_section       : dict with keys: section, title, content
        client                : AzureOpenAI client (same instance as pipeline)
        deployment            : deployment name string

    Returns:
        Updated sections list, sorted by section number.

    Behaviour:
        REPLACE  — same number, LLM says same/similar title → swap content in-place
        INSERT   — number not in list                       → insert in sorted order
        CONFLICT — same number, LLM says different title    → keep both, flag incoming
    """
    incoming_num   = updated_section["section"]
    incoming_title = updated_section["title"]

    # ── Find if section number already exists ──────────────────────────────
    existing_idx = None
    for i, sec in enumerate(sections_with_content):
        if sec["section"] == incoming_num:
            existing_idx = i
            break

    # ── CASE 1: Number not found → clean INSERT ────────────────────────────
    if existing_idx is None:
        print(f"  [UPDATE] INSERT  : {incoming_num} not found → inserting in sorted order")

        sections_with_content.append(updated_section)
        sections_with_content.sort(key=lambda x: parse_section(x["section"]))

        return sections_with_content

    # ── Number found: ask LLM whether titles are SAME or DIFFERENT ─────────
    existing_sec   = sections_with_content[existing_idx]
    existing_title = existing_sec["title"]

    print(f"  [UPDATE] Section {incoming_num} already exists — checking title via LLM...")
    print(f"    Existing : \"{existing_title}\"")
    print(f"    Incoming : \"{incoming_title}\"")

    same = titles_are_same(existing_title, incoming_title, client, deployment)

    # ── CASE 2: Same title → REPLACE content ──────────────────────────────
    if same:
        print(f"  [UPDATE] REPLACE : titles match → swapping content of {incoming_num}")

        sections_with_content[existing_idx] = {
            "section": incoming_num,
            "title":   existing_title,          # keep canonical original title
            "content": updated_section["content"]
        }
        return sections_with_content

    # ── CASE 3: Different title → CONFLICT ────────────────────────────────
    print(f"  [UPDATE] CONFLICT: {incoming_num} exists with different title")
    print(f"    Both sections will appear in markdown output. Incoming marked with *")

    conflict_pair = build_conflict_pair(existing_sec, updated_section)

    sections_with_content = (
        sections_with_content[:existing_idx]
        + conflict_pair
        + sections_with_content[existing_idx + 1:]
    )

    return sections_with_content


# ============================================
# MULTI-UPDATE WRAPPER (future-proof)
# ============================================

def apply_section_updates(
    sections_with_content: list[dict],
    updated_sections:      list[dict],
    client:                AzureOpenAI,
    deployment:            str
) -> list[dict]:
    """
    Apply multiple section updates sequentially.
    Each update is processed in order — conflicts flagged individually.

    Args:
        updated_sections : list of updated_section dicts

    Returns:
        Final updated sections list.
    """
    for update in updated_sections:
        sections_with_content = apply_section_update(
            sections_with_content,
            update,
            client,
            deployment
        )
    return sections_with_content