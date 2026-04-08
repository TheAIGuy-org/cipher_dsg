"""
dossier_gen_engine/section_update.py
--------------------------------------
Applies section updates to a parsed sections list.

Result cases:
  REPLACE  — section number exists, titles are similar → swap content in-place
  INSERT   — section number does not exist             → insert in sorted order
  CONFLICT — section number exists, titles clearly differ → keep both, flag incoming
"""


# ============================================
# HELPERS
# ============================================

def parse_section(section_str: str) -> list[int]:
    """Convert '2.2.3' -> [2, 2, 3] for sorting."""
    return [int(x) for x in section_str.split(".")]


def titles_are_same(title_a: str, title_b: str) -> bool:
    """
    Decide if two section titles refer to the same section.
    Normalises case, whitespace, and punctuation before comparing.
    """
    def normalise(t: str) -> str:
        return t.lower().strip().rstrip(".")

    return normalise(title_a) == normalise(title_b)


# ============================================
# CONFLICT SECTION BUILDER
# ============================================

def build_conflict_pair(original: dict, incoming: dict) -> list[dict]:
    """
    When a conflict is detected, returns TWO section dicts:
      1. Original — untouched
      2. Incoming — title flagged with *, content prefixed with conflict notice
    Both appear in the final markdown for human review.
    """
    original_copy = dict(original)

    conflict_copy = {
        "section": incoming["section"],
        "title":   f"{incoming['title']} *[CONFLICT - REVIEW REQUIRED]*",
        "content": (
            f"CONFLICT: This section was submitted as an update but conflicts with "
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
) -> list[dict]:
    """
    Applies a single section update to the sections list.

    Args:
        sections_with_content : list of {"section", "title", "content"} dicts
        updated_section       : dict with keys: section, title, content

    Returns:
        Updated sections list.

    Behaviour:
        REPLACE  — same number, same title  -> swap content in-place, keep original title
        INSERT   — number not in list       -> insert in sorted order
        CONFLICT — same number, diff title  -> keep both, flag incoming
    """
    incoming_num   = updated_section["section"]
    incoming_title = updated_section["title"]

    # Find if section number already exists
    existing_idx = None
    for i, sec in enumerate(sections_with_content):
        if sec["section"] == incoming_num:
            existing_idx = i
            break

    # CASE 1: Number not found -> INSERT
    if existing_idx is None:
        print(f"  [UPDATE] INSERT  : {incoming_num} not found -> inserting in sorted order")
        sections_with_content.append(updated_section)
        sections_with_content.sort(key=lambda x: parse_section(x["section"]))
        return sections_with_content

    existing_sec   = sections_with_content[existing_idx]
    existing_title = existing_sec["title"]

    # CASE 2: Same number, same title -> REPLACE
    if titles_are_same(existing_title, incoming_title):
        print(f"  [UPDATE] REPLACE : {incoming_num} -> swapping content")
        sections_with_content[existing_idx] = {
            "section": incoming_num,
            "title":   existing_title,      # keep canonical original title
            "content": updated_section["content"]
        }
        return sections_with_content

    # CASE 3: Same number, different title -> CONFLICT
    print(f"  [UPDATE] CONFLICT: {incoming_num} exists with different title")
    print(f"    Existing : \"{existing_title}\"")
    print(f"    Incoming : \"{incoming_title}\"")

    conflict_pair = build_conflict_pair(existing_sec, updated_section)
    sections_with_content = (
        sections_with_content[:existing_idx]
        + conflict_pair
        + sections_with_content[existing_idx + 1:]
    )
    return sections_with_content


# ============================================
# MULTI-UPDATE WRAPPER
# ============================================

def apply_section_updates(
    sections_with_content: list[dict],
    updated_sections:      list[dict],
) -> list[dict]:
    """
    Apply multiple section updates sequentially.
    Each update is processed in order — conflicts flagged individually.
    """
    for update in updated_sections:
        sections_with_content = apply_section_update(
            sections_with_content,
            update,
        )
    return sections_with_content
