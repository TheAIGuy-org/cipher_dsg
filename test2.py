"""
test_run_prompt.py
------------------
Test cases covering all ADD scenarios:
  1. ADD top-level section    (H1) — 2.2.8 does not exist
  2. ADD subsection           (H2) — 2.2.2.4 does not exist inside 2.2.2
  3. UPDATE existing section  (H2) — 2.2.2.1 exists, replace content
  4. ADD subsection to last   (H2) — 2.2.7.1 does not exist inside 2.2.7

Run one at a time by setting ACTIVE_TEST below.
"""
from updater.pdf_updater import run_prompt

PRODUCT_CODE = "1614557"

# ─────────────────────────────────────────────
# TEST 1 — ADD top-level H1 section (2.2.8)
# ─────────────────────────────────────────────
TEST_1 = dict(
    section_number = "2.2.8",
    section_title  = "Microbiological quality",
    generated_text = "\n\n".join([
        "According to the information received from the manufacturers/suppliers, "
        "the cosmetic product complies with the microbiological criteria defined "
        "in Annex I of the Regulation (EC) N\u00b01223/2009 on Cosmetic products.",

        "Microbiological testing has been performed on the finished product and "
        "confirms the absence of Pseudomonas aeruginosa, Staphylococcus aureus, "
        "Candida albicans, and Escherichia coli. Total aerobic microbial count "
        "and total yeast and mould count are within the accepted limits.",
    ]),
)

# ─────────────────────────────────────────────
# TEST 2 — ADD subsection H2 inside 2.2.2 (2.2.2.4)
# ─────────────────────────────────────────────
TEST_2 = dict(
    section_number = "2.2.2.4",
    section_title  = "Presence of endocrine disruptors",
    generated_text = "\n\n".join([
        "According to the information received from the manufacturers/suppliers, "
        "an evaluation of the presence of endocrine disruptors has been carried out "
        "in accordance with the applicable guidance documents.",

        "No substances identified as endocrine disruptors under Regulation (EU) 2023/707 "
        "are intentionally used in the formulation of this cosmetic product.",

        "\u2022 Benzophenone-3 (oxybenzone) is not present in this formulation.",

        "\u2022 Triclosan has not been used as an ingredient in this product.",
    ]),
)

# ─────────────────────────────────────────────
# TEST 3 — UPDATE existing H2 subsection (2.2.2.1)
# ─────────────────────────────────────────────
TEST_3 = dict(
    section_number = "2.2.2.1",
    section_title  = "Presence of allergens",
    generated_text = "\n\n".join([
        "According to the information received from the suppliers, based on the "
        "Regulation (EC) N\u00b01223/2009 on Cosmetic products and its amendments, "
        "an evaluation of the presence of allergens in the cosmetic product was undertaken.",

        "The following allergen needs to be declared:",

        "\u2022 Vanillin, may be found in the perfume, with updated notes indicating "
        "its potential presence.",
    ]),
)

# ─────────────────────────────────────────────
# TEST 4 — ADD first subsection H2 inside 2.2.7 (2.2.7.1)
# ─────────────────────────────────────────────
TEST_4 = dict(
    section_number = "2.2.7.1",
    section_title  = "Natural origin calculation method",
    generated_text = "\n\n".join([
        "The percentage of natural origin has been calculated in accordance with "
        "ISO 16128-2:2017, using the natural origin indexes provided by the "
        "respective ingredient manufacturers.",

        "The calculation takes into account the natural origin index of each "
        "ingredient weighted by its concentration in the final formulation.",
    ]),
)


# ─────────────────────────────────────────────
# SELECT WHICH TEST TO RUN
# ─────────────────────────────────────────────
ACTIVE_TEST = TEST_4   # ← change to TEST_1 / TEST_2 / TEST_3 / TEST_4

EXPECTED_OP = {
    id(TEST_1): "ADD    H1 — 2.2.8 (new top-level after 2.2.7)",
    id(TEST_2): "ADD    H2 — 2.2.2.4 (new subsection inside 2.2.2)",
    id(TEST_3): "UPDATE H2 — 2.2.2.1 (replace allergens content)",
    id(TEST_4): "ADD    H2 — 2.2.7.1 (first subsection inside 2.2.7)",
}


if __name__ == "__main__":
    label = EXPECTED_OP.get(id(ACTIVE_TEST), "unknown")
    print(f"Test: {label}")
    print(f"section_number : {ACTIVE_TEST['section_number']}")
    print(f"chunks         : {len(ACTIVE_TEST['generated_text'].split(chr(10)*2))}")
    for i, chunk in enumerate(ACTIVE_TEST["generated_text"].split("\n\n")):
        bullet = "\u2022" if chunk.startswith("\u2022") else " "
        print(f"  [{i}] {bullet} {repr(chunk[:70])}")
    print()

    output = run_prompt(
        section_number = ACTIVE_TEST["section_number"],
        section_title  = ACTIVE_TEST["section_title"],
        product_code   = PRODUCT_CODE,
        generated_text = ACTIVE_TEST["generated_text"],
    )

    print(f"\n\u2705 Done. {len(output.elements)} elements in updated struct.")