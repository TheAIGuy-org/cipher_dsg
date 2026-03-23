"""
test_run_prompt.py
------------------
Test: ADD section 2.2.8 Microbiological quality for product 1614557.
Section 2.2.8 does not exist — exercises the ADD operation.
"""
from updater.pdf_updater import run_prompt

PRODUCT_CODE   = "1614557"
SECTION_NUMBER = "2.2.8"
SECTION_TITLE  = "Microbiological quality"
GENERATED_TEXT = (
    "According to the information received from the manufacturers/suppliers, "
    "the cosmetic product complies with the microbiological criteria defined "
    "in Annex I of the Regulation (EC) N\u00b01223/2009 on Cosmetic products.\n\n"
    "Microbiological testing has been performed on the finished product and "
    "confirms the absence of Pseudomonas aeruginosa, Staphylococcus aureus, "
    "Candida albicans, and Escherichia coli. Total aerobic microbial count "
    "and total yeast and mould count are within the accepted limits."
)


if __name__ == "__main__":
    print(f"Running ADD for product '{PRODUCT_CODE}', section '{SECTION_NUMBER}'...")

    output = run_prompt(
        section_number = SECTION_NUMBER,
        section_title  = SECTION_TITLE,
        product_code   = PRODUCT_CODE,
        generated_text = GENERATED_TEXT,
    )

    print(f"\n✅ Done. {len(output.elements)} elements in updated struct.")