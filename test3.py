"""
test_run_prompt.py
------------------
Test: UPDATE section 2.2.2.1 Presence of allergens for product 1614557.
"""
from updater.pdf_updater import run_prompt

PRODUCT_CODE   = "1614557"
SECTION_NUMBER = "2.2.2.1"
SECTION_TITLE  = "Presence of allergens"

GENERATED_TEXT = "\n\n".join([
    "According to the information received from the suppliers, based on the "
    "Regulation (EC) N\u00b01223/2009 on Cosmetic products and its amendments, "
    "an evaluation of the presence of allergens in the cosmetic product was undertaken.",

    "The following allergen needs to be declared:",

    "\u2022 Vanillin, may be found in the perfume, with updated notes indicating "
    "its potential presence.",
])


if __name__ == "__main__":
    print(f"Running UPDATE for product '{PRODUCT_CODE}', section '{SECTION_NUMBER}'...")
    print(f"\ngenerated_text chunks ({len(GENERATED_TEXT.split(chr(10)*2))}):")
    for i, chunk in enumerate(GENERATED_TEXT.split("\n\n")):
        print(f"  [{i}] {repr(chunk[:80])}")
    print()

    output = run_prompt(
        section_number = SECTION_NUMBER,
        section_title  = SECTION_TITLE,
        product_code   = PRODUCT_CODE,
        generated_text = GENERATED_TEXT,
    )

    print(f"\n\u2705 Done. {len(output.elements)} elements in updated struct.")