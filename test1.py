"""
test_run_prompt.py
------------------
Test: UPDATE section 2.2.2.2 Presence of CMR substances for product 1614557.

This is a realistic test case with:
  - Multiple normal paragraphs
  - Multiple bullets in a row
  - A normal paragraph after the bullets
  - All chunks separated by \\n\\n
"""
from updater.pdf_updater import run_prompt

PRODUCT_CODE   = "1614557"
SECTION_NUMBER = "2.2.2.2"
SECTION_TITLE  = "Presence of CMR substances"

GENERATED_TEXT = "\n\n".join([
    "According to the information received from the manufacturers/suppliers, "
    "the cosmetic product does not intentionally contain any substance classified "
    "as CMR 1A or 1B according to Regulation (EC) N\u00b01272/2008 (CLP).",

    "However, according to the information received from the raw materials suppliers:",

    "\u2022 Toluene (classified as CMR 2) may be found in the ingredient "
    "Diethylamino hydroxybenzoyl hexyl benzoate at a level of max. 50 ppm.",

    "\u2022 Dihexylphthalate (classified as CMR 1B) may be found in the ingredient "
    "Diethylamino hydroxybenzoyl hexyl benzoate at a level of max. 20 ppm.",

    "\u2022 Hexane (classified as CMR 2) may be found in the ingredient "
    "Helianthus annuus seed oil at a level of max. 1 ppm.",

    "According to the information received from the raw materials suppliers, "
    "other restricted substances listed in Annex III of Regulation (EC) N\u00b01223/2009 "
    "are also present as impurities at trace levels:",

    "\u2022 P-aminobenzoic acid may be found in the ingredient Ethylhexyl triazone "
    "at a level of max. 2000 ppm.",

    "\u2022 Diethylene glycol (DEG) may be found in the ingredient Glyceryl stearate "
    "citrate at a level of max. 100 ppm.",

    "Moreover, according to Article 17 of the Regulation (EC) 1223/2009 on cosmetic "
    "products as amended, the presence of these trace substances is technically "
    "unavoidable and their presence is safe.",
])


if __name__ == "__main__":
    print(f"Running UPDATE for product '{PRODUCT_CODE}', section '{SECTION_NUMBER}'...")
    print(f"\ngenerated_text chunks ({len(GENERATED_TEXT.split(chr(10)*2))}):")
    for i, chunk in enumerate(GENERATED_TEXT.split("\n\n")):
        bullet = "\u2022" if chunk.startswith("\u2022") else " "
        print(f"  [{i}] {bullet} {repr(chunk[:70])}")
    print()

    output = run_prompt(
        section_number = SECTION_NUMBER,
        section_title  = SECTION_TITLE,
        product_code   = PRODUCT_CODE,
        generated_text = GENERATED_TEXT,
    )

    print(f"\n\u2705 Done. {len(output.elements)} elements in updated struct.")