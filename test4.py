"""
test_run_prompt.py
------------------
Test: UPDATE section 2.2.2.2 Presence of CMR substances for product 1614322 (Face Day Cream).
Reflects the SQL update: Toluene MaxLevelPPM changed from 80 → 62.
"""
from updater.pdf_updater import run_prompt

PRODUCT_CODE   = "1614322"
SECTION_NUMBER = "2.2.2.2"
SECTION_TITLE  = "Presence of CMR substances"

GENERATED_TEXT = "\n\n".join([
    "According to the information received from the manufacturers/suppliers, the cosmetic "
    "product does not contain intentionally added substances classified as CMR substances.",

    "Traces of heavy metals can be found in all the ingredients. Their amount is limited to "
    "10 ppm max in each ingredient, except for Isopropyl isostearate, Cetearyl alcohol, "
    "Acacia Senegal gum/xanthan gum and Niacinamide (below 20ppm).",

    "According to the information received from the raw materials suppliers,",

    "\u2022 62 ppm of Toluene (classified as CMR 2) may be found in the ingredient "
    "Tocopheryl acetate,",

    "\u2022 50 ppm of Dichloromethane (classified as CMR 2) may be found in the "
    "ingredient Panthenol.",

    "The above-mentioned impurities are suitably controlled by the respective suppliers "
    "for all the ingredients and technically unavoidable.",

    "Moreover, according to Article 17 of the Regulation (EC) 1223/2009 on cosmetic "
    "products as amended, the non-intended presence of a small quantity of a prohibited "
    "substance, stemming from impurities of natural or synthetic ingredients, the "
    "manufacturing process, storage, migration from packaging, which is technically "
    "unavoidable in good manufacturing practices, shall be permitted provided that such "
    "presence is in conformity with Article 3.",
])


if __name__ == "__main__":
    print(f"Product       : {PRODUCT_CODE} (Face Day Cream)")
    print(f"Section       : {SECTION_NUMBER} {SECTION_TITLE}")
    print(f"Key change    : Toluene 80 ppm → 62 ppm")
    print(f"Chunks        : {len(GENERATED_TEXT.split(chr(10)*2))}")
    print()
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