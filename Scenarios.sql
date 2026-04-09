-- ============================================================
-- CIPHER DSG — VALIDATED TEST SCENARIOS
-- Target DB: Bayer (SQL Server)
-- Safe to run on a clean schema_corrected.sql baseline.
--
-- Next safe IDs (as of baseline):
--   RawMaterialID : 48, 49
--   AllergenID    : 2, 3, 4
--   TraceID       : 18+ (not used below)
-- ============================================================

BEGIN TRANSACTION;

-- ============================================================
-- SCENARIO 1: Product 1 — Wellcare Face Day Cream (1614322)
-- ============================================================
-- Changes:
--   a) New Parfum raw material added to formulation (NO = 0.00)
--   b) Two allergens declared on that new raw material
--   c) Remove existing CMR substance Toluene (TraceID 1, RawMaterialID 12)
-- ============================================================

-- (a) New raw material — IngredientID 31 = Parfum (already used in Lipstick)
--     Must insert RawMaterials BEFORE ProductFormulations (FK dependency)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (48, 31, 21, 'Test Parfum SC1234', 0.00);

-- Link to Product 1 — triggers trg_ProductFormulations_Change → logs to ProductChangeLog
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct)
VALUES (1, 48, 1.00);

-- (b) Allergens on the new raw material
--     Requires ProductFormulations row (above) to exist so trigger can resolve product_code
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (2, 48, 'Linalool',  'Declared allergen from Test Parfum SC1234'),
       (3, 48, 'Geraniol',  'Declared allergen from Test Parfum SC1234');

-- (c) Remove CMR: Toluene linked to RawMaterialID 12 (DL ALPHA TOCOPHERYL ACETATE) in Product 1
--     trg_RawMaterialTraces_Change resolves product_code via ProductFormulations for RawMaterialID 12
DELETE FROM RawMaterialTraces WHERE TraceID = 1;

-- ============================================================
-- SCENARIO 2: Product 2 — Wellcare Lipstick (1614557)
-- ============================================================
-- Changes:
--   a) New semi-synthetic Glycerin raw material added to formulation (NO = 0.50)
--   b) One allergen declared on that new raw material
-- ============================================================

-- (a) New raw material — IngredientID 3 = GLYCERIN
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (49, 3, 2, 'SEMI-SYNTH GLYCERIN KLK', 0.50);

-- Link to Product 2
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct)
VALUES (2, 49, 10.00);

-- (b) Allergen on the new raw material
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (4, 49, 'Limonene', 'Trace in semi-synthetic Glycerin');

COMMIT TRANSACTION;
-- ROLLBACK TRANSACTION;

-- ============================================================
-- TRIGGER COVERAGE SUMMARY
-- ============================================================
-- INSERT RawMaterials (48, 49)    → NO trigger (trg_RawMaterials_Change is AFTER UPDATE only)
--                                   NaturalOriginIndex NOT in ProductChangeLog, but visible
--                                   to LLM via context view at generation time.
-- INSERT ProductFormulations      → trg_ProductFormulations_Change fires → logs INCI_Name INSERT
-- INSERT RawMaterialAllergens     → trg_RawMaterialAllergens_Change fires → logs AllergenName INSERT
-- DELETE RawMaterialTraces        → trg_RawMaterialTraces_Change fires → logs SubstanceName DELETE
-- ============================================================
