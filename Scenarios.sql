-- ============================================================
-- CIPHER DSG — VALIDATED TEST SCENARIOS (3-PRODUCT STAGGERED PIPELINE)
-- Target DB: Bayer (SQL Server)
-- Safe to run on a clean schema_corrected.sql baseline.
--
-- Next safe IDs (as of baseline):
--   RawMaterialID : 50+
--   AllergenID    : 5+
--   TraceID       : 18+
--
-- These scenarios trigger changes across ALL 3 dossiers in one
-- poll cycle, exercising the staggered multi-product pipeline:
--   P1: Wellcare Face Day Cream (1614322)
--   P2: Wellcare Lipstick       (1614557)
--   P3: Wellcare Cream           (1600188)
-- ============================================================

BEGIN TRANSACTION;

-- ============================================================
-- SCENARIO 1: Supplier switches Shea Butter raw material
--             across ALL 3 products
-- ============================================================
-- A shared supplier (Henry Lamotte, SupplierID 6) discontinues
-- its Shea Butter grade. A new raw material from a different
-- supplier replaces it in Product 1's formulation.
-- Products 2 and 3 get a new Tocopherol-based antioxidant from
-- the same new supplier — this raw material carries a CMR trace.
--
-- Trigger coverage:
--   INSERT RawMaterials (50, 51)  → trg_RawMaterials_Change (no fire — INSERT only fires for NaturalOriginIndex context view)
--   INSERT ProductFormulations    → trg_ProductFormulations_Change → logs INCI_Name INSERT
--   UPDATE RawMaterials (7)       → trg_RawMaterials_Change → logs SupplierName UPDATE, NaturalOriginIndex UPDATE
--   INSERT RawMaterialTraces (18) → trg_RawMaterialTraces_Change → logs SubstanceName INSERT
-- ============================================================

-- (a) Product 1 — Wellcare Face Day Cream (1614322)
--     Update Shea Butter supplier from Henry Lamotte (6) → BASF (8)
--     and change its NaturalOriginIndex from 1.00 → 0.85 (blended origin)
UPDATE RawMaterials
SET SupplierID = 8, NaturalOriginIndex = 0.85
WHERE RawMaterialID = 7;  -- SHEA BUTTER, REFINED (used in Product 1)

-- (b) Product 2 — Wellcare Lipstick (1614557)
--     Add new raw material: Tocopherol (Vitamin E) from BASF
--     with a CMR trace (Benzene)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (50, 34, 8, 'TOCOPHEROL BASF PREMIUM', 0.00);

INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct)
VALUES (2, 50, 0.15);

INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (18, 50, 'Benzene', 'CMR 1A', 2.0);

-- (c) Product 3 — Wellcare Cream (1600188)
--     Add same new Tocopherol raw material to its formulation
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct)
VALUES (3, 50, 0.10);


-- ============================================================
-- SCENARIO 2: New allergen declared on existing raw materials
--             in all 3 products
-- ============================================================
-- A regulatory update requires declaring Citronellol as an
-- allergen wherever Panthenol (D-PANTHENOL) is used.
-- D-Panthenol appears in:
--   Product 1: RawMaterialID 6  (SupplierID 5, DSM)
--   Product 2: RawMaterialID 31 (SupplierID 5, DSM)
--   Product 3: RawMaterialID 39 (SupplierID 25, ROCHE)
--
-- Additionally, Product 3 gets a second allergen (Eugenol)
-- on its Lanolin ingredient (RawMaterialID 44).
--
-- Trigger coverage:
--   INSERT RawMaterialAllergens → trg_RawMaterialAllergens_Change → logs AllergenName INSERT
-- ============================================================

-- (a) Product 1 — Allergen on D-Panthenol
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (5, 6, 'Citronellol', 'Regulatory update — allergen declaration required on D-Panthenol');

-- (b) Product 2 — Allergen on D-Panthenol
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (6, 31, 'Citronellol', 'Regulatory update — allergen declaration required on D-Panthenol');

-- (c) Product 3 — Allergen on D-Panthenol + Allergen on Lanolin
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (7, 39, 'Citronellol', 'Regulatory update — allergen declaration required on D-Panthenol');

INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (8, 44, 'Eugenol', 'Trace allergen identified in Lanolin (EWALAN 20) batch testing');


-- ============================================================
-- SCENARIO 3: CMR substance reclassification + removal
--             across all 3 products
-- ============================================================
-- EU regulatory body reclassifies Dichloromethane from CMR 2 to
-- CMR 1B (more severe). Affects:
--   Product 1: TraceID 2  (RawMaterialID 6,  D-Panthenol, 50 ppm)
--   Product 3: TraceID 14 (RawMaterialID 39, D-Panthenol, 50 ppm)
--
-- Product 2: Lipstick removes Dichloromethane entirely (trace was
-- on D-Panthenol, RawMaterialID 31) — supplier provided new CoA.
-- Also lower the ppm on Toluene for Uvinul A Plus (TraceID 3)
-- after reformulation.
--
-- Trigger coverage:
--   UPDATE RawMaterialTraces → trg_RawMaterialTraces_Change → logs Classification UPDATE, MaxLevelPPM UPDATE
--   DELETE RawMaterialTraces → trg_RawMaterialTraces_Change → logs SubstanceName DELETE
-- ============================================================

-- (a) Product 1 — Reclassify Dichloromethane to CMR 1B
UPDATE RawMaterialTraces
SET Classification = 'CMR 1B'
WHERE TraceID = 2;  -- Dichloromethane on D-Panthenol (Product 1)

-- (b) Product 2 — Remove Dichloromethane entirely + reduce Toluene ppm
DELETE FROM RawMaterialTraces
WHERE TraceID = 8;  -- Dichloromethane on D-Panthenol (Product 2, RawMaterialID 31)

UPDATE RawMaterialTraces
SET MaxLevelPPM = 50.0
WHERE TraceID = 3;  -- Toluene on Uvinul A Plus (Product 2) from 100.0 → 50.0

-- (c) Product 3 — Reclassify Dichloromethane to CMR 1B + lower ppm
UPDATE RawMaterialTraces
SET Classification = 'CMR 1B', MaxLevelPPM = 30.0
WHERE TraceID = 14;  -- Dichloromethane on D-Panthenol (Product 3) from 50.0 → 30.0

COMMIT TRANSACTION;
-- ROLLBACK TRANSACTION;

-- ============================================================
-- TRIGGER COVERAGE SUMMARY
-- ============================================================
-- Scenario 1:
--   UPDATE RawMaterials (7)       → trg_RawMaterials_Change → SupplierName UPDATE, NaturalOriginIndex UPDATE (P1)
--   INSERT RawMaterials (50)      → (no trigger on INSERT)
--   INSERT ProductFormulations    → trg_ProductFormulations_Change → INCI_Name INSERT (P2, P3)
--   INSERT RawMaterialTraces (18) → trg_RawMaterialTraces_Change → SubstanceName INSERT (P2)
--
-- Scenario 2:
--   INSERT RawMaterialAllergens (5,6,7,8) → trg_RawMaterialAllergens_Change → AllergenName INSERT (P1, P2, P3)
--
-- Scenario 3:
--   UPDATE RawMaterialTraces (2)  → trg_RawMaterialTraces_Change → Classification UPDATE (P1)
--   DELETE RawMaterialTraces (8)  → trg_RawMaterialTraces_Change → SubstanceName DELETE (P2)
--   UPDATE RawMaterialTraces (3)  → trg_RawMaterialTraces_Change → MaxLevelPPM UPDATE (P2)
--   UPDATE RawMaterialTraces (14) → trg_RawMaterialTraces_Change → Classification UPDATE, MaxLevelPPM UPDATE (P3)
--
-- Products hit:  P1 (1614322), P2 (1614557), P3 (1600188) — all three
-- Dossier sections affected:
--   2.2.1  (Reference Formula)      — P2, P3 via new formulation ingredient
--   2.2.2.1 (Allergens)             — P1, P2, P3 via new allergen declarations
--   2.2.2.2 (CMR Substances)        — P1, P2, P3 via trace changes
--   2.2.7  (Natural Origin)         — P1 via NaturalOriginIndex change on Shea Butter
-- ============================================================
