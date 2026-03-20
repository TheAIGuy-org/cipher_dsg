-- Heavy Metal

-- INSERT INTO RawMaterialTraces (
--     TraceID,
--     RawMaterialID,
--     SubstanceName,
--     Classification,
--     MaxLevelPPM
-- )
-- VALUES (
--     300,  -- New ID
--     12,   -- Castor Oil (used in Face Day Cream - ProductCode 1614322)
--     'Lead (Pb)',
--     'Heavy Metal - Regulated Substance',
--     10.0
-- );



--1. Add "Mercury" as an allergen for the same raw material that currently lists "Vanillin"
-- for ProductCode 1614557 (BEPANTHOL Lipstick).
 DECLARE @NewAllergenID INT = (SELECT ISNULL(MAX(AllergenID), 0) + 1
                              FROM dbo.RawMaterialAllergens);

INSERT INTO dbo.RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
SELECT TOP (1)
       @NewAllergenID,
       rma.RawMaterialID,
       N'Mercury',
       N'Added alongside existing Vanillin declaration'
FROM dbo.RawMaterialAllergens AS rma
JOIN dbo.RawMaterials         AS rm  ON rm.RawMaterialID = rma.RawMaterialID
JOIN dbo.ProductFormulations  AS pf  ON pf.RawMaterialID = rm.RawMaterialID
JOIN dbo.Products             AS p   ON p.ProductID = pf.ProductID
WHERE p.ProductCode = '1614557'
  AND rma.AllergenName = N'Vanillin'
  -- ensure we don't duplicate "Mercury" on the same raw material
  AND NOT EXISTS (
        SELECT 1
        FROM dbo.RawMaterialAllergens AS r2
        WHERE r2.RawMaterialID = rma.RawMaterialID
          AND r2.AllergenName  = N'Mercury'
  );

--2. Update the CMR value in FDC
  UPDATE dbo.RawMaterialTraces
SET MaxLevelPPM = 60.0
WHERE SubstanceName = 'Toluene'
  AND RawMaterialID IN (
        SELECT pf.RawMaterialID
        FROM dbo.ProductFormulations pf
        JOIN dbo.Products p ON p.ProductID = pf.ProductID
        WHERE p.ProductCode = '1614322'   -- Face Day Cream
    )
  AND MaxLevelPPM = 80.0;  -- safety condition


  --3. Allergen +CMR in FDC

  BEGIN TRY
    BEGIN TRAN;

    DECLARE @ProductCode VARCHAR(255) = '1614322';  -- Face Day Cream
    DECLARE @RM_ForAllergen INT;
    DECLARE @NewAllergenID INT;

    /* 1) Choose a deterministic RawMaterial used by Face Day Cream to attach the allergen.
          We'll use the raw material with the highest PercentageInProduct for this product.
          (This makes the script re-runnable and independent of specific IDs.) */
    SELECT TOP (1)
           @RM_ForAllergen = pf.RawMaterialID
    FROM dbo.ProductFormulations pf
    JOIN dbo.Products p ON p.ProductID = pf.ProductID
    WHERE p.ProductCode = @ProductCode
    ORDER BY pf.PercentageInProduct DESC, pf.RawMaterialID ASC;

    /* 1a) Insert allergen 'Mercury2' if it does not already exist on that raw material.
           Use a table lock around MAX(AllergenID)+1 to avoid race conditions. */
    IF NOT EXISTS (
        SELECT 1
        FROM dbo.RawMaterialAllergens a
        WHERE a.RawMaterialID = @RM_ForAllergen
          AND a.AllergenName  = N'Mercury2'
    )
    BEGIN
        SELECT @NewAllergenID = ISNULL(MAX(AllergenID), 0) + 1
        FROM dbo.RawMaterialAllergens WITH (TABLOCKX, HOLDLOCK);

        INSERT INTO dbo.RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
        VALUES (@NewAllergenID, @RM_ForAllergen, N'Mercury2',
                N'Introduced for Face Day Cream (testing change capture).');
    END

    /* 2) Update Dichloromethane 50 ppm ? 80 ppm for Face Day Cream only. */
    UPDATE rmt
    SET rmt.MaxLevelPPM = 80.0
    FROM dbo.RawMaterialTraces rmt
    JOIN dbo.ProductFormulations pf ON pf.RawMaterialID = rmt.RawMaterialID
    JOIN dbo.Products p ON p.ProductID = pf.ProductID
    WHERE p.ProductCode = @ProductCode
      AND rmt.SubstanceName = 'Dichloromethane'
      AND rmt.MaxLevelPPM = 50.0;  -- safety guard

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;

    -- surface the error
    THROW;
END CATCH;

-- Face Day Cream — 10 multi-section scenarios (ProductCode 1614322)

--1. D-1) Add gum allergen + tighten DCM limit + tweak % actives, Sections: Allergens, CMR, Reference Formula

PRINT 'FD-1: Allergens + CMR + PF tweaks';
BEGIN TRAN;

-- Allergens: add on gum (RM 15)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 200)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (200, 15, N'Acacia-derived proteins', N'Naturally occurring in gum');

-- CMR: tighten Dichloromethane on Panthenol (TraceID=2 baseline 50 -> 30)
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 2)
UPDATE RawMaterialTraces SET MaxLevelPPM = 30.0 WHERE TraceID = 2;

-- PF: small % rebalance (Glycerin +0.2, Niacinamide -0.2)
UPDATE pf SET pf.PercentageInProduct = 5.20000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 3;

UPDATE pf SET pf.PercentageInProduct = 1.80000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 8;

COMMIT;

--2. Add dioxane to Panthenol + swap Xanthan supplier + add structurant, Sections: CMR (INSERT), Reference Formula (UPDATE+INSERT), Natural Origin (via PF)

PRINT 'FD-2: CMR add + PF swap + PF add';
BEGIN TRAN;

-- CMR: add 1,4-dioxane to Panthenol (RM 6)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 261)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (261, 6, '1,4-dioxane', 'CMR 1B', 1.0);

-- PF swap: alt Xanthan (new RM 501 from supplier 23)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 501)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (501, 17, 23, 'Xanthan Gum (Alt Supplier)', 1.00);

UPDATE pf SET pf.RawMaterialID = 501
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 17;

-- PF add: microcrystalline cellulose as structurant (RM 502, 0.05%)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 502)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (502, 29, 19, 'Sensocel Fine', 1.00);

IF NOT EXISTS (
  SELECT 1 FROM ProductFormulations pf
  JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
  WHERE pf.RawMaterialID=502
)
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct)
SELECT p.ProductID, 502, 0.05000 FROM Products p WHERE p.ProductCode='1614322';

COMMIT;

-- 3. Remove toluene trace + add argan allergen + supplier change for Vit E, Sections: CMR (DELETE), Allergens (INSERT), Supplier (UPDATE)

PRINT 'FD-3: CMR delete + Allergen add + Supplier change';
BEGIN TRAN;

-- CMR: remove Toluene (TraceID=1) on Vit E acetate (RM 12)
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 1)
DELETE FROM RawMaterialTraces WHERE TraceID = 1;

-- Allergens: Argan oil (RM 16) declare Linalool
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 201)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (201, 16, 'Linalool', 'Declared by supplier');

-- Supplier: change Vit E acetate supplier DSM (5) -> BASF (8)
UPDATE RawMaterials SET SupplierID = 8 WHERE RawMaterialID = 12;

COMMIT;

--4. Swap Squalane RM + add acetaldehyde trace + add panthenol allergen, Sections: Reference Formula (UPDATE), CMR (INSERT), Allergens (INSERT)

PRINT 'FD-4: PF swap + CMR add + Allergen add';
BEGIN TRAN;

-- PF swap: Squalane (RM 11) -> new RM 503 from CRODA (supplier 9)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 503)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (503, 11, 9, 'CRODA Squalane', 1.00);

UPDATE pf SET pf.RawMaterialID = 503
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 11;

-- CMR: add acetaldehyde to Panthenol
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 262)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (262, 6, 'Acetaldehyde', 'CMR 2', 2.0);

-- Allergen: Panthenol-related impurity note
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 202)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (202, 6, 'Panthenol-related impurity', 'Trace-level declaration');

COMMIT;

--5. Rebalance for higher natural origin + declare Citral on argan, Sections: Reference Formula (UPDATE), Allergens (INSERT), (Natural Origin via PF)

PRINT 'FD-5: PF rebalance for NOI + Allergen add';
BEGIN TRAN;

-- PF: lower 1,2-hexanediol (synthetic) 2.75 -> 2.50
UPDATE pf SET pf.PercentageInProduct = 2.50000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 5;

-- PF: increase Shea butter 2.50 -> 2.80
UPDATE pf SET pf.PercentageInProduct = 2.80000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 7;

-- PF: increase Argan oil 0.20 -> 0.30
UPDATE pf SET pf.PercentageInProduct = 0.30000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 16;

-- Allergens on Argan: Citral
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 203)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (203, 16, 'Citral', 'Minor component in argan');

COMMIT;

--6. Remove Behenyl Alcohol + add formaldehyde on Panthenol + gum allergen, Sections: Reference Formula (DELETE), CMR (INSERT), Allergens (INSERT)

PRINT 'FD-6: PF delete + CMR add + Allergen add';
BEGIN TRAN;

-- PF: remove Behenyl Alcohol (RM 14)
DELETE pf
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 14;

-- CMR: add Formaldehyde 1 ppm to Panthenol
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 263)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (263, 6, 'Formaldehyde', 'CMR 1B', 1.0);

-- Allergen: gum proteins on RM 15 (if not already added)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 204)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (204, 15, N'Acacia proteins', N'Declared by supplier');

COMMIT;

--7. Replace emulsifier + reduce Niacinamide + add dioxane trace, Sections: Reference Formula (UPDATE), CMR (INSERT)

PRINT 'FD-7: PF swap + PF % tweak + CMR add';
BEGIN TRAN;

-- PF swap: EMULIUM MELLIFERA (RM 4) -> alt emulsifier (same INCI=4), RM 504
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 504)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (504, 4, 11, 'EMULIUM MELLIFERA (Alt Lot)', 1.00);

UPDATE pf SET pf.RawMaterialID = 504
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 4;

-- PF: reduce Niacinamide 2.0 -> 1.7
UPDATE pf SET pf.PercentageInProduct = 1.70000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 8;

-- CMR: add 1,4-dioxane to emulsifier
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 264)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (264, 504, '1,4-dioxane', 'CMR 2', 1.0);

COMMIT;

--8. Escalate Toluene class + tighten limit + change supplier for Vit E, Sections: CMR (UPDATE + UPDATE), Supplier (UPDATE)
PRINT 'FD-8: CMR class/limit updates + Supplier change';
BEGIN TRAN;

-- If TraceID=1 still exists: class CMR 2 -> CMR 1B; limit 80 -> 40 (example)
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 1)
BEGIN
  UPDATE RawMaterialTraces SET Classification = 'CMR 1B' WHERE TraceID = 1;
  UPDATE RawMaterialTraces SET MaxLevelPPM = 40.0        WHERE TraceID = 1;
END

-- Supplier: Vit E acetate DSM(5) -> CRODA(9)
UPDATE RawMaterials SET SupplierID = 9 WHERE RawMaterialID = 12;

COMMIT;

--9. Add shea allergen + add hexane trace to IPIS + tweak %s, Sections: Allergens (INSERT), CMR (INSERT), Reference Formula (UPDATE)

PRINT 'FD-9: Allergen add + CMR add + PF % tweaks';
BEGIN TRAN;

-- Allergen: Shea (RM 7) Tree nut proteins
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 205)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (205, 7, 'Tree nut proteins', 'Naturally present');

-- CMR: add Hexane 1 ppm to IPIS (RM 10)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 265)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (265, 10, 'Hexane', 'CMR 2', 1.0);

-- PF: minor balance on water to keep 100% (water is RM 1) +0.05; reduce Vit E 0.50 -> 0.45
UPDATE pf SET pf.PercentageInProduct = 0.45000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 12;

-- Increase water slightly if needed (example; comment out if you keep total unconstrained)
-- UPDATE pf SET pf.PercentageInProduct = 67.59000
-- FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
-- WHERE pf.RawMaterialID = 1;

COMMIT;

--10. Remove DCM trace + swap cetearyl alcohol + add gum allergen, Sections: CMR (DELETE), Reference Formula (UPDATE), Allergens (INSERT)

PRINT 'FD-10: CMR delete + PF swap + Allergen add';
BEGIN TRAN;

-- CMR: remove Dichloromethane (TraceID=2) if still present
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 2)
DELETE FROM RawMaterialTraces WHERE TraceID = 2;

-- PF swap: cetearyl alcohol (RM 9) -> alt RM 505 (same INCI=9, supplier 23)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 505)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (505, 9, 23, 'Cetearyl Alcohol (Alt)', 1.00);

UPDATE pf SET pf.RawMaterialID = 505
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614322'
WHERE pf.RawMaterialID = 9;

-- Allergens: gum system (RM 15) aromatic aldehydes
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 206)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (206, 15, 'Trace aromatic aldehydes', 'Supplier declaration');

COMMIT;

--B) Lipstick — 10 multi-section scenarios (ProductCode 1614557)
--1.  Add perfume allergen + tighten phthalate + rebalance waxes, Sections: Allergens, CMR (UPDATE), Reference Formula
PRINT 'LS-1: Allergen add + CMR limit tighten + PF rebalance';
BEGIN TRAN;

-- Allergen: add Limonene on perfume (RM 34)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 220)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (220, 34, 'Limonene', 'IFRA update');

-- CMR: Dihexylphthalate (TraceID=5) 500 -> 350 ppm
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 5)
UPDATE RawMaterialTraces SET MaxLevelPPM = 350.0 WHERE TraceID = 5;

-- PF: +0.1 Tribehenin (RM 21), -0.1 Castor oil (RM 20)
UPDATE pf SET pf.PercentageInProduct = 14.600
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 21;

UPDATE pf SET pf.PercentageInProduct = 14.600
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 20;

COMMIT;

--2. Add DMF trace on UV filter + remove microcrystalline + add antioxidant allergen, Sections: CMR (INSERT), Reference Formula (DELETE), Allergens (INSERT)
PRINT 'LS-2: CMR add + PF delete + Allergen add';
BEGIN TRAN;

-- CMR: add N,N-dimethylformamide on RM 27 (if not already present)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 281)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (281, 27, 'N,N-dimethylformamide', 'CMR 1B', 5.0);

-- PF: remove microcrystalline cellulose (RM 32)
DELETE pf
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 32;

-- Allergens: add Coumarin on perfume (RM 34)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 221)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (221, 34, 'Coumarin', 'New declaration');

COMMIT;

--3. Reclassify toluene + decrease limit + tweak UV filter %, Sections: CMR (UPDATE), Reference Formula (UPDATE)
PRINT 'LS-3: CMR class/limit + PF % tweak';
BEGIN TRAN;

-- CMR: Toluene (TraceID=3) CMR 2 -> CMR 1B; 100 -> 60
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 3)
BEGIN
  UPDATE RawMaterialTraces SET Classification = 'CMR 1B' WHERE TraceID = 3;
  UPDATE RawMaterialTraces SET MaxLevelPPM = 60.0        WHERE TraceID = 3;
END

-- PF: reduce Uvinul A plus (RM 22) 9.0 -> 8.7
UPDATE pf SET pf.PercentageInProduct = 8.700
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 22;

COMMIT;

--4.  Add allergen & update classification for DMF + supplier change for GSC, Sections: Allergens (INSERT), CMR (UPDATE), Supplier (UPDATE)


PRINT 'LS-4: Allergen add + CMR classification + Supplier change';
BEGIN TRAN;

-- Allergens: add Vanillin to perfume (RM 34)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 222)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (222, 34, 'Vanillin', 'Trace in perfume');

-- CMR: set Classification NULL->'CMR 2' for an existing neutralized record (e.g., TraceID 11 Nitrosamines removed in some runs)
-- Example: update TraceID 6 DMF on RM 27: keep class 'CMR 1B' but demonstrate class update to same (still logs)
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 6)
UPDATE RawMaterialTraces SET Classification = 'CMR 1B' WHERE TraceID = 6;

-- Supplier change on Dermofeel GSC SG (RM 36) from 23->8 (Evonik->BASF) as example
UPDATE RawMaterials SET SupplierID = 8 WHERE RawMaterialID = 36;

COMMIT;

--5. Swap Beeswax supplier + remove PABA trace + adjust pigment %, Sections: Reference Formula (UPDATE), CMR (DELETE), Reference Formula (UPDATE)

PRINT 'LS-5: PF swap + CMR delete + PF % tweak';
BEGIN TRAN;

-- PF swap: Beeswax (RM 33) -> new RM 521 (same INCI=30 supplier 20 kept or alt)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 521)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (521, 30, 20, 'Beeswax White (Alt Lot)', 1.00);

UPDATE pf SET pf.RawMaterialID = 521
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 33;

-- CMR: delete P-aminobenzoic acid (TraceID = 12)
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 12)
DELETE FROM RawMaterialTraces WHERE TraceID = 12;

-- PF: tweak pigment (RM 26 Ronaflair LF-2000) 5.0 -> 4.8
UPDATE pf SET pf.PercentageInProduct = 4.800
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 26;

COMMIT;

--6. Add aldehyde trace on perfume + add allergen + reduce Tinosorb S %, Sections: CMR (INSERT), Allergens (INSERT), Reference Formula (UPDATE)

PRINT 'LS-6: CMR add + Allergen add + PF % reduce';
BEGIN TRAN;

-- CMR: add Acetaldehyde 1 ppm to perfume (associate to RM 34 via a synthetic stub RM if needed)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 282)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (282, 34, 'Acetaldehyde', 'CMR 2', 1.0);

-- Allergens: add Linalool (if not already)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 223)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (223, 34, 'Linalool', 'IFRA-based disclosure');

-- PF: Tinosorb S (RM 27) 3.7 -> 3.5
UPDATE pf SET pf.PercentageInProduct = 3.500
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 27;

COMMIT;

--7. Reduce DMF limits + add vanillin note + swap structurant, Sections: CMR (UPDATE), Allergens (INSERT), Reference Formula (UPDATE)

PRINT 'LS-7: CMR limit reduce + Allergen add + PF swap';
BEGIN TRAN;

-- CMR: reduce DMF limit on RM 27 TraceID=6 from 10 -> 6
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 6)
UPDATE RawMaterialTraces SET MaxLevelPPM = 6.0 WHERE TraceID = 6;

-- Allergen: Vanillin on tocopherol mix (RM 35) as a trace
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 224)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (224, 35, 'Vanillin', 'Trace from antioxidant blend');

-- PF swap: tribehenin (RM 21) -> alt RM 522 (same INCI=20, supplier 9)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 522)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (522, 20, 9, 'Tribehenin (Alt)', 1.00);

UPDATE pf SET pf.RawMaterialID = 522
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 21;

COMMIT;

--8. Remove Nitrosamines + add Citrals + tweak waxes for NOI, Sections: CMR (DELETE), Allergens (INSERT), Reference Formula (UPDATE)

PRINT 'LS-8: CMR delete + Allergen add + PF % NOI shift';
BEGIN TRAN;

-- CMR: delete Nitrosamines (TraceID=11) if exists
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 11)
DELETE FROM RawMaterialTraces WHERE TraceID = 11;

-- Allergens: add Citral on perfume (RM 34)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 225)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (225, 34, 'Citral', 'Fragrance constituent');

-- PF: increase Rice wax 5.75 -> 6.00; decrease C10-18 triglycerides 5.00 -> 4.75
UPDATE pf SET pf.PercentageInProduct = 6.000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 24;

UPDATE pf SET pf.PercentageInProduct = 4.750
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 25;

COMMIT;

--9. DMF classification drop + add hexane on Vit E + remove fragrance, Sections: CMR (UPDATE + INSERT), Reference Formula (DELETE)

PRINT 'LS-9: CMR class drop + CMR add + PF delete';
BEGIN TRAN;

-- CMR: set Classification to NULL for TraceID=6 (DMF) to reflect vendor clarification
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 6)
UPDATE RawMaterialTraces SET Classification = NULL WHERE TraceID = 6;

-- CMR: add Hexane 0.5 ppm to Vit E Acetate (RM 28)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 283)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (283, 28, 'Hexane', 'CMR 2', 0.5);

-- PF: remove perfume (RM 34)
DELETE pf
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 34;

COMMIT;

--10. Swap castor oil supplier + add benz(a)pyrene limit + add allergen, Sections: Reference Formula (UPDATE), CMR (INSERT), Allergens (INSERT)

PRINT 'LS-10: PF supplier swap + CMR add + Allergen add';
BEGIN TRAN;

-- PF swap via RM change: castor oil (RM 20) -> new RM 523 (same INCI=19, supplier 28)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 523)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (523, 19, 28, 'Castor Oil Refined (Alt Supplier)', 1.00);

UPDATE pf SET pf.RawMaterialID = 523
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1614557'
WHERE pf.RawMaterialID = 20;

-- CMR: add Benzo(a)pyrene 0.001 ppm to castor oil
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 284)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (284, 523, 'Benzo(a)pyrene', 'CMR 1B', 0.001);

-- Allergens: add 'Benzaldehyde' trace on perfume is absent (perfume may not exist post LS-9); add to RM 35 instead
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 226)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (226, 35, 'Benzaldehyde', 'Trace impurity');

COMMIT;

--Cream — 10 multi-section scenarios (ProductCode 1600188)
--1. Lower 1,4-dioxane on preservative + add allergen on lanolin + tweak %s, Sections: CMR (UPDATE), Allergens (INSERT), Reference Formula (UPDATE)

PRINT 'CR-1: CMR limit lower + Allergen add + PF % tweak';
BEGIN TRAN;

-- CMR: 1,4-dioxane (TraceID=17) 2.0 -> 1.0 on RM 46
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 17)
UPDATE RawMaterialTraces SET MaxLevelPPM = 1.0 WHERE TraceID = 17;

-- Allergens: Lanolin (RM 44) 'Wool derivatives'
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 240)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (240, 44, 'Wool derivatives', 'Lanolin origin');

-- PF: reduce IPM 10.0 -> 9.6; increase Cetyl 4.0 -> 4.4
UPDATE pf SET pf.PercentageInProduct = 9.60000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 40;

UPDATE pf SET pf.PercentageInProduct = 4.40000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 41;

COMMIT;

--2.  Add ethylene oxide trace + swap emulsifier + add allergen on PG, Sections: CMR (INSERT), Reference Formula (UPDATE), Allergens (INSERT)

PRINT 'CR-2: CMR add + PF swap + Allergen add';
BEGIN TRAN;

-- CMR: add Ethylene oxide 0.5 ppm to preservative (RM 46)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 301)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (301, 46, 'Ethylene oxide', 'CMR 1B', 0.5);

-- PF swap: Stearyl alcohol (RM 42) -> alt RM 541 (same INCI=37)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 541)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (541, 37, 8, 'Stearyl Alcohol (Alt)', 1.00);

UPDATE pf SET pf.RawMaterialID = 541
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 42;

-- Allergens: add 'Aldehyde traces' to Propylene Glycol (RM 43)
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 241)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (241, 43, 'Aldehyde traces', 'From synthesis');

COMMIT;

--3. Remove DCM + add hexane on IPM + reduce preservative %, Sections: CMR (DELETE + INSERT), Reference Formula (UPDATE)

PRINT 'CR-3: CMR delete/add + PF % reduce';
BEGIN TRAN;

-- CMR: remove Dichloromethane on Panthenol (TraceID=14) if present
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 14)
DELETE FROM RawMaterialTraces WHERE TraceID = 14;

-- CMR: add Hexane 0.8 ppm to IPM (RM 40)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 302)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (302, 40, 'Hexane', 'CMR 2', 0.8);

-- PF: reduce Phenoxyethanol 0.5 -> 0.4
UPDATE pf SET pf.PercentageInProduct = 0.40000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 46;

COMMIT;

--4. Add allergen on Cetyl + tighten DEG on emulsifier + swap lanolin RM, Sections: Allergens (INSERT), CMR (UPDATE), Reference Formula (UPDATE)

PRINT 'CR-4: Allergen add + CMR tighten + PF swap';
BEGIN TRAN;

-- Allergens: Cetyl Alcohol (RM 41) 'Fatty alcohol traces'
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 242)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (242, 41, 'Fatty alcohol traces', 'Minor impurities');

-- CMR: tighten DEG (if present; use TraceID=303 new on RM 45)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 303)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (303, 45, 'Diethylene glycol (DEG)', NULL, 1000.0);

-- Now tighten limit 1000 -> 600 (this produces MaxLevelPPM UPDATE log)
UPDATE RawMaterialTraces SET MaxLevelPPM = 600.0 WHERE TraceID = 303;

-- PF swap: Lanolin (RM 44) -> alt RM 542 (same INCI=39 different supplier 20)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 542)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (542, 39, 20, 'Lanolin (Alt Supplier)', 1.00);

UPDATE pf SET pf.RawMaterialID = 542
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 44;

COMMIT;

--5. Multi % rebalance for NOI + add acetaldehyde to preservative, Sections: Reference Formula (UPDATE), CMR (INSERT)

PRINT 'CR-5: PF rebalance for NOI + CMR add';
BEGIN TRAN;

-- PF: increase water 73.35 -> 73.70
UPDATE pf SET pf.PercentageInProduct = 73.70000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 38;

-- PF: decrease IPM 10.0 -> 9.7
UPDATE pf SET pf.PercentageInProduct = 9.70000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 40;

-- CMR: add Acetaldehyde 1 ppm to preservative (RM 46)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 304)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (304, 46, 'Acetaldehyde', 'CMR 2', 1.0);

COMMIT;

--6. Supplier change for emulsifier + add allergen on IPM + lower dioxane, Sections: Supplier (UPDATE), Allergens (INSERT), CMR (UPDATE)


PRINT 'CR-6: Supplier change + Allergen add + CMR lower';
BEGIN TRAN;

-- Supplier: Potassium Cetyl Phosphate (RM 45) supplier 25->8
UPDATE RawMaterials SET SupplierID = 8 WHERE RawMaterialID = 45;

-- Allergens: IPM (RM 40) 'Isopropanol residuals'
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 243)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (243, 40, 'Isopropanol residuals', 'Trace from synthesis');

-- CMR: lower 1,4-dioxane on preservative TraceID=17 further 1.0 -> 0.5
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 17)
UPDATE RawMaterialTraces SET MaxLevelPPM = 0.5 WHERE TraceID = 17;

COMMIT;

--7. Remove ethylene glycol + add formaldehyde + reduce cetyl %, Sections: CMR (DELETE + INSERT), Reference Formula (UPDATE)

PRINT 'CR-7: CMR delete/add + PF % change';
BEGIN TRAN;

-- CMR: delete ethylene glycol (TraceID=15) if present
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 15)
DELETE FROM RawMaterialTraces WHERE TraceID = 15;

-- CMR: add Formaldehyde 0.8 ppm to preservative (RM 46)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 305)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (305, 46, 'Formaldehyde', 'CMR 1B', 0.8);

-- PF: decrease cetyl 4.4 -> 4.1
UPDATE pf SET pf.PercentageInProduct = 4.10000
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 41;

COMMIT;

--8. PF swap to triglyceride (replace IPM) + add dioxane on new RM + add lanolin allergen, Sections: Reference Formula (UPDATE), CMR (INSERT), Allergens (INSERT)

PRINT 'CR-8: PF emollient swap + CMR add + Allergen add';
BEGIN TRAN;

-- PF swap: IPM (RM 40) -> C10-18 triglycerides alt RM 543 (INCI=24, supplier 3)
IF NOT EXISTS (SELECT 1 FROM RawMaterials WHERE RawMaterialID = 543)
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex)
VALUES (543, 24, 3, 'Triglycerides (Alt)', 1.00);

UPDATE pf SET pf.RawMaterialID = 543
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 40;

-- CMR: add 1,4-dioxane 0.7 ppm to RM 543
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 306)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (306, 543, '1,4-dioxane', 'CMR 2', 0.7);

-- Allergens: Lanolin (RM 542 or 44) add 'Cholesterol-related traces'
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 244)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (244, 542, 'Cholesterol-related traces', 'From animal-derived source');

COMMIT;

--9. Bulk NOI shift: more water/lanolin, less IPM/preservative + add acetaldehyde, Sections: Reference Formula (UPDATE x4), CMR (INSERT)
PRINT 'CR-9: PF bulk NOI shift + CMR add';
BEGIN TRAN;

UPDATE pf SET pf.PercentageInProduct = 73.90000  -- water +
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 38;

UPDATE pf SET pf.PercentageInProduct = 0.30000   -- lanolin +
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID IN (44, 542);  -- whichever exists

UPDATE pf SET pf.PercentageInProduct = 9.50000   -- IPM -
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID IN (40, 543);  -- original or swapped

UPDATE pf SET pf.PercentageInProduct = 0.30000   -- phenoxyethanol -
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 46;

-- CMR: add Acetaldehyde 0.5 ppm to RM 41 (cetyl)
IF NOT EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 307)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM)
VALUES (307, 41, 'Acetaldehyde', 'CMR 2', 0.5);

COMMIT;

--10. Remove PG line + add allergen on Vit E + reduce dioxane limit, Sections: Reference Formula (DELETE), Allergens (INSERT), CMR (UPDATE)

PRINT 'CR-10: PF delete + Allergen add + CMR lower';
BEGIN TRAN;

-- PF: remove Propylene Glycol (RM 43)
DELETE pf
FROM ProductFormulations pf JOIN Products p ON p.ProductID=pf.ProductID AND p.ProductCode='1600188'
WHERE pf.RawMaterialID = 43;

-- Allergens: add 'Aromatic traces' to Vit E acetate source? Cream does not have Vit E acetate by default.
-- Use Panthenol (RM 39) instead:
IF NOT EXISTS (SELECT 1 FROM RawMaterialAllergens WHERE AllergenID = 245)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes)
VALUES (245, 39, 'Aromatic traces', 'From synthesis');

-- CMR: lower 1,4-dioxane (TraceID=17) 0.5 -> 0.3
IF EXISTS (SELECT 1 FROM RawMaterialTraces WHERE TraceID = 17)
UPDATE RawMaterialTraces SET MaxLevelPPM = 0.3 WHERE TraceID = 17;

COMMIT;