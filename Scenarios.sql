-- Heavy Metal

INSERT INTO RawMaterialTraces (
    TraceID,
    RawMaterialID,
    SubstanceName,
    Classification,
    MaxLevelPPM
)
VALUES (
    300,  -- New ID
    12,   -- Castor Oil (used in Face Day Cream - ProductCode 1614322)
    'Lead (Pb)',
    'Heavy Metal - Regulated Substance',
    10.0
);



-- Add "Mercury" as an allergen for the same raw material that currently lists "Vanillin"
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

-- Update the CMR value in FDC
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


  -- Allergen +CMR in FDC

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