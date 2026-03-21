-- ── Context View: Reference Formula (2.2.1) ─────────────────────────────────
-- Outputs: INCI Name, Commercial Name, Supplier Name (No Percentage)
CREATE OR ALTER VIEW dbo.vw_Context_ReferenceFormula AS
SELECT DISTINCT
    pf.ProductID,
    p.ProductCode,
    ing.INCI_Name,
    rm.CommercialName AS [Commercial Name],
    s.SupplierName AS [Supplier Name]
FROM dbo.ProductFormulations pf
JOIN dbo.RawMaterials rm ON pf.RawMaterialID = rm.RawMaterialID
JOIN dbo.Ingredients ing ON rm.IngredientID = ing.IngredientID
JOIN dbo.Suppliers s ON rm.SupplierID = s.SupplierID
JOIN dbo.Products p ON pf.ProductID = p.ProductID;
GO

-- ── Context View: Allergens (2.2.2.1) ───────────────────────────────────────
-- Outputs: INCI Name, Allergen Name
CREATE OR ALTER VIEW dbo.vw_Context_Allergens AS
SELECT DISTINCT
    pf.ProductID,
    p.ProductCode,
    ing.INCI_Name,
    rma.AllergenName
FROM dbo.RawMaterialAllergens rma
JOIN dbo.RawMaterials rm ON rma.RawMaterialID = rm.RawMaterialID
JOIN dbo.Ingredients ing ON rm.IngredientID = ing.IngredientID
JOIN dbo.ProductFormulations pf ON pf.RawMaterialID = rm.RawMaterialID
JOIN dbo.Products p ON pf.ProductID = p.ProductID;
GO

-- ── Context View: CMR Substances (2.2.2.2) ──────────────────────────────────
-- Outputs: INCI Name, Trace Substance, Trace Limit, Classification
CREATE OR ALTER VIEW dbo.vw_Context_CMR AS
SELECT DISTINCT
    pf.ProductID,
    p.ProductCode,
    ing.INCI_Name,
    rmt.SubstanceName AS [Trace Substance],
    rmt.MaxLevelPPM AS [Trace Limit],
    rmt.Classification
FROM dbo.RawMaterialTraces rmt
JOIN dbo.RawMaterials rm ON rmt.RawMaterialID = rm.RawMaterialID
JOIN dbo.Ingredients ing ON rm.IngredientID = ing.IngredientID
JOIN dbo.ProductFormulations pf ON pf.RawMaterialID = rm.RawMaterialID
JOIN dbo.Products p ON pf.ProductID = p.ProductID;
GO

-- ── Context View: Natural Origin (2.2.7) ────────────────────────────────────
-- Outputs: Product Percentage, Natural Origin Index, Ingredient Trade Name, Supplier
CREATE OR ALTER VIEW dbo.vw_Context_NaturalOrigin AS
SELECT DISTINCT
    pf.ProductID,
    p.ProductCode,
    ing.INCI_Name AS [Ingredient Name],
    rm.CommercialName AS [Trade Name],
    s.SupplierName AS [Manufacturer/Supplier],
    pf.PercentageInProduct AS [Product Percentage],
    rm.NaturalOriginIndex AS [Natural Origin Index]
FROM dbo.ProductFormulations pf
JOIN dbo.RawMaterials rm ON pf.RawMaterialID = rm.RawMaterialID
JOIN dbo.Ingredients ing ON rm.IngredientID = ing.IngredientID
JOIN dbo.Suppliers s ON rm.SupplierID = s.SupplierID
JOIN dbo.Products p ON pf.ProductID = p.ProductID;
GO
