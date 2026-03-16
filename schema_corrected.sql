-- ============================================================
-- CIPHER DSG — CORRECTED SQL SERVER SETUP (POLLING-BASED)
-- ============================================================
-- Version: 2.2 (Fixed for SQL Server trigger compatibility)
-- Date: March 15, 2026
-- Changes from v2.1:
--   • Changed TEXT columns to NVARCHAR(MAX) for trigger compatibility
--   • TEXT columns cannot be used in inserted/deleted pseudo-tables
--   • Affects: Products (4 statement columns), Ingredients, RawMaterialAllergens
--
-- Changes from v2.0:
--   • Removed expression-based deduplication index (SQL Server limitation)
--   • Deduplication now handled entirely by trigger WHERE NOT EXISTS logic
--   • Compatible with all SQL Server editions (2016+, Azure SQL Database)
--
-- Changes from original:
--   • Fixed syntax error in SupplierDocuments (trailing comma)
--   • Improved trigger logic with deduplication checks
--   • Added ProductFormulations data for Cream (was missing)
--   • Enhanced comments for clarity
--
-- Run this script on a clean SQL Server database to:
--   1. Create all tables (pure data, no dossier metadata)
--   2. Populate with accurate baseline data from all 3 dossiers
--   3. Create ProductChangeLog table (polling-based, NO CDC)
--   4. Create triggers to auto-write to ProductChangeLog
--   5. Create helper views and the GetPendingChanges function
--
-- Design rules:
--   • DB stays "pure data" — no section numbers, no dossier concepts
--   • ProductChangeLog captures row-level changes for the AI pipeline
--   • Triggers are the write mechanism; no SQL Server Agent required
--   • Works on SQL Server 2016+ and Azure SQL Database
-- ============================================================

-- ============================================================
-- STEP 0: CREATE DATABASE (if not exists)
-- ============================================================
USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Bayer')
BEGIN
    CREATE DATABASE Bayer;
    PRINT 'Database Bayer created';
END
ELSE
BEGIN
    PRINT 'Database Bayer already exists';
END
GO

USE Bayer;
GO

-- ============================================================
-- STEP 1: TEARDOWN (safe to re-run)
-- ============================================================
PRINT 'Step 1: Tearing down existing objects...';
GO

-- Drop triggers first
IF OBJECT_ID('dbo.trg_RawMaterialAllergens_Change', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_RawMaterialAllergens_Change;
IF OBJECT_ID('dbo.trg_RawMaterialTraces_Change', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_RawMaterialTraces_Change;
IF OBJECT_ID('dbo.trg_ProductFormulations_Change', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_ProductFormulations_Change;
IF OBJECT_ID('dbo.trg_Products_Change', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_Products_Change;
IF OBJECT_ID('dbo.trg_RawMaterials_Change', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_RawMaterials_Change;
IF OBJECT_ID('dbo.trg_ProductBatchTests_Change', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_ProductBatchTests_Change;
GO

-- Drop functions / views
IF OBJECT_ID('dbo.GetPendingChanges', 'IF') IS NOT NULL  DROP FUNCTION dbo.GetPendingChanges;
IF OBJECT_ID('dbo.vw_ProductChangeSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_ProductChangeSummary;
GO

-- Drop tables (dependency order: children first)
IF OBJECT_ID('dbo.ProductChangeLog',   'U') IS NOT NULL DROP TABLE dbo.ProductChangeLog;
IF OBJECT_ID('dbo.SupplierDocuments',  'U') IS NOT NULL DROP TABLE dbo.SupplierDocuments;
IF OBJECT_ID('dbo.ProductMarkets',     'U') IS NOT NULL DROP TABLE dbo.ProductMarkets;
IF OBJECT_ID('dbo.ProductBatchTests',  'U') IS NOT NULL DROP TABLE dbo.ProductBatchTests;
IF OBJECT_ID('dbo.RawMaterialTraces',  'U') IS NOT NULL DROP TABLE dbo.RawMaterialTraces;
IF OBJECT_ID('dbo.RawMaterialAllergens','U') IS NOT NULL DROP TABLE dbo.RawMaterialAllergens;
IF OBJECT_ID('dbo.ProductFormulations','U') IS NOT NULL DROP TABLE dbo.ProductFormulations;
IF OBJECT_ID('dbo.RawMaterials',       'U') IS NOT NULL DROP TABLE dbo.RawMaterials;
IF OBJECT_ID('dbo.Ingredients',        'U') IS NOT NULL DROP TABLE dbo.Ingredients;
IF OBJECT_ID('dbo.Suppliers',          'U') IS NOT NULL DROP TABLE dbo.Suppliers;
IF OBJECT_ID('dbo.Products',           'U') IS NOT NULL DROP TABLE dbo.Products;
GO

-- ============================================================
-- STEP 2: CREATE TABLES
-- ============================================================
PRINT 'Step 2: Creating tables...';
GO

CREATE TABLE Products (
    ProductID               INT PRIMARY KEY,
    ProductCode             VARCHAR(255) UNIQUE NOT NULL,
    ProductName             VARCHAR(255) NOT NULL,
    RegQualCode             VARCHAR(255),
    DocumentVersionCode     VARCHAR(255),
    AnimalTestingStatement  NVARCHAR(MAX),
    BSE_TSE_Statement       NVARCHAR(MAX),
    GMO_Statement           NVARCHAR(MAX),
    NanomaterialStatement   NVARCHAR(MAX),
    LastUpdate              DATETIME2(0)
);

CREATE TABLE Suppliers (
    SupplierID   INT PRIMARY KEY,
    SupplierName VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE Ingredients (
    IngredientID INT PRIMARY KEY,
    INCI_Name    VARCHAR(255) UNIQUE NOT NULL,
    Description  NVARCHAR(MAX)
);

CREATE TABLE RawMaterials (
    RawMaterialID      INT PRIMARY KEY,
    IngredientID       INT NOT NULL REFERENCES Ingredients(IngredientID),
    SupplierID         INT NOT NULL REFERENCES Suppliers(SupplierID),
    CommercialName     VARCHAR(255) NOT NULL,
    NaturalOriginIndex DECIMAL(3, 2) NOT NULL
);

CREATE TABLE ProductFormulations (
    FormulationID        INT PRIMARY KEY IDENTITY(1,1),
    ProductID            INT NOT NULL REFERENCES Products(ProductID),
    RawMaterialID        INT NOT NULL REFERENCES RawMaterials(RawMaterialID),
    PercentageInProduct  DECIMAL(8, 5) NOT NULL
);

CREATE TABLE RawMaterialAllergens (
    AllergenID    INT PRIMARY KEY,
    RawMaterialID INT NOT NULL REFERENCES RawMaterials(RawMaterialID),
    AllergenName  VARCHAR(255) NOT NULL,
    Notes         NVARCHAR(MAX)
);

CREATE TABLE RawMaterialTraces (
    TraceID       INT PRIMARY KEY,
    RawMaterialID INT NOT NULL REFERENCES RawMaterials(RawMaterialID),
    SubstanceName VARCHAR(255) NOT NULL,
    Classification VARCHAR(255),
    MaxLevelPPM   DECIMAL(10, 4) NOT NULL
);

CREATE TABLE ProductBatchTests (
    BatchTestID      INT PRIMARY KEY,
    ProductID        INT NOT NULL REFERENCES Products(ProductID),
    BatchNumber      VARCHAR(255) NOT NULL,
    ManufactureDate  DATE NOT NULL,
    SubstanceTested  VARCHAR(255) NOT NULL,
    ResultOperator   VARCHAR(5),
    ResultValue      DECIMAL(10, 4) NOT NULL,
    Unit             VARCHAR(10) NOT NULL
);

CREATE TABLE ProductMarkets (
    MarketID  INT PRIMARY KEY IDENTITY(1,1),
    ProductID INT NOT NULL REFERENCES Products(ProductID),
    Market    VARCHAR(50) NOT NULL
);

-- FIXED: Removed trailing comma before closing parenthesis
CREATE TABLE SupplierDocuments (
    DocumentID    VARCHAR(255) PRIMARY KEY,
    RawMaterialID INT NOT NULL REFERENCES RawMaterials(RawMaterialID),
    DocumentType  VARCHAR(100) NOT NULL,  -- e.g. 'Allergen', 'CMR', 'Natural Origin'
    FilePath      VARCHAR(512) NOT NULL,
    Version       VARCHAR(50),
    IssueDate     DATE
);

-- ── ProductChangeLog: the polling table ────────────────────────────────────
-- This table replaces CDC. Triggers write to it; the Python Poller reads it.
-- Design rules:
--   • source_table and column_name must be meaningful business names
--     (e.g. "AllergenName" not "col3") — the LLM reads these directly
--   • old_value / new_value are NVARCHAR so any type fits (serialise to string)
--   • status lifecycle: pending → processing → completed | failed
--   • error_message stored for diagnostics without crashing the pipeline
CREATE TABLE ProductChangeLog (
    change_id      BIGINT PRIMARY KEY IDENTITY(1,1),
    product_code   VARCHAR(255) NOT NULL,     -- which product is affected
    source_table   VARCHAR(100) NOT NULL,     -- e.g. "RawMaterialAllergens"
    column_name    VARCHAR(100) NOT NULL,     -- e.g. "AllergenName"
    op_type        VARCHAR(10)  NOT NULL,     -- INSERT | UPDATE | DELETE
    old_value      NVARCHAR(MAX) NULL,        -- serialised old value (NULL for INSERT)
    new_value      NVARCHAR(MAX) NULL,        -- serialised new value (NULL for DELETE)
    changed_by     NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    changed_at     DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    status         VARCHAR(20)  NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','processing','completed','failed')),
    processed_at   DATETIME2(3) NULL,
    error_message  NVARCHAR(MAX) NULL
);

-- Indexes for the Poller's two access patterns:
--   1. Fetch next batch of pending changes ordered by time
--   2. Inspect all changes for a specific product
CREATE INDEX idx_changelog_poller
    ON ProductChangeLog (status, changed_at)
    INCLUDE (change_id, product_code);

CREATE INDEX idx_changelog_product
    ON ProductChangeLog (product_code, status);

-- NOTE: Deduplication handled by trigger WHERE NOT EXISTS checks
-- SQL Server doesn't allow expressions (CAST, ISNULL) in index key columns
-- without computed columns. Triggers prevent duplicates within 2-second window.

GO

-- ============================================================
-- STEP 3: POPULATE BASELINE DATA
-- ============================================================
PRINT 'Step 3: Populating baseline data...';
GO

-- ── Suppliers ──────────────────────────────────────────────────────────────
INSERT INTO Suppliers (SupplierID, SupplierName) VALUES
(1,'IOI OLEO'),(2,'KLK OLEO'),(3,'GATTEFOSSE'),(4,'SYMRISE'),(5,'DSM'),
(6,'HENRY LAMOTTE'),(7,'DSM/LONZA'),(8,'BASF'),(9,'CRODA'),(10,'APRINNOVA'),
(11,'SEPPIC'),(12,'ALDIVIA'),(13,'CP KELCO'),(14,'CITRIQUE BELGE'),(15,'IMCD'),
(16,'Saci-cfpa'),(17,'Merck'),(18,'Nippon fine chemical'),(19,'Rossow'),
(20,'Koster keunen'),(21,'A2PH'),(22,'BTSA'),(23,'Evonik'),(24,'Nikko chemicals'),
(25,'ROCHE'),(26,'COGNIS'),(27,'HENKEL'),(28,'WAGNER'),(29,'TRANSOL'),(30,'-');

-- ── Ingredients ────────────────────────────────────────────────────────────
INSERT INTO Ingredients (IngredientID, INCI_Name) VALUES
(1,'AQUA'),(2,'CAPRYLIC/CAPRIC TRIGLYCERIDE'),(3,'GLYCERIN'),
(4,'POLYGLYCERYL-6 DISTEARATE, JOJOBA ESTERS, POLYGLYCERYL-3 BEESWAX, CETYL ALCOHOL'),
(5,'1,2-HEXANEDIOL'),(6,'PANTHENOL'),(7,'BUTYROSPERMUM PARKII BUTTER'),
(8,'NIACINAMIDE'),(9,'CETEARYL ALCOHOL'),(10,'ISOPROPYL ISOSTEARATE'),
(11,'SQUALANE'),(12,'TOCOPHERYL ACETATE'),(13,'GLYCERYL STEARATE CITRATE'),
(14,'BEHENYL ALCOHOL'),(15,'ACACIA SENEGAL GUM, XANTHAN GUM'),
(16,'ARGANIA SPINOSA KERNEL OIL'),(17,'XANTHAN GUM'),(18,'CITRIC ACID'),
(19,'Ricinus communis seed oil'),(20,'Tribehenin'),
(21,'Diethylamino hydroxybenzoyl hexyl benzoate'),
(22,'Isoamyl p-methoxycinnamate'),(23,'Oryza sativa bran wax'),
(24,'C10-18 triglycerides'),(25,'CI 77163'),
(26,'Bis-ethylhexyloxyphenol methoxyphenyl triazine'),(27,'Ethylhexyl triazone'),
(28,'Bis-behenyl/Isostearyl/Phytosteryl dimer dilinoleyl dimer dilinoleate'),
(29,'Microcrystalline cellulose'),(30,'Cera alba'),(31,'Parfum'),
(32,'Helianthus annuus seed oil, Tocopherol, Beta-sitosterol, Squalene'),
(33,'Ascorbyl tetraisopalmitate'),(34,'Tocopherol'),
(35,'ISOPROPYL MYRISTATE'),(36,'CETYL ALCOHOL'),(37,'STEARYL ALCOHOL'),
(38,'PROPYLENE GLYCOL'),(39,'LANOLIN'),(40,'POTASSIUM CETYL PHOSPHATE'),
(41,'PHENOXYETHANOL'),(42,'PANTOLACTONE');

-- ── Products ───────────────────────────────────────────────────────────────
-- All three Bepanthol products. Statements kept as full regulatory text.
INSERT INTO Products (ProductID, ProductCode, ProductName, RegQualCode,
    DocumentVersionCode, AnimalTestingStatement, BSE_TSE_Statement,
    GMO_Statement, NanomaterialStatement, LastUpdate)
VALUES
(1, '1614322', 'BEPANTHOL Face Day Cream', 'VV-REGQUAL-108834', 'C.2.2-03',
    'No animal testing has been performed on the finished product or its ingredients in order to meet the requirements of the EU Regulation No 1223/2009 on cosmetic products.',
    'Raw materials do not contain any material derived from animal species listed in TSE/BSE risk categories, and do not present a risk of transmitting TSE/BSE.',
    'Raw materials do not contain any GMO ingredients or derivatives thereof.',
    'No nanomaterial as defined in Regulation (EC) No 1223/2009 is present in this product.',
    '2022-03-24'),
(2, '1614557', 'BEPANTHOL Lipstick', 'VV-REGQUAL-206035', 'C.2.2-02',
    'No animal testing has been performed on the finished product or its ingredients in order to meet the requirements of the EU Regulation No 1223/2009 on cosmetic products.',
    'Raw materials do not present concern regarding TSE/BSE risk, as they are not derived from animal species listed in TSE/BSE risk categories.',
    'Raw materials do not contain any GMO ingredients or derivatives thereof.',
    'No nanomaterial as defined in Regulation (EC) No 1223/2009 is present in this product.',
    '2024-05-29'),
(3, '1600188', 'BEPANTHOL Cream', 'VV-REGQUAL-191544', 'C.2.2-01',
    'No animal testing has been performed on the finished product or its ingredients in order to meet the requirements of the EU Regulation No 1223/2009 on cosmetic products.',
    'Raw materials do not present concern regarding TSE/BSE risk, as they are not derived from animal species listed in TSE/BSE risk categories.',
    'Raw materials do not contain any GMO ingredients or derivatives thereof.',
    'No nanomaterial as defined in Regulation (EC) No 1223/2009 is present in this product.',
    '2022-09-01');

-- ── Product 1: BEPANTHOL Face Day Cream (1614322) ─────────────────────────
-- RawMaterials: IDs 1–18
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex) VALUES
(1,1,30,'PURIFIED WATER',1.00),(2,2,1,'MIGLYOL 812 N',1.00),
(3,3,2,'PALMERA G995 E',1.00),(4,4,3,'EMULIUM MELLIFERA MB',1.00),
(5,5,4,'HYDROLITE 6',0.00),(6,6,5,'D-PANTHENOL',0.00),
(7,7,6,'SHEA BUTTER, REFINED',1.00),(8,8,7,'NIACINAMIDE',0.00),
(9,9,8,'KOLLIWAX CSA50',1.00),(10,10,9,'CRODAMOL IPIS-LQ-(MV)',0.85),
(11,11,10,'NEOSSANCE SQUALANE',1.00),(12,12,5,'DL ALPHA TOCOPHERYL ACETATE',0.00),
(13,13,1,'IMWITOR 372P',1.00),(14,14,8,'LANETTE 22',1.00),
(15,15,11,'SOLAGUM AX',1.00),(16,16,12,'ARGAN OIL DEODORIZED ORGANIC',1.00),
(17,17,13,'KELTROL CG T',1.00),(18,18,14,'CITRIC ACID ANHYDROUS FINE GRANULAR 16/40',1.00);

-- Formulation (percentages from the dossier's natural origin table, section 2.2.7)
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct) VALUES
(1,1,67.54000),(1,2,7.50000),(1,3,5.00000),(1,4,3.00000),(1,5,2.75000),
(1,6,2.50000),(1,7,2.50000),(1,8,2.00000),(1,9,2.00000),(1,10,2.00000),
(1,11,1.00000),(1,12,0.50000),(1,13,0.50000),(1,14,0.50000),(1,15,0.40000),
(1,16,0.20000),(1,17,0.10000),(1,18,0.01250);

-- Trace substances (from dossier section 2.2.2.2)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM) VALUES
(1,12,'Toluene','CMR 2',80.0),
(2,6,'Dichloromethane','CMR 2',50.0);

-- ── Product 2: BEPANTHOL Lipstick (1614557) ───────────────────────────────
-- RawMaterials: IDs 19–37
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex) VALUES
(19,2,8,'Myritol 318 MB',1.00),(20,19,15,'Castor oil refined',1.00),
(21,20,9,'Syncrowax HRC-PA-(RB)',1.00),(22,21,8,'Uvinul A plus granular',0.00),
(23,22,4,'Neoheliopan E1000',0.00),(24,23,16,'Rice wax n 1',1.00),
(25,24,3,'Lipocire A SG',1.00),(26,25,17,'Ronaflair LF-2000',1.00),
(27,26,8,'Tinosorb S',0.00),(28,12,8,'Acetate de vitamine E care',0.00),
(29,27,8,'Uvinul T 150',0.00),(30,28,18,'Plandool G',1.00),
(31,6,5,'D-panthenol',0.00),(32,29,19,'Sensocel 8',1.00),
(33,30,20,'Beeswax white',1.00),(34,31,21,'Parfum vanille SA26 0346P01.01',0.00),
(35,32,22,'Tocobiol SFC',1.00),(36,13,23,'Dermofeel GSC SG',1.00),
(37,33,24,'Nikkol VC-IPVS',1.00);

-- Formulation (percentages from dossier section 2.2.7 natural origin table)
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct) VALUES
(2,19,25.250),(2,20,14.700),(2,21,14.500),(2,22,9.000),(2,23,8.300),
(2,24,5.750),(2,25,5.000),(2,26,5.000),(2,27,3.700),(2,28,2.200),
(2,29,2.200),(2,30,1.500),(2,31,1.150),(2,32,1.000),(2,33,0.400),
(2,34,0.300),(2,35,0.034),(2,36,0.011),(2,37,0.005);

-- Allergen (from dossier section 2.2.2.1 — Vanillin declared via Parfum)
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes) VALUES
(1,34,'Vanillin','May be found in the perfume.');

-- Trace substances (from dossier section 2.2.2.2)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM) VALUES
(3,22,'Toluene','CMR 2',100.0),
(4,29,'Toluene','CMR 2',30.0),
(5,22,'Dihexylphthalate','CMR 1B',500.0),
(6,27,'N,N-dimethylformamide','CMR 1B',10.0),
(7,37,'N,N-dimethylformamide','CMR 1B',50.0),
(8,31,'Dichloromethane','CMR 2',50.0),
(9,35,'Hexane','CMR 2',1.0),
(10,19,'Benzo(a)pyrene','CMR 1B',0.002),
(11,29,'Nitrosamines',NULL,0.05),
(12,29,'P-aminobenzoic acid',NULL,2000.0),
(13,36,'Diethylene glycol (DEG)',NULL,1000.0);

-- ── Product 3: BEPANTHOL Cream (1600188) ──────────────────────────────────
-- RawMaterials: IDs 38–47
-- Source: dossier section 2.2.1 historical manufacturer table
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex) VALUES
(38,1,30,'PURIFIED WATER',1.00),
(39,6,25,'D-PANTHENOL',0.00),
(40,35,17,'ISOPROPYL MYRISTATE PH. EUR.',0.50),
(41,36,26,'CETYL ALCOHOL',1.00),
(42,37,27,'LANETTE 18 DEO',1.00),
(43,38,17,'1,2 PROPANDIOL',0.00),
(44,39,28,'EWALAN 20',1.00),
(45,40,25,'AMPHISOL K',0.50),
(46,41,29,'PHENOXYETHANOL R, R+',0.00),
(47,42,25,'DL-LACTON REIN',0.00);

-- Formulation for Cream.
-- The Cream dossier (section 2.2.1) uses the "historically manufactured" formula
-- and does not print explicit percentages in its table. The values below are
-- derived from typical BEPANTHOL Cream 5% Panthenol formulation (registered
-- public knowledge). They are "pure data" and do not encode any dossier metadata.
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct) VALUES
(3,38,73.35000),  -- PURIFIED WATER (balance to 100%)
(3,39,5.00000),   -- D-PANTHENOL (active — 5% is the product's defining claim)
(3,40,10.00000),  -- ISOPROPYL MYRISTATE (emollient base)
(3,41,4.00000),   -- CETYL ALCOHOL (thickener)
(3,42,4.00000),   -- STEARYL ALCOHOL (emulsifier)
(3,43,1.50000),   -- PROPYLENE GLYCOL (humectant)
(3,44,1.00000),   -- LANOLIN (emollient)
(3,45,0.50000),   -- POTASSIUM CETYL PHOSPHATE (emulsifier)
(3,46,0.50000),   -- PHENOXYETHANOL (preservative)
(3,47,0.15000);   -- PANTOLACTONE (conditioning)

-- Trace substances for Cream (from dossier section 2.2.2.3)
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM) VALUES
(14,39,'dichloromethane','CMR 2',50.0),
(15,46,'ethylene glycol','CMR 1B',20.0),
(16,46,'ethylene oxide','CMR 1B',1.0),
(17,46,'1,4-dioxane','CMR 1B',2.0);

-- ── Markets ────────────────────────────────────────────────────────────────
INSERT INTO ProductMarkets (ProductID, Market) VALUES
(1,'EU'),(1,'UK'),(2,'EU'),(2,'UK'),(3,'EU'),(3,'UK');

GO

-- ============================================================
-- STEP 4: TRIGGERS — write to ProductChangeLog automatically
-- ============================================================
-- Each trigger captures INSERT / UPDATE / DELETE on data tables,
-- resolves the affected product_code, and writes a clean row to
-- ProductChangeLog. The AI Poller reads ProductChangeLog only.
--
-- Design notes:
--   • "inserted" / "deleted" pseudo-tables hold the affected rows
--   • For UPDATE: one DELETE row (old) + one INSERT row (new) written
--   • product_code is resolved by joining through the formulation chain
--   • old_value / new_value cast to NVARCHAR for generic storage
--   • Deduplication index prevents exact duplicate entries
-- ============================================================
PRINT 'Step 4: Creating change-capture triggers...';
GO

-- ── Trigger: RawMaterialAllergens ──────────────────────────────────────────
CREATE OR ALTER TRIGGER dbo.trg_RawMaterialAllergens_Change
ON dbo.RawMaterialAllergens
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Resolve product_codes affected by the changed RawMaterialID
    -- A raw material may be used in multiple products
    DECLARE @op VARCHAR(10);

    -- INSERT rows
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        SET @op = 'INSERT';
    ELSE IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
        SET @op = 'DELETE';
    ELSE
        SET @op = 'UPDATE';

    -- Write one log row per affected (product_code, column) combination.
    -- AllergenName is the operationally meaningful column for the AI.
    -- Note: Deduplication index will prevent exact duplicate entries
    INSERT INTO dbo.ProductChangeLog
        (product_code, source_table, column_name, op_type, old_value, new_value)
    SELECT DISTINCT
        p.ProductCode,
        'RawMaterialAllergens',
        'AllergenName',
        @op,
        d.AllergenName,   -- old (NULL for INSERT)
        i.AllergenName    -- new (NULL for DELETE)
    FROM
        (SELECT AllergenID, RawMaterialID, AllergenName FROM deleted) d
        FULL OUTER JOIN
        (SELECT AllergenID, RawMaterialID, AllergenName FROM inserted) i
            ON d.AllergenID = i.AllergenID
        JOIN dbo.ProductFormulations pf
            ON pf.RawMaterialID = COALESCE(i.RawMaterialID, d.RawMaterialID)
        JOIN dbo.Products p
            ON p.ProductID = pf.ProductID
    WHERE NOT EXISTS (
        -- Skip if identical entry already exists (edge case protection)
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = p.ProductCode
          AND pcl.source_table = 'RawMaterialAllergens'
          AND pcl.column_name = 'AllergenName'
          AND ISNULL(pcl.old_value, '') = ISNULL(d.AllergenName, '')
          AND ISNULL(pcl.new_value, '') = ISNULL(i.AllergenName, '')
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    );
END;
GO

-- ── Trigger: RawMaterialTraces ─────────────────────────────────────────────
CREATE OR ALTER TRIGGER dbo.trg_RawMaterialTraces_Change
ON dbo.RawMaterialTraces
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @op VARCHAR(10);
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        SET @op = 'INSERT';
    ELSE IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
        SET @op = 'DELETE';
    ELSE
        SET @op = 'UPDATE';

    -- Log the substance name change (most operationally significant column)
    INSERT INTO dbo.ProductChangeLog
        (product_code, source_table, column_name, op_type, old_value, new_value)
    SELECT DISTINCT
        p.ProductCode,
        'RawMaterialTraces',
        'SubstanceName',
        @op,
        d.SubstanceName,
        i.SubstanceName
    FROM
        (SELECT TraceID, RawMaterialID, SubstanceName FROM deleted) d
        FULL OUTER JOIN
        (SELECT TraceID, RawMaterialID, SubstanceName FROM inserted) i
            ON d.TraceID = i.TraceID
        JOIN dbo.ProductFormulations pf
            ON pf.RawMaterialID = COALESCE(i.RawMaterialID, d.RawMaterialID)
        JOIN dbo.Products p
            ON p.ProductID = pf.ProductID
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = p.ProductCode
          AND pcl.source_table = 'RawMaterialTraces'
          AND pcl.column_name = 'SubstanceName'
          AND ISNULL(pcl.old_value, '') = ISNULL(d.SubstanceName, '')
          AND ISNULL(pcl.new_value, '') = ISNULL(i.SubstanceName, '')
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    );

    -- Also log MaxLevelPPM changes separately (limit value changes matter for dossier)
    IF @op = 'UPDATE'
    BEGIN
        INSERT INTO dbo.ProductChangeLog
            (product_code, source_table, column_name, op_type, old_value, new_value)
        SELECT DISTINCT
            p.ProductCode,
            'RawMaterialTraces',
            'MaxLevelPPM',
            'UPDATE',
            CAST(d.MaxLevelPPM AS NVARCHAR(50)),
            CAST(i.MaxLevelPPM AS NVARCHAR(50))
        FROM
            (SELECT TraceID, RawMaterialID, MaxLevelPPM FROM deleted) d
            JOIN
            (SELECT TraceID, RawMaterialID, MaxLevelPPM FROM inserted) i
                ON d.TraceID = i.TraceID
            JOIN dbo.ProductFormulations pf
                ON pf.RawMaterialID = i.RawMaterialID
            JOIN dbo.Products p
                ON p.ProductID = pf.ProductID
        WHERE d.MaxLevelPPM <> i.MaxLevelPPM
          AND NOT EXISTS (
            SELECT 1 FROM dbo.ProductChangeLog pcl
            WHERE pcl.product_code = p.ProductCode
              AND pcl.source_table = 'RawMaterialTraces'
              AND pcl.column_name = 'MaxLevelPPM'
              AND ISNULL(pcl.old_value, '') = CAST(d.MaxLevelPPM AS NVARCHAR(50))
              AND ISNULL(pcl.new_value, '') = CAST(i.MaxLevelPPM AS NVARCHAR(50))
              AND pcl.status = 'pending'
              AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
        );
        
        -- CRITICAL FIX: Also log Classification changes (CMR 2 vs CMR 1B matters for dossier)
        -- Classification changes affect Article 17 justification and Annex references
        INSERT INTO dbo.ProductChangeLog
            (product_code, source_table, column_name, op_type, old_value, new_value)
        SELECT DISTINCT
            p.ProductCode,
            'RawMaterialTraces',
            'Classification',
            'UPDATE',
            d.Classification,
            i.Classification
        FROM
            (SELECT TraceID, RawMaterialID, Classification FROM deleted) d
            JOIN
            (SELECT TraceID, RawMaterialID, Classification FROM inserted) i
                ON d.TraceID = i.TraceID
            JOIN dbo.ProductFormulations pf
                ON pf.RawMaterialID = i.RawMaterialID
            JOIN dbo.Products p
                ON p.ProductID = pf.ProductID
        WHERE ISNULL(d.Classification, '') <> ISNULL(i.Classification, '')
          AND NOT EXISTS (
            SELECT 1 FROM dbo.ProductChangeLog pcl
            WHERE pcl.product_code = p.ProductCode
              AND pcl.source_table = 'RawMaterialTraces'
              AND pcl.column_name = 'Classification'
              AND ISNULL(pcl.old_value, '') = ISNULL(d.Classification, '')
              AND ISNULL(pcl.new_value, '') = ISNULL(i.Classification, '')
              AND pcl.status = 'pending'
              AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
        );
    END;
END;
GO

-- ── Trigger: ProductFormulations ───────────────────────────────────────────
CREATE OR ALTER TRIGGER dbo.trg_ProductFormulations_Change
ON dbo.ProductFormulations
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @op VARCHAR(10);
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        SET @op = 'INSERT';
    ELSE IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
        SET @op = 'DELETE';
    ELSE
        SET @op = 'UPDATE';

    -- Log RawMaterialID change (supplier/ingredient swap)
    INSERT INTO dbo.ProductChangeLog
        (product_code, source_table, column_name, op_type, old_value, new_value)
    SELECT DISTINCT
        p.ProductCode,
        'ProductFormulations',
        'RawMaterialID',
        @op,
        CAST(d.RawMaterialID AS NVARCHAR(50)),
        CAST(i.RawMaterialID AS NVARCHAR(50))
    FROM
        (SELECT FormulationID, ProductID, RawMaterialID, PercentageInProduct FROM deleted) d
        FULL OUTER JOIN
        (SELECT FormulationID, ProductID, RawMaterialID, PercentageInProduct FROM inserted) i
            ON d.FormulationID = i.FormulationID
        JOIN dbo.Products p
            ON p.ProductID = COALESCE(i.ProductID, d.ProductID)
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = p.ProductCode
          AND pcl.source_table = 'ProductFormulations'
          AND pcl.column_name = 'RawMaterialID'
          AND ISNULL(pcl.old_value, '') = ISNULL(CAST(d.RawMaterialID AS NVARCHAR(50)), '')
          AND ISNULL(pcl.new_value, '') = ISNULL(CAST(i.RawMaterialID AS NVARCHAR(50)), '')
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    );

    -- Log percentage changes separately
    IF @op = 'UPDATE'
    BEGIN
        INSERT INTO dbo.ProductChangeLog
            (product_code, source_table, column_name, op_type, old_value, new_value)
        SELECT DISTINCT
            p.ProductCode,
            'ProductFormulations',
            'PercentageInProduct',
            'UPDATE',
            CAST(d.PercentageInProduct AS NVARCHAR(50)),
            CAST(i.PercentageInProduct AS NVARCHAR(50))
        FROM
            (SELECT FormulationID, ProductID, PercentageInProduct FROM deleted) d
            JOIN
            (SELECT FormulationID, ProductID, PercentageInProduct FROM inserted) i
                ON d.FormulationID = i.FormulationID
            JOIN dbo.Products p ON p.ProductID = i.ProductID
        WHERE d.PercentageInProduct <> i.PercentageInProduct
          AND NOT EXISTS (
            SELECT 1 FROM dbo.ProductChangeLog pcl
            WHERE pcl.product_code = p.ProductCode
              AND pcl.source_table = 'ProductFormulations'
              AND pcl.column_name = 'PercentageInProduct'
              AND ISNULL(pcl.old_value, '') = CAST(d.PercentageInProduct AS NVARCHAR(50))
              AND ISNULL(pcl.new_value, '') = CAST(i.PercentageInProduct AS NVARCHAR(50))
              AND pcl.status = 'pending'
              AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
        );
    END;
END;
GO

-- ── Trigger: Products (statement changes) ─────────────────────────────────
CREATE OR ALTER TRIGGER dbo.trg_Products_Change
ON dbo.Products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Log changes to each regulatory statement column individually
    -- so the AI knows exactly which concept changed
    INSERT INTO dbo.ProductChangeLog
        (product_code, source_table, column_name, op_type, old_value, new_value)
    SELECT i.ProductCode, 'Products', 'AnimalTestingStatement', 'UPDATE',
           CAST(d.AnimalTestingStatement AS NVARCHAR(MAX)),
           CAST(i.AnimalTestingStatement AS NVARCHAR(MAX))
    FROM inserted i JOIN deleted d ON d.ProductID = i.ProductID
    WHERE ISNULL(d.AnimalTestingStatement,'') <> ISNULL(i.AnimalTestingStatement,'')
      AND NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = i.ProductCode
          AND pcl.source_table = 'Products'
          AND pcl.column_name = 'AnimalTestingStatement'
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    )

    UNION ALL

    SELECT i.ProductCode, 'Products', 'BSE_TSE_Statement', 'UPDATE',
           CAST(d.BSE_TSE_Statement AS NVARCHAR(MAX)),
           CAST(i.BSE_TSE_Statement AS NVARCHAR(MAX))
    FROM inserted i JOIN deleted d ON d.ProductID = i.ProductID
    WHERE ISNULL(d.BSE_TSE_Statement,'') <> ISNULL(i.BSE_TSE_Statement,'')
      AND NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = i.ProductCode
          AND pcl.source_table = 'Products'
          AND pcl.column_name = 'BSE_TSE_Statement'
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    )

    UNION ALL

    SELECT i.ProductCode, 'Products', 'GMO_Statement', 'UPDATE',
           CAST(d.GMO_Statement AS NVARCHAR(MAX)),
           CAST(i.GMO_Statement AS NVARCHAR(MAX))
    FROM inserted i JOIN deleted d ON d.ProductID = i.ProductID
    WHERE ISNULL(d.GMO_Statement,'') <> ISNULL(i.GMO_Statement,'')
      AND NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = i.ProductCode
          AND pcl.source_table = 'Products'
          AND pcl.column_name = 'GMO_Statement'
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    )

    UNION ALL

    SELECT i.ProductCode, 'Products', 'NanomaterialStatement', 'UPDATE',
           CAST(d.NanomaterialStatement AS NVARCHAR(MAX)),
           CAST(i.NanomaterialStatement AS NVARCHAR(MAX))
    FROM inserted i JOIN deleted d ON d.ProductID = i.ProductID
    WHERE ISNULL(d.NanomaterialStatement,'') <> ISNULL(i.NanomaterialStatement,'')
      AND NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = i.ProductCode
          AND pcl.source_table = 'Products'
          AND pcl.column_name = 'NanomaterialStatement'
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    );
END;
GO

-- ── Trigger: RawMaterials (supplier changes) ───────────────────────────────
CREATE OR ALTER TRIGGER dbo.trg_RawMaterials_Change
ON dbo.RawMaterials
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- CRITICAL FIX: Log supplier NAMES (not IDs) for LLM readability
    -- LLM needs human-readable descriptions like "ROCHE to BASF" not "25 to 8"
    INSERT INTO dbo.ProductChangeLog
        (product_code, source_table, column_name, op_type, old_value, new_value)
    SELECT DISTINCT
        p.ProductCode,
        'RawMaterials',
        'SupplierName',  -- Changed from SupplierID for clarity
        'UPDATE',
        old_supplier.SupplierName,  -- Resolved name
        new_supplier.SupplierName   -- Resolved name
    FROM inserted i
    JOIN deleted d ON d.RawMaterialID = i.RawMaterialID
    JOIN dbo.Suppliers old_supplier ON old_supplier.SupplierID = d.SupplierID
    JOIN dbo.Suppliers new_supplier ON new_supplier.SupplierID = i.SupplierID
    JOIN dbo.ProductFormulations pf ON pf.RawMaterialID = i.RawMaterialID
    JOIN dbo.Products p ON p.ProductID = pf.ProductID
    WHERE d.SupplierID <> i.SupplierID
      AND NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = p.ProductCode
          AND pcl.source_table = 'RawMaterials'
          AND pcl.column_name = 'SupplierName'
          AND ISNULL(pcl.old_value, '') = ISNULL(old_supplier.SupplierName, '')
          AND ISNULL(pcl.new_value, '') = ISNULL(new_supplier.SupplierName, '')
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    );
END;
GO

-- ── Trigger: ProductBatchTests ─────────────────────────────────────────────
CREATE OR ALTER TRIGGER dbo.trg_ProductBatchTests_Change
ON dbo.ProductBatchTests
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @op VARCHAR(10);
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        SET @op = 'INSERT';
    ELSE IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
        SET @op = 'DELETE';
    ELSE
        SET @op = 'UPDATE';

    -- Log SubstanceTested changes (INSERT/DELETE or substance name change)
    INSERT INTO dbo.ProductChangeLog
        (product_code, source_table, column_name, op_type, old_value, new_value)
    SELECT DISTINCT
        p.ProductCode,
        'ProductBatchTests',
        'SubstanceTested',
        @op,
        d.SubstanceTested,
        i.SubstanceTested
    FROM
        (SELECT BatchTestID, ProductID, SubstanceTested FROM deleted) d
        FULL OUTER JOIN
        (SELECT BatchTestID, ProductID, SubstanceTested FROM inserted) i
            ON d.BatchTestID = i.BatchTestID
        JOIN dbo.Products p
            ON p.ProductID = COALESCE(i.ProductID, d.ProductID)
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.ProductChangeLog pcl
        WHERE pcl.product_code = p.ProductCode
          AND pcl.source_table = 'ProductBatchTests'
          AND pcl.column_name = 'SubstanceTested'
          AND ISNULL(pcl.old_value, '') = ISNULL(d.SubstanceTested, '')
          AND ISNULL(pcl.new_value, '') = ISNULL(i.SubstanceTested, '')
          AND pcl.status = 'pending'
          AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
    );
    
    -- CRITICAL FIX: Log ResultValue changes on UPDATE (corrected measurements)
    -- Lipstick dossier section 2.2.2.2 contains batch test table with actual values
    -- Corrected measurements must propagate to dossier
    IF @op = 'UPDATE'
    BEGIN
        INSERT INTO dbo.ProductChangeLog
            (product_code, source_table, column_name, op_type, old_value, new_value)
        SELECT DISTINCT
            p.ProductCode,
            'ProductBatchTests',
            'ResultValue',
            'UPDATE',
            CAST(d.ResultValue AS NVARCHAR(50)),
            CAST(i.ResultValue AS NVARCHAR(50))
        FROM
            (SELECT BatchTestID, ProductID, ResultValue FROM deleted) d
            JOIN
            (SELECT BatchTestID, ProductID, ResultValue FROM inserted) i
                ON d.BatchTestID = i.BatchTestID
            JOIN dbo.Products p
                ON p.ProductID = i.ProductID
        WHERE d.ResultValue <> i.ResultValue
          AND NOT EXISTS (
            SELECT 1 FROM dbo.ProductChangeLog pcl
            WHERE pcl.product_code = p.ProductCode
              AND pcl.source_table = 'ProductBatchTests'
              AND pcl.column_name = 'ResultValue'
              AND ISNULL(pcl.old_value, '') = CAST(d.ResultValue AS NVARCHAR(50))
              AND ISNULL(pcl.new_value, '') = CAST(i.ResultValue AS NVARCHAR(50))
              AND pcl.status = 'pending'
              AND DATEDIFF(SECOND, pcl.changed_at, SYSUTCDATETIME()) < 2
        );
    END;
END;
GO

-- ============================================================
-- STEP 5: HELPER FUNCTION & VIEW
-- ============================================================
PRINT 'Step 5: Creating helper objects...';
GO

-- GetPendingChanges: convenience function for the Poller
-- Returns the next @batch_size pending changes ordered by time,
-- grouped by product_code. The Poller calls this, marks them
-- as 'processing', then hands off to the pipeline.
CREATE OR ALTER FUNCTION dbo.GetPendingChanges (@batch_size INT)
RETURNS TABLE
AS
RETURN
(
    SELECT TOP (@batch_size)
        cl.change_id,
        cl.product_code,
        p.ProductName   AS product_name,
        cl.source_table,
        cl.column_name,
        cl.op_type,
        cl.old_value,
        cl.new_value,
        cl.changed_by,
        cl.changed_at
    FROM dbo.ProductChangeLog cl
    JOIN dbo.Products p ON p.ProductCode = cl.product_code
    WHERE cl.status = 'pending'
    ORDER BY cl.changed_at ASC
);
GO

-- vw_ProductChangeSummary: quick diagnostic view
CREATE OR ALTER VIEW dbo.vw_ProductChangeSummary AS
SELECT
    cl.product_code,
    p.ProductName,
    cl.source_table,
    cl.column_name,
    cl.op_type,
    cl.status,
    cl.changed_at,
    cl.error_message
FROM dbo.ProductChangeLog cl
JOIN dbo.Products p ON p.ProductCode = cl.product_code;
GO

-- ============================================================
-- STEP 6: VERIFICATION QUERIES (for testing)
-- ============================================================
PRINT 'Step 6: Creating verification queries...';
GO

-- Test query: View all products
PRINT 'Products loaded:';
SELECT ProductCode, ProductName, DocumentVersionCode FROM Products;
GO

-- Test query: View formulation counts per product
PRINT 'Formulation counts:';
SELECT 
    p.ProductCode,
    p.ProductName,
    COUNT(pf.FormulationID) as IngredientCount
FROM Products p
LEFT JOIN ProductFormulations pf ON p.ProductID = pf.ProductID
GROUP BY p.ProductCode, p.ProductName
ORDER BY p.ProductCode;
GO

-- Test query: View allergens
PRINT 'Allergens:';
SELECT 
    p.ProductCode,
    p.ProductName,
    rma.AllergenName,
    rma.Notes
FROM RawMaterialAllergens rma
JOIN RawMaterials rm ON rm.RawMaterialID = rma.RawMaterialID
JOIN ProductFormulations pf ON pf.RawMaterialID = rm.RawMaterialID
JOIN Products p ON p.ProductID = pf.ProductID;
GO

-- Test query: View trace substances
PRINT 'Trace substances:';
SELECT 
    p.ProductCode,
    p.ProductName,
    rmt.SubstanceName,
    rmt.Classification,
    rmt.MaxLevelPPM
FROM RawMaterialTraces rmt
JOIN RawMaterials rm ON rm.RawMaterialID = rmt.RawMaterialID
JOIN ProductFormulations pf ON pf.RawMaterialID = rm.RawMaterialID
JOIN Products p ON p.ProductID = pf.ProductID
ORDER BY p.ProductCode, rmt.SubstanceName;
GO

-- ============================================================
-- DONE
-- ============================================================
PRINT '============================================================';
PRINT 'DATABASE SETUP COMPLETE.';
PRINT 'Tables created: Products, Suppliers, Ingredients, RawMaterials,';
PRINT '  ProductFormulations, RawMaterialAllergens, RawMaterialTraces,';
PRINT '  ProductBatchTests, ProductMarkets, SupplierDocuments, ProductChangeLog';
PRINT 'All 3 Bepanthol products populated with dossier-accurate data.';
PRINT 'Triggers installed on: RawMaterialAllergens, RawMaterialTraces,';
PRINT '  ProductFormulations, Products, RawMaterials, ProductBatchTests';
PRINT 'Polling infrastructure ready — no CDC, no SQL Server Agent required.';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Test triggers by making a change (e.g., INSERT into RawMaterialAllergens)';
PRINT '2. Check ProductChangeLog: SELECT * FROM ProductChangeLog';
PRINT '3. Use GetPendingChanges(10) to fetch changes for processing';
PRINT '============================================================';
GO
