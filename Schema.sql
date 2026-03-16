-- ===================================================================
-- BAYER DOSSIER POC - DEFINITIVE "FACTORY RESET" SCRIPT (V3)
-- ===================================================================
-- This script will:
-- 1. Completely reset the environment by dropping all objects.
-- 2. Create the full schema.
-- 3. Populate the schema with all baseline data.
-- 4. Enable CDC on the database and ALL relevant data tables.
-- 5. Create a new, more powerful change detection function.
--
-- After running this, the database will be in a pristine state,
-- ready for the "Grand Unified Scenario" setup.
-- ===================================================================

-- Select the target database


-- Step 1: Preliminary Check
PRINT 'Reminder: Please ensure the "SQL Server Agent" service is running for CDC jobs.';
GO

-- Step 2: Complete Teardown of Existing Objects
PRINT 'Step 2: Performing complete teardown of existing objects...';
-- Disable CDC on the database if it exists, which also drops all related CDC objects.
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 1
BEGIN
    EXEC sys.sp_cdc_disable_db;
END
GO
-- Drop all objects in a specific order to respect dependencies
IF OBJECT_ID('dbo.GetDossierChanges', 'FN') IS NOT NULL DROP FUNCTION dbo.GetDossierChanges;
IF OBJECT_ID('dbo.ProductMarkets', 'U') IS NOT NULL DROP TABLE dbo.ProductMarkets;
IF OBJECT_ID('dbo.ProductBatchTests', 'U') IS NOT NULL DROP TABLE dbo.ProductBatchTests;
IF OBJECT_ID('dbo.RawMaterialTraces', 'U') IS NOT NULL DROP TABLE dbo.RawMaterialTraces;
IF OBJECT_ID('dbo.RawMaterialAllergens', 'U') IS NOT NULL DROP TABLE dbo.RawMaterialAllergens;
IF OBJECT_ID('dbo.ProductFormulations', 'U') IS NOT NULL DROP TABLE dbo.ProductFormulations;
IF OBJECT_ID('dbo.RawMaterials', 'U') IS NOT NULL DROP TABLE dbo.RawMaterials;
IF OBJECT_ID('dbo.Ingredients', 'U') IS NOT NULL DROP TABLE dbo.Ingredients;
IF OBJECT_ID('dbo.Suppliers', 'U') IS NOT NULL DROP TABLE dbo.Suppliers;
IF OBJECT_ID('dbo.Products', 'U') IS NOT NULL DROP TABLE dbo.Products;
GO

-- Step 3: Re-Create Schema
PRINT 'Step 3: Creating the database schema...';
CREATE TABLE Products ( ProductID INT PRIMARY KEY, ProductCode VARCHAR(255) UNIQUE NOT NULL, ProductName VARCHAR(255) NOT NULL, RegQualCode VARCHAR(255), DocumentVersionCode VARCHAR(255), AnimalTestingStatement TEXT, BSE_TSE_Statement TEXT, GMO_Statement TEXT, NanomaterialStatement TEXT, LastUpdate DATETIME2(0) );
CREATE TABLE Suppliers ( SupplierID INT PRIMARY KEY, SupplierName VARCHAR(255) UNIQUE NOT NULL );
CREATE TABLE Ingredients ( IngredientID INT PRIMARY KEY, INCI_Name VARCHAR(255) UNIQUE NOT NULL, Description TEXT );
CREATE TABLE RawMaterials ( RawMaterialID INT PRIMARY KEY, IngredientID INT NOT NULL, SupplierID INT NOT NULL, CommercialName VARCHAR(255) NOT NULL, NaturalOriginIndex DECIMAL(3, 2) NOT NULL );
CREATE TABLE ProductFormulations ( FormulationID INT PRIMARY KEY IDENTITY(1,1), ProductID INT NOT NULL, RawMaterialID INT NOT NULL, PercentageInProduct DECIMAL(8, 5) NOT NULL );
CREATE TABLE RawMaterialAllergens ( AllergenID INT PRIMARY KEY, RawMaterialID INT NOT NULL, AllergenName VARCHAR(255) NOT NULL, Notes TEXT );
CREATE TABLE RawMaterialTraces ( TraceID INT PRIMARY KEY, RawMaterialID INT NOT NULL, SubstanceName VARCHAR(255) NOT NULL, Classification VARCHAR(255), MaxLevelPPM DECIMAL(10, 4) NOT NULL );
CREATE TABLE ProductBatchTests ( BatchTestID INT PRIMARY KEY, ProductID INT NOT NULL, BatchNumber VARCHAR(255) NOT NULL, ManufactureDate DATE NOT NULL, SubstanceTested VARCHAR(255) NOT NULL, ResultOperator VARCHAR(5), ResultValue DECIMAL(10, 4) NOT NULL, Unit VARCHAR(10) NOT NULL );
CREATE TABLE ProductMarkets ( MarketID INT PRIMARY KEY IDENTITY(1,1), ProductID INT NOT NULL, Market VARCHAR(50) NOT NULL );

-- Create a new table to link Raw Materials to their specific compliance documents
CREATE TABLE SupplierDocuments (
    DocumentID VARCHAR(255) PRIMARY KEY,       -- The unique ID from the supplier's PDF
    RawMaterialID INT NOT NULL,               -- Foreign key to the RawMaterials table
    DocumentType VARCHAR(100) NOT NULL,       -- e.g., 'Allergen', 'CMR', 'Natural Origin'
    FilePath VARCHAR(512) NOT NULL,           -- The relative path to the physical file
    Version VARCHAR(50),
    IssueDate DATE,
    CONSTRAINT FK_SupplierDocuments_RawMaterials FOREIGN KEY (RawMaterialID) REFERENCES RawMaterials(RawMaterialID)
);
GO

-- Step 4: Populate All Baseline Data
PRINT 'Step 4: Populating baseline data...';
-- All INSERT statements from your script...
INSERT INTO Suppliers (SupplierID, SupplierName) VALUES (1,'IOI OLEO'),(2,'KLK OLEO'),(3,'GATTEFOSSE'),(4,'SYMRISE'),(5,'DSM'),(6,'HENRY LAMOTTE'),(7,'DSM/LONZA'),(8,'BASF'),(9,'CRODA'),(10,'APRINNOVA'),(11,'SEPPIC'),(12,'ALDIVIA'),(13,'CP KELCO'),(14,'CITRIQUE BELGE'),(15,'IMCD'),(16,'Saci-cfpa'),(17,'Merck'),(18,'Nippon fine chemical'),(19,'Rossow'),(20,'Koster keunen'),(21,'A2PH'),(22,'BTSA'),(23,'Evonik'),(24,'Nikko chemicals'),(25,'ROCHE'),(26,'COGNIS'),(27,'HENKEL'),(28,'WAGNER'),(29,'TRANSOL'),(30,'-');
INSERT INTO Ingredients (IngredientID, INCI_Name) VALUES (1,'AQUA'),(2,'CAPRYLIC/CAPRIC TRIGLYCERIDE'),(3,'GLYCERIN'),(4,'POLYGLYCERYL-6 DISTEARATE, JOJOBA ESTERS, POLYGLYCERYL-3 BEESWAX, CETYL ALCOHOL'),(5,'1,2-HEXANEDIOL'),(6,'PANTHENOL'),(7,'BUTYROSPERMUM PARKII BUTTER'),(8,'NIACINAMIDE'),(9,'CETEARYL ALCOHOL'),(10,'ISOPROPYL ISOSTEARATE'),(11,'SQUALANE'),(12,'TOCOPHERYL ACETATE'),(13,'GLYCERYL STEARATE CITRATE'),(14,'BEHENYL ALCOHOL'),(15,'ACACIA SENEGAL GUM, XANTHAN GUM'),(16,'ARGANIA SPINOSA KERNEL OIL'),(17,'XANTHAN GUM'),(18,'CITRIC ACID'),(19,'Ricinus communis seed oil'),(20,'Tribehenin'),(21,'Diethylamino hydroxybenzoyl hexyl benzoate'),(22,'Isoamyl p-methoxycinnamate'),(23,'Oryza sativa bran wax'),(24,'C10-18 triglycerides'),(25,'CI 77163'),(26,'Bis-ethylhexyloxyphenol methoxyphenyl triazine'),(27,'Ethylhexyl triazone'),(28,'Bis-behenyl/Isostearyl/Phytosteryl dimer dilinoleyl dimer dilinoleate'),(29,'Microcrystalline cellulose'),(30,'Cera alba'),(31,'Parfum'),(32,'Helianthus annuus seed oil, Tocopherol, Beta-sitosterol, Squalene'),(33,'Ascorbyl tetraisopalmitate'),(34,'Tocopherol'),(35,'ISOPROPYL MYRISTATE'),(36,'CETYL ALCOHOL'),(37,'STEARYL ALCOHOL'),(38,'PROPYLENE GLYCOL'),(39,'LANOLIN'),(40,'POTASSIUM CETYL PHOSPHATE'),(41,'PHENOXYETHANOL'),(42,'PANTOLACTONE');
INSERT INTO Products (ProductID, ProductCode, ProductName, RegQualCode, DocumentVersionCode, AnimalTestingStatement, BSE_TSE_Statement, GMO_Statement, NanomaterialStatement, LastUpdate) VALUES (1, '1614322', 'BEPANTHOL Face Day Cream', 'VV-REGQUAL-108834', 'C.2.2-03', 'No animal testing...', 'Raw materials do not contain...', 'Raw materials do not contain...', 'No nanomaterial...', '2022-03-24');
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex) VALUES (1,1,30,'PURIFIED WATER',1.00),(2,2,1,'MIGLYOL 812 N',1.00),(3,3,2,'PALMERA G995 E',1.00),(4,4,3,'EMULIUM MELLIFERA MB',1.00),(5,5,4,'HYDROLITE 6',0.00),(6,6,5,'D-PANTHENOL',0.00),(7,7,6,'SHEA BUTTER, REFINED',1.00),(8,8,7,'NIACINAMIDE',0.00),(9,9,8,'KOLLIWAX CSA50',1.00),(10,10,9,'CRODAMOL IPIS-LQ-(MV)',0.85),(11,11,10,'NEOSSANCE SQUALANE',1.00),(12,12,5,'DL ALPHA TOCOPHERYL ACETATE',0.00),(13,13,1,'IMWITOR 372P',1.00),(14,14,8,'LANETTE 22',1.00),(15,15,11,'SOLAGUM AX',1.00),(16,16,12,'ARGAN OIL DEODORIZED ORGANIC',1.00),(17,17,13,'KELTROL CG T',1.00),(18,18,14,'CITRIC ACID ANHYDROUS FINE GRANULAR 16/40',1.00);
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct) VALUES (1,1,67.54000),(1,2,7.50000),(1,3,5.00000),(1,4,3.00000),(1,5,2.75000),(1,6,2.50000),(1,7,2.50000),(1,8,2.00000),(1,9,2.00000),(1,10,2.00000),(1,11,1.00000),(1,12,0.50000),(1,13,0.50000),(1,14,0.50000),(1,15,0.40000),(1,16,0.20000),(1,17,0.10000),(1,18,0.01250);
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM) VALUES (1, 12, 'Toluene', 'CMR 2', 80.0),(2, 6, 'Dichloromethane', 'CMR 2', 50.0);
INSERT INTO Products (ProductID, ProductCode, ProductName, RegQualCode, DocumentVersionCode, AnimalTestingStatement, BSE_TSE_Statement, GMO_Statement, NanomaterialStatement, LastUpdate) VALUES (2, '1614557', 'BEPANTHOL Lipstick', 'VV-REGQUAL-206035', 'C.2.2-02', 'No animal testing...', 'Raw materials do not present...', 'Raw materials do not contain...', 'No nanomaterial...', '2024-05-29');
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex) VALUES (19,2,8,'Myritol 318 MB',1.00),(20,19,15,'Castor oil refined',1.00),(21,20,9,'Syncrowax HRC-PA-(RB)',1.00),(22,21,8,'Uvinul A plus granular',0.00),(23,22,4,'Neoheliopan E1000',0.00),(24,23,16,'Rice wax n 1',1.00),(25,24,3,'Lipocire A SG',1.00),(26,25,17,'Ronaflair LF-2000',1.00),(27,26,8,'Tinosorb S',0.00),(28,12,8,'Acetate de vitamine E care',0.00),(29,27,8,'Uvinul T 150',0.00),(30,28,18,'Plandool G',1.00),(31,6,5,'D-panthenol',0.00),(32,29,19,'Sensocel 8',1.00),(33,30,20,'Beeswax white',1.00),(34,31,21,'Parfum vanille SA26 0346P01.01',0.00),(35,32,22,'Tocobiol SFC',1.00),(36,13,23,'Dermofeel GSC SG',1.00),(37,33,24,'Nikkol VC-IPVS',1.00);
INSERT INTO ProductFormulations (ProductID, RawMaterialID, PercentageInProduct) VALUES (2,19,25.25),(2,20,14.7),(2,21,14.5),(2,22,9.0),(2,23,8.3),(2,24,5.75),(2,25,5.0),(2,26,5.0),(2,27,3.7),(2,28,2.2),(2,29,2.2),(2,30,1.5),(2,31,1.15),(2,32,1.0),(2,33,0.4),(2,34,0.3),(2,35,0.034),(2,36,0.011),(2,37,0.005);
INSERT INTO RawMaterialAllergens (AllergenID, RawMaterialID, AllergenName, Notes) VALUES (1, 34, 'Vanillin', 'May be found in the perfume.');
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM) VALUES (3,22,'Toluene','CMR 2',100.0),(4,29,'Toluene','CMR 2',30.0),(5,22,'Dihexylphthalate','CMR 1B',500.0),(6,27,'N,N-dimethylformamide','CMR 1B',10.0),(7,37,'N,N-dimethylformamide','CMR 1B',50.0),(8,31,'Dichloromethane','CMR 2',50.0),(9,35,'Hexane','CMR 2',1.0),(10,19,'Benzo(a)pyrene','CMR 1B',0.002),(11,29,'Nitrosamines',NULL,0.05),(12,29,'P-aminobenzoic acid',NULL,2000.0),(13,36,'Diethylene glycol (DEG)',NULL,1000.0);
INSERT INTO Products (ProductID, ProductCode, ProductName, RegQualCode, DocumentVersionCode, AnimalTestingStatement, BSE_TSE_Statement, GMO_Statement, NanomaterialStatement, LastUpdate) VALUES (3, '1600188', 'BEPANTHOL Cream', 'VV-REGQUAL-191544', 'C.2.2-01', 'No animal testing...', 'Raw materials do not present...', 'Raw materials do not contain...', 'No nanomaterial...', '2022-09-01');
INSERT INTO RawMaterials (RawMaterialID, IngredientID, SupplierID, CommercialName, NaturalOriginIndex) VALUES (38,1,30,'PURIFIED WATER',1.00),(39,6,25,'D-PANTHENOL',0.00),(40,35,17,'ISOPROPYL MYRISTATE PH. EUR.',0.50),(41,36,26,'CETYL ALCOHOL',1.00),(42,37,27,'LANETTE 18 DEO',1.00),(43,38,17,'1,2 PROPANDIOL',0.00),(44,39,28,'EWALAN 20',1.00),(45,40,25,'AMPHISOL K',0.50),(46,41,29,'PHENOXYETHANOL R, R+',0.00),(47,42,25,'DL-LACTON REIN',0.00);
INSERT INTO RawMaterialTraces (TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM) VALUES (14,39,'dichloromethane','CMR 2',50.0),(15,46,'ethylene glycol','CMR 1B',20.0),(16,46,'ethylene oxide','CMR 1B',1.0),(17,46,'1,4-dioxane','CMR 1B',2.0);
GO

-- Step 5: Enable Change Data Capture (CDC) on ALL Tables
PRINT 'Step 5: Enabling Change Data Capture on the database and ALL tables...';
EXEC sys.sp_cdc_enable_db;
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'Products', @role_name = NULL, @supports_net_changes = 1;
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'ProductFormulations', @role_name = NULL, @supports_net_changes = 1;
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'RawMaterialTraces', @role_name = NULL, @supports_net_changes = 1;
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'RawMaterialAllergens', @role_name = NULL, @supports_net_changes = 1;
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'ProductMarkets', @role_name = NULL, @supports_net_changes = 1;
GO

-- Step 6: Create the Definitive, More Powerful "Change Detection" Function
PRINT 'Step 6: Creating the definitive wrapper function for the AI system...';
GO
CREATE OR ALTER FUNCTION dbo.GetDossierChanges (@since_datetime DATETIME2(0))
RETURNS TABLE
AS
RETURN
(
    WITH RawChanges AS (
        -- Capture all raw INSERT, UPDATE, DELETE operations from CDC tables
        SELECT __$start_lsn AS lsn, __$operation AS op, 'ProductFormulations' AS TableName, ProductID, RawMaterialID, NULL AS SubstanceName FROM cdc.dbo_ProductFormulations_CT WHERE __$start_lsn > sys.fn_cdc_map_time_to_lsn('smallest greater than', @since_datetime)
        UNION ALL
        SELECT __$start_lsn, __$operation, 'RawMaterialTraces', (SELECT TOP 1 pf.ProductID FROM ProductFormulations pf WHERE pf.RawMaterialID = ct.RawMaterialID), RawMaterialID, SubstanceName FROM cdc.dbo_RawMaterialTraces_CT ct WHERE __$start_lsn > sys.fn_cdc_map_time_to_lsn('smallest greater than', @since_datetime)
        -- ... (add other tables here if needed: RawMaterialAllergens, ProductMarkets, etc.)
    ),
    -- Correlate deletes and inserts that happen in the same transaction to identify a "switch"
    Switches AS (
        SELECT
            dels.lsn,
            'SUPPLIER_SWITCH' AS ChangeType,
            'ProductFormulations' AS TableName,
            dels.ProductID,
            ins.RawMaterialID AS NewRawMaterialID, -- The new material being inserted
            dels.RawMaterialID AS OldRawMaterialID  -- The old material being deleted
        FROM RawChanges dels
        JOIN RawChanges ins ON dels.ProductID = ins.ProductID AND dels.lsn = ins.lsn -- Must be in the same transaction (same LSN)
        WHERE dels.op = 1 AND ins.op = 2 -- A DELETE followed by an INSERT
    )
    -- Final SELECT to build the clean change log for the AI
    SELECT
        COALESCE(s.ChangeType, CASE rc.op WHEN 1 THEN 'DELETE' WHEN 2 THEN 'INSERT' WHEN 4 THEN 'UPDATE' END) AS ChangeType,
        rc.TableName,
        p.ProductID,
        p.ProductName,
        p.ProductCode,
        (
            SELECT
                rc.RawMaterialID,
                rm.CommercialName,
                ing.INCI_Name,
                s.OldRawMaterialID, -- Include for context
                'Supplier was switched in the formulation.' AS Notes
            FROM RawMaterials rm JOIN Ingredients ing ON rm.IngredientID = ing.IngredientID
            WHERE rm.RawMaterialID = rc.RawMaterialID
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ) AS ChangeDetails,
        sys.fn_cdc_map_lsn_to_time(rc.lsn) as ChangeTimestamp
    FROM RawChanges rc
    JOIN Products p ON rc.ProductID = p.ProductID
    LEFT JOIN Switches s ON rc.lsn = s.lsn AND rc.op = 2 -- Join the switch info onto the INSERT operation
    -- Exclude the raw DELETE part of a switch, as it's now handled by the SUPPLIER_SWITCH event
    WHERE NOT (rc.op = 1 AND EXISTS (SELECT 1 FROM Switches s_inner WHERE s_inner.lsn = rc.lsn))
);
GO

PRINT '===================================================================';
PRINT 'DATABASE FACTORY RESET & FULL INSTRUMENTATION IS COMPLETE.';
PRINT 'The database is now in a pristine, baseline state, ready for the POC.';
PRINT '===================================================================';
GO