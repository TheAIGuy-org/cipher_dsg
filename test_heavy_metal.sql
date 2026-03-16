-- Test: Add Lead heavy metal to Face Day Cream (Product 1614322)
-- This tests if the system creates a new section or updates existing trace substances section

-- Correct schema for RawMaterialTraces:
-- TraceID, RawMaterialID, SubstanceName, Classification, MaxLevelPPM

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

-- This should trigger:
-- 1. ProductChangeLog entry via RawMaterialTraces trigger
-- 2. Agent detects change in next poll cycle (within 30s)
-- 3. Phase 3: Extracts "heavy metal trace substance" concept
-- 4. Phase 4: LLM maps to section 2.2.2.2 (Traces of regulated substances)
-- 5. Phase 5: Generates update with Lead information
-- 6. Phase 7: Stores new version in Neo4j

-- Expected AI behavior:
-- - Should update existing section 2.2.2.2 (not create new section)
-- - Should add Lead to the list of trace substances
-- - Should mention 10 ppm level and regulatory monitoring
