-- ============================================
-- TEST: Standalone Object with No Dependencies
-- ============================================
-- Purpose: Verify that objects with no upstream dependencies are still included in migration

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

-- Step 1: Create a standalone table with no dependencies for testing
CREATE OR REPLACE TABLE STANDALONE_TEST_TABLE (
    id NUMBER,
    name VARCHAR(100),
    created_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert some test data
INSERT INTO STANDALONE_TEST_TABLE (id, name) VALUES
(1, 'Test Record 1'),
(2, 'Test Record 2'),
(3, 'Test Record 3');

-- Step 2: Run migration for this standalone object
CALL sp_orchestrate_migration(
    'PROD_DB',
    'MART_INVESTMENTS_BOLT',
    'DEV_DB',
    'MART_INVESTMENTS_BOLT',
    ARRAY_CONSTRUCT('STANDALONE_TEST_TABLE'),
    'MIGRATION_SHARE_STANDALONE',
    'IMSDLC'
);

-- Step 3: Verify the object was captured
SELECT
    'migration_share_objects' as table_name,
    COUNT(*) as record_count,
    LISTAGG(object_name, ', ') as objects
FROM migration_share_objects
WHERE object_name = 'STANDALONE_TEST_TABLE'
GROUP BY 1

UNION ALL

SELECT
    'migration_ddl_scripts' as table_name,
    COUNT(*) as record_count,
    LISTAGG(object_name, ', ') as objects
FROM migration_ddl_scripts
WHERE object_name = 'STANDALONE_TEST_TABLE'
GROUP BY 1

UNION ALL

SELECT
    'migration_ctas_scripts' as table_name,
    COUNT(*) as record_count,
    LISTAGG(object_name, ', ') as objects
FROM migration_ctas_scripts
WHERE object_name = 'STANDALONE_TEST_TABLE'
GROUP BY 1;

-- Step 4: Check the share was created with the object
SHOW GRANTS TO SHARE MIGRATION_SHARE_STANDALONE;

-- Step 5: View detailed results
SELECT
    object_name,
    object_type,
    dependency_level,
    fully_qualified_name
FROM migration_share_objects
WHERE object_name = 'STANDALONE_TEST_TABLE';

-- Expected Results:
-- ✅ migration_share_objects should have 1 record for STANDALONE_TEST_TABLE with level=0
-- ✅ migration_ddl_scripts should have 1 record with the DDL
-- ✅ migration_ctas_scripts should have 1 record with the CTAS script
-- ✅ Share should have grants for STANDALONE_TEST_TABLE
