-- ============================================================================
-- SNOWFLAKE CROSS-ACCOUNT MIGRATION: EXECUTION GUIDE
-- Source: IMCUST (PROD_DB) → Target: IMSDLC (DEV_DB)
-- ============================================================================

-- ============================================================================
-- PART 1: MANUAL EXECUTION (Step-by-Step)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- MANUAL STEP 1: Discovery (IMCUST)
-- ----------------------------------------------------------------------------
-- Execute in IMCUST account
-- File: IMCUST/MANUAL_01_discovery.sql
-- Purpose: Discover all objects and dependencies
-- Expected Time: 15-30 minutes
-- Review Results: Identify all objects to migrate

-- ----------------------------------------------------------------------------
-- MANUAL STEP 2: Extract DDL (IMCUST)
-- ----------------------------------------------------------------------------
-- Execute in IMCUST account
-- File: IMCUST/MANUAL_02_extract_ddl.sql
-- Purpose: Extract DDL for all discovered objects
-- Expected Time: 10-20 minutes
-- Action Required: Copy DDL output for transformation

-- ----------------------------------------------------------------------------
-- MANUAL STEP 3: Create Share (IMCUST)
-- ----------------------------------------------------------------------------
-- Execute in IMCUST account
-- File: IMCUST/MANUAL_03_create_share.sql
-- Purpose: Create data share and grant access to IMSDLC
-- Expected Time: 5-10 minutes
-- Verify: Share created and IMSDLC added

-- ----------------------------------------------------------------------------
-- MANUAL STEP 4: Consume Share (IMSDLC)
-- ----------------------------------------------------------------------------
-- Execute in IMSDLC account
-- File: IMSDLC/MANUAL_01_consume_share.sql
-- Purpose: Consume share and verify data access
-- Expected Time: 5-10 minutes
-- Verify: Can query shared tables

-- ----------------------------------------------------------------------------
-- MANUAL STEP 5: Transform DDL
-- ----------------------------------------------------------------------------
-- Manual Action Required:
-- 1. Copy DDL from STEP 2 output
-- 2. Replace all instances: PROD_DB → DEV_DB
-- 3. Save transformed DDL for next step
-- Expected Time: 15-30 minutes

-- ----------------------------------------------------------------------------
-- MANUAL STEP 6: Create Objects (IMSDLC)
-- ----------------------------------------------------------------------------
-- Execute in IMSDLC account
-- File: IMSDLC/MANUAL_02_create_objects.sql
-- Purpose: Create tables, views, procedures
-- Action Required: Paste transformed DDL into script
-- Expected Time: 20-30 minutes

-- ----------------------------------------------------------------------------
-- MANUAL STEP 7: Populate Data (IMSDLC)
-- ----------------------------------------------------------------------------
-- Execute in IMSDLC account
-- File: IMSDLC/MANUAL_03_populate_data.sql
-- Purpose: Migrate data from shared tables
-- Expected Time: 1-4 hours (depends on data volume)
-- Monitor: Warehouse usage and credits

-- ----------------------------------------------------------------------------
-- MANUAL STEP 8: Validate (IMSDLC)
-- ----------------------------------------------------------------------------
-- Execute in IMSDLC account
-- File: IMSDLC/MANUAL_04_validate.sql
-- Purpose: Comprehensive validation
-- Expected Time: 30-45 minutes
-- Critical: All validations must PASS before cleanup

-- ----------------------------------------------------------------------------
-- MANUAL STEP 9: Cleanup (IMSDLC then IMCUST)
-- ----------------------------------------------------------------------------
-- Execute in IMSDLC: IMSDLC/MANUAL_05_cleanup.sql
-- Execute in IMCUST: IMCUST/MANUAL_04_cleanup.sql
-- Purpose: Remove temporary shared database and share
-- WARNING: Only after all validations pass
-- Expected Time: 5-10 minutes

-- ============================================================================
-- PART 2: AUTOMATED EXECUTION (Using Stored Procedures)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- AUTOMATED SETUP: Install Procedures
-- ----------------------------------------------------------------------------

-- In IMCUST: Execute IMCUST/AUTOMATED_migration_procedure.sql
-- Creates:
--   - SP_PREPARE_MIGRATION_SHARE
--   - SP_EXTRACT_ALL_DDL
--   - SP_DISCOVER_DEPENDENCIES

-- In IMSDLC: Execute IMSDLC/AUTOMATED_migration_procedure.sql
-- Creates:
--   - SP_TRANSFORM_DDL
--   - SP_CREATE_TABLES_FROM_SHARE
--   - SP_POPULATE_DATA_FROM_SHARE
--   - SP_VALIDATE_MIGRATION
--   - SP_COMPLETE_MIGRATION

-- ----------------------------------------------------------------------------
-- AUTOMATED STEP 1: Discover Dependencies (IMCUST)
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;

CALL PROD_DB.PUBLIC.SP_DISCOVER_DEPENDENCIES(
    'PROD_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS,VW_CURRENT_HOLDINGS,SP_LOAD_DIM_STOCKS,SP_CALCULATE_DAILY_POSITIONS',
    10  -- Max recursion depth
);

-- Review results to identify all dependencies

-- ----------------------------------------------------------------------------
-- AUTOMATED STEP 2: Extract DDL (IMCUST)
-- ----------------------------------------------------------------------------

CALL PROD_DB.PUBLIC.SP_EXTRACT_ALL_DDL(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'TABLE,VIEW,PROCEDURE'
);

-- Save results for transformation

-- ----------------------------------------------------------------------------
-- AUTOMATED STEP 3: Prepare Migration Share (IMCUST)
-- ----------------------------------------------------------------------------

CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS',
    'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'nfmyizv.imsdlc'
);

-- Verify: Returns success message with table count

-- ----------------------------------------------------------------------------
-- AUTOMATED STEP 4: Complete Migration (IMSDLC)
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;

-- Option A: Complete automated migration (tables only)
CALL DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- Option B: Step-by-step automated migration

-- B1: Create table structures only
CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE  -- FALSE = structure only, TRUE = structure + data
);

-- B2: Populate data separately
CALL DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE  -- FALSE = don't truncate, TRUE = truncate before load
);

-- ----------------------------------------------------------------------------
-- AUTOMATED STEP 5: Validate Migration (IMSDLC)
-- ----------------------------------------------------------------------------

CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- Review results: All should show status = 'PASS'

-- ----------------------------------------------------------------------------
-- AUTOMATED STEP 6: Views and Procedures (MANUAL)
-- ----------------------------------------------------------------------------

-- NOTE: Views and procedures require manual transformation
-- Reason: Complex DDL references need careful transformation

-- 1. Get DDL from IMCUST:
CALL PROD_DB.PUBLIC.SP_EXTRACT_ALL_DDL(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'VIEW,PROCEDURE'
);

-- 2. For each view/procedure DDL, transform:
CALL DEV_DB.PUBLIC.SP_TRANSFORM_DDL(
    '<paste_original_ddl_here>',
    'PROD_DB',
    'DEV_DB'
);

-- 3. Execute transformed DDL manually in IMSDLC

-- ============================================================================
-- PART 3: QUICK START (Automated - Tables Only)
-- ============================================================================

-- IMCUST:
CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS',
    'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'nfmyizv.imsdlc'
);

-- IMSDLC:
CALL DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- Validate:
CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- ============================================================================
-- PART 4: VERIFICATION QUERIES
-- ============================================================================

-- Check share exists (IMCUST)
SHOW SHARES LIKE 'MIGRATION_SHARE_IMCUST_TO_IMSDLC';

-- Check shared database created (IMSDLC)
SHOW DATABASES LIKE 'MIGRATION_SHARED_DB';

-- Check objects created (IMSDLC)
SELECT table_schema, table_type, COUNT(*)
FROM DEV_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
GROUP BY table_schema, table_type;

-- Check row counts match
SELECT
    'STOCK_METADATA_RAW' AS table_name,
    (SELECT COUNT(*) FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW) AS source,
    (SELECT COUNT(*) FROM DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW) AS target,
    CASE WHEN source = target THEN 'PASS' ELSE 'FAIL' END AS status;

-- ============================================================================
-- PART 5: TROUBLESHOOTING
-- ============================================================================

-- Issue: Share not visible in IMSDLC
-- Solution: Verify account format (org.account)
SHOW PARAMETERS LIKE 'ORGANIZATION_NAME';  -- Run in both accounts
SHOW PARAMETERS LIKE 'ACCOUNT_NAME';       -- Run in both accounts

-- Issue: OBJECT_DEPENDENCIES returns empty
-- Solution: Wait 3 hours for ACCOUNT_USAGE latency
SELECT CURRENT_TIMESTAMP() AS now,
       DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS three_hours_ago;

-- Issue: Row count mismatch
-- Solution: Check for filters or errors during INSERT
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%INSERT INTO DEV_DB%'
  AND execution_status != 'SUCCESS'
ORDER BY start_time DESC;

-- Issue: Procedure fails with permission error
-- Solution: Verify ACCOUNTADMIN role
SHOW GRANTS TO USER svc4snowflakedeploy;

-- ============================================================================
-- PART 6: CLEANUP
-- ============================================================================

-- After migration validated (IMSDLC):
DROP DATABASE IF EXISTS MIGRATION_SHARED_DB;

-- After IMSDLC confirms success (IMCUST):
ALTER SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC
    REMOVE ACCOUNTS = nfmyizv.imsdlc;

DROP SHARE IF EXISTS MIGRATION_SHARE_IMCUST_TO_IMSDLC;

-- ============================================================================
-- PART 7: REUSABLE TEMPLATE FOR FUTURE MIGRATIONS
-- ============================================================================

-- IMCUST: Prepare any migration
CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    '<schemas_comma_separated>',
    '<objects_comma_separated>',
    '<share_name>',
    '<org.target_account>'
);

-- IMSDLC: Execute migration
CALL DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION(
    '<shared_database_name>',
    '<target_database_name>',
    '<schemas_comma_separated>'
);

-- Validate
CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    '<shared_database_name>',
    '<target_database_name>',
    '<schemas_comma_separated>'
);
