-- ============================================================================
-- IMCUST - MANUAL CLEANUP SCRIPT
-- Description: Remove share and cleanup after migration complete
-- WARNING: Only run after IMSDLC confirms successful migration
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;

-- ----------------------------------------------------------------------------
-- PRE-CLEANUP VERIFICATION
-- ----------------------------------------------------------------------------

-- Verify share exists
SHOW SHARES LIKE 'MIGRATION_SHARE_IMCUST_TO_IMSDLC';

-- Check share grants
SHOW GRANTS TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

SHOW GRANTS OF SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

-- ----------------------------------------------------------------------------
-- CLEANUP STEP 1: Remove IMSDLC Account from Share
-- WARNING: Uncomment only after IMSDLC confirms migration complete
-- ----------------------------------------------------------------------------

-- ALTER SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC
--     REMOVE ACCOUNTS = nfmyizv.imsdlc;

-- ----------------------------------------------------------------------------
-- CLEANUP STEP 2: Drop Share
-- WARNING: Uncomment only after removing accounts
-- ----------------------------------------------------------------------------

-- DROP SHARE IF EXISTS MIGRATION_SHARE_IMCUST_TO_IMSDLC;

-- ----------------------------------------------------------------------------
-- POST-CLEANUP VERIFICATION
-- ----------------------------------------------------------------------------

-- Verify share is dropped
-- SHOW SHARES LIKE 'MIGRATION_SHARE_IMCUST_TO_IMSDLC';
-- Should return 0 rows

-- Verify source data still intact
SELECT 'STOCK_METADATA_RAW' AS table_name, COUNT(*) AS row_count
FROM PROD_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
UNION ALL
SELECT 'DIM_STOCKS', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
UNION ALL
SELECT 'DIM_PORTFOLIOS', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
UNION ALL
SELECT 'FACT_TRANSACTIONS', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
UNION ALL
SELECT 'FACT_DAILY_POSITIONS', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;
