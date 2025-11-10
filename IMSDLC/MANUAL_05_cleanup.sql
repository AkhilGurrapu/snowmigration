-- ============================================================================
-- IMSDLC - MANUAL CLEANUP SCRIPT
-- Description: Cleanup temporary shared database after validation passes
-- WARNING: Only run after all validations pass
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;

-- ----------------------------------------------------------------------------
-- PRE-CLEANUP VERIFICATION
-- ----------------------------------------------------------------------------

-- Verify migration completed successfully
SELECT
    'PRE-CLEANUP CHECK' AS check_type,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.TABLES
     WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
       AND table_type = 'BASE TABLE') AS tables_in_dev_db,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.VIEWS
     WHERE table_schema = 'MART_INVESTMENTS_BOLT') AS views_in_dev_db,
    (SELECT SUM(row_count) FROM DEV_DB.INFORMATION_SCHEMA.TABLES
     WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
       AND table_type = 'BASE TABLE') AS total_rows_in_dev_db;

-- Verify shared database exists
SHOW DATABASES LIKE 'MIGRATION_SHARED_DB';

-- ----------------------------------------------------------------------------
-- CLEANUP: Drop Temporary Shared Database
-- WARNING: Uncomment only after confirming all validations pass
-- ----------------------------------------------------------------------------

-- DROP DATABASE IF EXISTS MIGRATION_SHARED_DB;

-- ----------------------------------------------------------------------------
-- POST-CLEANUP VERIFICATION
-- ----------------------------------------------------------------------------

-- Verify shared database is dropped
-- SHOW DATABASES LIKE 'MIGRATION_SHARED_DB';
-- Should return 0 rows

-- Verify DEV_DB objects still exist
SELECT
    table_schema,
    table_type,
    COUNT(*) AS object_count
FROM DEV_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
GROUP BY table_schema, table_type
ORDER BY table_schema, table_type;

-- Verify data still accessible
SELECT 'STOCK_METADATA_RAW' AS table_name, COUNT(*) AS row_count
FROM DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
UNION ALL
SELECT 'DIM_STOCKS', COUNT(*) FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
UNION ALL
SELECT 'DIM_PORTFOLIOS', COUNT(*) FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
UNION ALL
SELECT 'FACT_TRANSACTIONS', COUNT(*) FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
UNION ALL
SELECT 'FACT_DAILY_POSITIONS', COUNT(*) FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;
