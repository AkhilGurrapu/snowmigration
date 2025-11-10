-- ============================================================================
-- IMSDLC - MANUAL VALIDATION SCRIPT
-- Description: Comprehensive validation of migrated objects and data
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;

-- ----------------------------------------------------------------------------
-- VALIDATION 1: Object Count Verification
-- ----------------------------------------------------------------------------

SELECT
    table_schema,
    table_type AS object_type,
    COUNT(*) AS object_count
FROM DEV_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
GROUP BY table_schema, table_type
ORDER BY table_schema, table_type;

-- ----------------------------------------------------------------------------
-- VALIDATION 2: Row Count Match (Critical)
-- ----------------------------------------------------------------------------

WITH source_counts AS (
    SELECT 'STOCK_METADATA_RAW' AS table_name, COUNT(*) AS count
    FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
    UNION ALL
    SELECT 'DIM_STOCKS', COUNT(*)
    FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
    UNION ALL
    SELECT 'DIM_PORTFOLIOS', COUNT(*)
    FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
    UNION ALL
    SELECT 'FACT_TRANSACTIONS', COUNT(*)
    FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
    UNION ALL
    SELECT 'FACT_DAILY_POSITIONS', COUNT(*)
    FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS
),
target_counts AS (
    SELECT 'STOCK_METADATA_RAW' AS table_name, COUNT(*) AS count
    FROM DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
    UNION ALL
    SELECT 'DIM_STOCKS', COUNT(*)
    FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
    UNION ALL
    SELECT 'DIM_PORTFOLIOS', COUNT(*)
    FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
    UNION ALL
    SELECT 'FACT_TRANSACTIONS', COUNT(*)
    FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
    UNION ALL
    SELECT 'FACT_DAILY_POSITIONS', COUNT(*)
    FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS
)
SELECT
    s.table_name,
    s.count AS source_count,
    t.count AS target_count,
    CASE
        WHEN s.count = t.count THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM source_counts s
JOIN target_counts t ON s.table_name = t.table_name
ORDER BY s.table_name;

-- ----------------------------------------------------------------------------
-- VALIDATION 3: Column Definition Match
-- ----------------------------------------------------------------------------

WITH source_columns AS (
    SELECT
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        data_type,
        is_nullable,
        character_maximum_length,
        numeric_precision,
        numeric_scale
    FROM MIGRATION_SHARED_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
),
target_columns AS (
    SELECT
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        data_type,
        is_nullable,
        character_maximum_length,
        numeric_precision,
        numeric_scale
    FROM DEV_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
)
SELECT
    COALESCE(s.table_schema, t.table_schema) AS table_schema,
    COALESCE(s.table_name, t.table_name) AS table_name,
    COALESCE(s.column_name, t.column_name) AS column_name,
    s.data_type AS source_data_type,
    t.data_type AS target_data_type,
    CASE
        WHEN s.column_name IS NULL THEN 'MISSING IN SOURCE'
        WHEN t.column_name IS NULL THEN 'MISSING IN TARGET'
        WHEN s.data_type != t.data_type THEN 'DATA TYPE MISMATCH'
        WHEN s.ordinal_position != t.ordinal_position THEN 'POSITION MISMATCH'
        ELSE 'MATCH'
    END AS validation_status
FROM source_columns s
FULL OUTER JOIN target_columns t
    ON s.table_schema = t.table_schema
    AND s.table_name = t.table_name
    AND s.column_name = t.column_name
WHERE COALESCE(s.data_type, '') != COALESCE(t.data_type, '')
   OR s.column_name IS NULL
   OR t.column_name IS NULL
ORDER BY table_schema, table_name, ordinal_position;

-- If no rows returned, all columns match

-- ----------------------------------------------------------------------------
-- VALIDATION 4: View Functionality Test
-- ----------------------------------------------------------------------------

-- Test view is queryable
SELECT 'VW_CURRENT_HOLDINGS' AS view_name,
       COUNT(*) AS row_count,
       'QUERYABLE' AS status
FROM DEV_DB.MART_INVESTMENTS_BOLT.VW_CURRENT_HOLDINGS;

-- Sample view output
SELECT * FROM DEV_DB.MART_INVESTMENTS_BOLT.VW_CURRENT_HOLDINGS LIMIT 10;

-- ----------------------------------------------------------------------------
-- VALIDATION 5: Stored Procedure Execution Test
-- ----------------------------------------------------------------------------

-- Test procedure 1
CALL DEV_DB.MART_INVESTMENTS_BOLT.SP_LOAD_DIM_STOCKS();

-- Test procedure 2
CALL DEV_DB.MART_INVESTMENTS_BOLT.SP_CALCULATE_DAILY_POSITIONS();

-- ----------------------------------------------------------------------------
-- VALIDATION 6: Dependency Verification
-- ----------------------------------------------------------------------------

-- Check dependencies exist in target
SELECT
    REFERENCING_OBJECT_NAME,
    REFERENCING_OBJECT_DOMAIN,
    REFERENCED_OBJECT_NAME,
    REFERENCED_OBJECT_DOMAIN,
    REFERENCED_SCHEMA
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCING_DATABASE = 'DEV_DB'
  AND REFERENCING_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY REFERENCING_OBJECT_NAME;

-- Check for broken external dependencies
SELECT DISTINCT
    REFERENCING_OBJECT_NAME,
    REFERENCING_OBJECT_DOMAIN,
    REFERENCED_DATABASE,
    REFERENCED_SCHEMA,
    REFERENCED_OBJECT_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCING_DATABASE = 'DEV_DB'
  AND REFERENCING_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
  AND REFERENCED_DATABASE != 'DEV_DB';

-- Should return 0 rows (no external dependencies)

-- ----------------------------------------------------------------------------
-- VALIDATION 7: Data Sampling and Comparison
-- ----------------------------------------------------------------------------

-- Compare first 100 rows of critical table
WITH source_sample AS (
    SELECT *, 'SOURCE' AS origin
    FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
    ORDER BY TRANSACTION_ID
    LIMIT 100
),
target_sample AS (
    SELECT *, 'TARGET' AS origin
    FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
    ORDER BY TRANSACTION_ID
    LIMIT 100
)
SELECT * FROM source_sample
UNION ALL
SELECT * FROM target_sample
ORDER BY TRANSACTION_ID, origin;

-- ----------------------------------------------------------------------------
-- VALIDATION 8: NULL Value Comparison
-- ----------------------------------------------------------------------------

SELECT
    'SOURCE' AS source_type,
    'STOCK_METADATA_RAW' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(STOCK_ID) AS non_null_stock_id,
    COUNT(TICKER) AS non_null_ticker,
    COUNT(COMPANY_NAME) AS non_null_company_name
FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW

UNION ALL

SELECT
    'TARGET' AS source_type,
    'STOCK_METADATA_RAW' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(STOCK_ID) AS non_null_stock_id,
    COUNT(TICKER) AS non_null_ticker,
    COUNT(COMPANY_NAME) AS non_null_company_name
FROM DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;

-- ----------------------------------------------------------------------------
-- VALIDATION 9: Aggregate Comparisons
-- ----------------------------------------------------------------------------

-- Compare aggregates on fact tables
WITH source_agg AS (
    SELECT
        'SOURCE' AS source_type,
        COUNT(*) AS transaction_count,
        SUM(TRANSACTION_AMOUNT) AS total_amount,
        AVG(TRANSACTION_AMOUNT) AS avg_amount,
        MIN(TRANSACTION_DATE) AS min_date,
        MAX(TRANSACTION_DATE) AS max_date
    FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
),
target_agg AS (
    SELECT
        'TARGET' AS source_type,
        COUNT(*) AS transaction_count,
        SUM(TRANSACTION_AMOUNT) AS total_amount,
        AVG(TRANSACTION_AMOUNT) AS avg_amount,
        MIN(TRANSACTION_DATE) AS min_date,
        MAX(TRANSACTION_DATE) AS max_date
    FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
)
SELECT * FROM source_agg
UNION ALL
SELECT * FROM target_agg;

-- ----------------------------------------------------------------------------
-- VALIDATION 10: Migration Summary Report
-- ----------------------------------------------------------------------------

SELECT
    'MIGRATION VALIDATION SUMMARY' AS report_section,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.TABLES
     WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
       AND table_type = 'BASE TABLE') AS tables_migrated,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.VIEWS
     WHERE table_schema = 'MART_INVESTMENTS_BOLT') AS views_migrated,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.PROCEDURES
     WHERE procedure_schema = 'MART_INVESTMENTS_BOLT') AS procedures_migrated,
    (SELECT SUM(row_count) FROM DEV_DB.INFORMATION_SCHEMA.TABLES
     WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
       AND table_type = 'BASE TABLE') AS total_rows_migrated;
