-- ============================================================================
-- IMSDLC - MANUAL DATA POPULATION SCRIPT
-- Description: Populate tables from shared database using INSERT INTO SELECT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;

-- ----------------------------------------------------------------------------
-- STEP 1: Populate Base Table (SRC Schema)
-- ----------------------------------------------------------------------------

INSERT INTO DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
SELECT * FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;

-- Verify
SELECT 'STOCK_METADATA_RAW' AS table_name,
       COUNT(*) AS row_count,
       MIN(STOCK_ID) AS min_id,
       MAX(STOCK_ID) AS max_id
FROM DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;

-- ----------------------------------------------------------------------------
-- STEP 2: Populate Dimension Tables (MART Schema)
-- ----------------------------------------------------------------------------

INSERT INTO DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS;

-- Verify
SELECT 'DIM_STOCKS' AS table_name,
       COUNT(*) AS row_count,
       MIN(STOCK_ID) AS min_id,
       MAX(STOCK_ID) AS max_id
FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS;

INSERT INTO DEV_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS;

-- Verify
SELECT 'DIM_PORTFOLIOS' AS table_name,
       COUNT(*) AS row_count,
       MIN(PORTFOLIO_ID) AS min_id,
       MAX(PORTFOLIO_ID) AS max_id
FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS;

-- ----------------------------------------------------------------------------
-- STEP 3: Populate Fact Tables (MART Schema)
-- ----------------------------------------------------------------------------

INSERT INTO DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS;

-- Verify
SELECT 'FACT_TRANSACTIONS' AS table_name,
       COUNT(*) AS row_count,
       MIN(TRANSACTION_ID) AS min_id,
       MAX(TRANSACTION_ID) AS max_id
FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS;

INSERT INTO DEV_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;

-- Verify
SELECT 'FACT_DAILY_POSITIONS' AS table_name,
       COUNT(*) AS row_count,
       MIN(POSITION_DATE) AS min_date,
       MAX(POSITION_DATE) AS max_date
FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;

-- ----------------------------------------------------------------------------
-- STEP 4: Row Count Comparison (Source vs Target)
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
        ELSE 'FAIL - ROW COUNT MISMATCH'
    END AS validation_status,
    s.count - t.count AS difference
FROM source_counts s
JOIN target_counts t ON s.table_name = t.table_name
ORDER BY s.table_name;

-- ----------------------------------------------------------------------------
-- STEP 5: Sample Data Comparison (First 10 rows)
-- ----------------------------------------------------------------------------

SELECT 'SOURCE' AS source_type, *
FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
ORDER BY STOCK_ID
LIMIT 10;

SELECT 'TARGET' AS source_type, *
FROM DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
ORDER BY STOCK_ID
LIMIT 10;

-- ----------------------------------------------------------------------------
-- STEP 6: Monitor Query Performance and Credits
-- ----------------------------------------------------------------------------

SELECT
    query_text,
    execution_status,
    total_elapsed_time / 1000 AS elapsed_seconds,
    rows_inserted,
    rows_updated,
    rows_deleted,
    credits_used_cloud_services,
    warehouse_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%INSERT INTO DEV_DB%'
  AND start_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
