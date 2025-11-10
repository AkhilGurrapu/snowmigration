-- ============================================================================
-- IMSDLC - MANUAL CONSUME SHARE SCRIPT
-- Description: Consume share from IMCUST and verify access
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;

-- ----------------------------------------------------------------------------
-- STEP 1: Verify Share Visibility
-- ----------------------------------------------------------------------------

SHOW SHARES;

-- ----------------------------------------------------------------------------
-- STEP 2: Describe Share Before Consuming
-- ----------------------------------------------------------------------------

DESCRIBE SHARE nfmyizv.imcust.MIGRATION_SHARE_IMCUST_TO_IMSDLC;

-- ----------------------------------------------------------------------------
-- STEP 3: Create Database from Share
-- ----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS MIGRATION_SHARED_DB
    FROM SHARE nfmyizv.imcust.MIGRATION_SHARE_IMCUST_TO_IMSDLC
    COMMENT = 'Temporary shared database for one-time migration from IMCUST';

-- ----------------------------------------------------------------------------
-- STEP 4: Verify Access to Shared Schemas
-- ----------------------------------------------------------------------------

SHOW SCHEMAS IN DATABASE MIGRATION_SHARED_DB;

-- ----------------------------------------------------------------------------
-- STEP 5: Verify Access to Shared Tables
-- ----------------------------------------------------------------------------

SHOW TABLES IN SCHEMA MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT;

SHOW TABLES IN SCHEMA MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT;

-- ----------------------------------------------------------------------------
-- STEP 6: Test Read Access and Get Row Counts
-- ----------------------------------------------------------------------------

SELECT 'STOCK_METADATA_RAW' AS table_name, COUNT(*) AS row_count
FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;

SELECT 'DIM_STOCKS' AS table_name, COUNT(*) AS row_count
FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS;

SELECT 'DIM_PORTFOLIOS' AS table_name, COUNT(*) AS row_count
FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS;

SELECT 'FACT_TRANSACTIONS' AS table_name, COUNT(*) AS row_count
FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS;

SELECT 'FACT_DAILY_POSITIONS' AS table_name, COUNT(*) AS row_count
FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;

-- ----------------------------------------------------------------------------
-- STEP 7: Sample Data Verification (First 5 rows)
-- ----------------------------------------------------------------------------

SELECT * FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW LIMIT 5;

SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS LIMIT 5;

SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS LIMIT 5;

SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS LIMIT 5;

SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS LIMIT 5;

-- ----------------------------------------------------------------------------
-- STEP 8: Verify Column Metadata
-- ----------------------------------------------------------------------------

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
ORDER BY table_schema, table_name, ordinal_position;
