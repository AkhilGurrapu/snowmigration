-- ============================================================================
-- IMSDLC - MANUAL CREATE OBJECTS SCRIPT
-- Description: Create tables, views, and procedures with transformed DDL
-- Instructions: Replace PROD_DB with DEV_DB in DDL from IMCUST extraction
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;

-- ----------------------------------------------------------------------------
-- STEP 1: Create Tables (Empty Structure Only)
-- ----------------------------------------------------------------------------

-- Option 1: Using CTAS with WHERE 1=0 (creates structure, no data)

CREATE OR REPLACE TABLE DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW AS
SELECT * FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
WHERE 1=0;

CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS AS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
WHERE 1=0;

CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS AS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
WHERE 1=0;

CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS AS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
WHERE 1=0;

CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS AS
SELECT * FROM MIGRATION_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS
WHERE 1=0;

-- ----------------------------------------------------------------------------
-- Option 2: Use Transformed DDL from GET_DDL output
-- Instructions:
-- 1. Copy DDL from IMCUST MANUAL_02_extract_ddl.sql results
-- 2. Replace all instances of PROD_DB with DEV_DB
-- 3. Paste transformed DDL below
-- ----------------------------------------------------------------------------

/*
-- Example template (replace with actual DDL):

CREATE OR REPLACE TABLE DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW (
    STOCK_ID NUMBER(38,0),
    TICKER VARCHAR(16777216),
    COMPANY_NAME VARCHAR(16777216),
    -- ... other columns
)
CLUSTER BY (STOCK_ID);

-- Repeat for all tables with actual DDL
*/

-- ----------------------------------------------------------------------------
-- STEP 2: Verify Table Creation
-- ----------------------------------------------------------------------------

SELECT
    table_schema,
    table_name,
    table_type,
    row_count,
    created
FROM DEV_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- ----------------------------------------------------------------------------
-- STEP 3: Create Views (Use Transformed DDL)
-- Instructions:
-- 1. Copy view DDL from IMCUST extraction
-- 2. Replace PROD_DB with DEV_DB in view definition
-- 3. Verify all table references are correct
-- ----------------------------------------------------------------------------

/*
-- Example template (replace with actual transformed view DDL):

CREATE OR REPLACE VIEW DEV_DB.MART_INVESTMENTS_BOLT.VW_CURRENT_HOLDINGS AS
SELECT
    fdp.portfolio_id,
    fdp.stock_id,
    ds.ticker,
    ds.company_name,
    fdp.position_quantity,
    fdp.position_value,
    fdp.position_date
FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS fdp
INNER JOIN DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS ds
    ON fdp.stock_id = ds.stock_id
WHERE fdp.position_date = CURRENT_DATE();

*/

-- ----------------------------------------------------------------------------
-- STEP 4: Verify View Creation
-- ----------------------------------------------------------------------------

SELECT
    table_schema,
    table_name,
    is_secure,
    created
FROM DEV_DB.INFORMATION_SCHEMA.VIEWS
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name;

-- Test view is queryable (will return 0 rows until data loaded)
SELECT COUNT(*) AS view_row_count
FROM DEV_DB.MART_INVESTMENTS_BOLT.VW_CURRENT_HOLDINGS;

-- ----------------------------------------------------------------------------
-- STEP 5: Create Stored Procedures (Use Transformed DDL)
-- Instructions:
-- 1. Copy procedure DDL from IMCUST extraction
-- 2. Replace PROD_DB with DEV_DB in procedure body
-- 3. Verify all internal references updated
-- ----------------------------------------------------------------------------

/*
-- Example template (replace with actual transformed procedure DDL):

CREATE OR REPLACE PROCEDURE DEV_DB.MART_INVESTMENTS_BOLT.SP_LOAD_DIM_STOCKS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Procedure logic here
    -- Ensure all references to PROD_DB are changed to DEV_DB
    MERGE INTO DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS AS target
    USING DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW AS source
    ON target.stock_id = source.stock_id
    WHEN MATCHED THEN
        UPDATE SET
            target.ticker = source.ticker,
            target.company_name = source.company_name
    WHEN NOT MATCHED THEN
        INSERT (stock_id, ticker, company_name)
        VALUES (source.stock_id, source.ticker, source.company_name);

    RETURN 'SP_LOAD_DIM_STOCKS completed successfully';
END;
$$;

CREATE OR REPLACE PROCEDURE DEV_DB.MART_INVESTMENTS_BOLT.SP_CALCULATE_DAILY_POSITIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Procedure logic here
    -- Ensure all references to PROD_DB are changed to DEV_DB

    RETURN 'SP_CALCULATE_DAILY_POSITIONS completed successfully';
END;
$$;

*/

-- ----------------------------------------------------------------------------
-- STEP 6: Verify Procedure Creation
-- ----------------------------------------------------------------------------

SELECT
    procedure_schema,
    procedure_name,
    argument_signature,
    procedure_language,
    created
FROM DEV_DB.INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY procedure_schema, procedure_name;

-- ----------------------------------------------------------------------------
-- STEP 7: Verify Column Definitions Match Source
-- ----------------------------------------------------------------------------

-- Compare column structures
SELECT
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable
FROM DEV_DB.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name, ordinal_position;
