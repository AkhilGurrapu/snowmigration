-- ============================================================================
-- IMCUST - MANUAL DDL EXTRACTION SCRIPT
-- Description: Extract DDL for all discovered objects
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;

-- ----------------------------------------------------------------------------
-- STEP 1: Extract Table DDL (Base Tables)
-- ----------------------------------------------------------------------------

SELECT
    'STOCK_METADATA_RAW' AS object_name,
    'TABLE' AS object_type,
    'SRC_INVESTMENTS_BOLT' AS schema_name,
    GET_DDL('TABLE', 'PROD_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW', TRUE) AS ddl_statement;

SELECT
    'DIM_STOCKS' AS object_name,
    'TABLE' AS object_type,
    'MART_INVESTMENTS_BOLT' AS schema_name,
    GET_DDL('TABLE', 'PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS', TRUE) AS ddl_statement;

SELECT
    'DIM_PORTFOLIOS' AS object_name,
    'TABLE' AS object_type,
    'MART_INVESTMENTS_BOLT' AS schema_name,
    GET_DDL('TABLE', 'PROD_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS', TRUE) AS ddl_statement;

SELECT
    'FACT_TRANSACTIONS' AS object_name,
    'TABLE' AS object_type,
    'MART_INVESTMENTS_BOLT' AS schema_name,
    GET_DDL('TABLE', 'PROD_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS', TRUE) AS ddl_statement;

SELECT
    'FACT_DAILY_POSITIONS' AS object_name,
    'TABLE' AS object_type,
    'MART_INVESTMENTS_BOLT' AS schema_name,
    GET_DDL('TABLE', 'PROD_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS', TRUE) AS ddl_statement;

-- ----------------------------------------------------------------------------
-- STEP 2: Extract All Tables DDL (Dynamic - Run if more tables discovered)
-- ----------------------------------------------------------------------------

SELECT
    table_schema,
    table_name,
    'TABLE' AS object_type,
    GET_DDL('TABLE', 'PROD_DB.' || table_schema || '.' || table_name, TRUE) AS ddl_statement
FROM PROD_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- ----------------------------------------------------------------------------
-- STEP 3: Extract View DDL
-- ----------------------------------------------------------------------------

SELECT
    'VW_CURRENT_HOLDINGS' AS object_name,
    'VIEW' AS object_type,
    'MART_INVESTMENTS_BOLT' AS schema_name,
    GET_DDL('VIEW', 'PROD_DB.MART_INVESTMENTS_BOLT.VW_CURRENT_HOLDINGS', TRUE) AS ddl_statement;

-- Extract all views
SELECT
    table_schema,
    table_name,
    'VIEW' AS object_type,
    GET_DDL('VIEW', 'PROD_DB.' || table_schema || '.' || table_name, TRUE) AS ddl_statement
FROM PROD_DB.INFORMATION_SCHEMA.VIEWS
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name;

-- ----------------------------------------------------------------------------
-- STEP 4: Extract Stored Procedure DDL
-- ----------------------------------------------------------------------------

-- Note: Must include argument signature for procedures
SELECT
    procedure_schema,
    procedure_name,
    argument_signature,
    'PROCEDURE' AS object_type,
    GET_DDL('PROCEDURE',
            'PROD_DB.' || procedure_schema || '.' || procedure_name ||
            '(' || COALESCE(argument_signature, '') || ')',
            TRUE) AS ddl_statement
FROM PROD_DB.INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY procedure_schema, procedure_name;

-- ----------------------------------------------------------------------------
-- STEP 5: Extract Row Counts for Validation
-- ----------------------------------------------------------------------------

SELECT 'STOCK_METADATA_RAW' AS table_name, COUNT(*) AS row_count
FROM PROD_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;

SELECT 'DIM_STOCKS' AS table_name, COUNT(*) AS row_count
FROM PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS;

SELECT 'DIM_PORTFOLIOS' AS table_name, COUNT(*) AS row_count
FROM PROD_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS;

SELECT 'FACT_TRANSACTIONS' AS table_name, COUNT(*) AS row_count
FROM PROD_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS;

SELECT 'FACT_DAILY_POSITIONS' AS table_name, COUNT(*) AS row_count
FROM PROD_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;

-- ----------------------------------------------------------------------------
-- STEP 6: Extract Complete Schema DDL (Alternative approach)
-- ----------------------------------------------------------------------------

-- Extract entire schema recursively (includes all objects)
SELECT GET_DDL('SCHEMA', 'PROD_DB.SRC_INVESTMENTS_BOLT', TRUE) AS schema_ddl;

SELECT GET_DDL('SCHEMA', 'PROD_DB.MART_INVESTMENTS_BOLT', TRUE) AS schema_ddl;
