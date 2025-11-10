-- IMCUST Account: Object Discovery and Inventory
-- Purpose: Identify all objects in mart_investments_bolt and src_investments_bolt schemas
-- Database: prod_db

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;

-- =============================================================================
-- 1. SCHEMA OVERVIEW
-- =============================================================================
SELECT 
    '=== SCHEMA OVERVIEW ===' AS section;

SELECT 
    catalog_name AS database_name,
    schema_name,
    schema_owner,
    created,
    last_altered,
    comment
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
ORDER BY schema_name;

-- =============================================================================
-- 2. TABLE INVENTORY
-- =============================================================================
SELECT 
    '=== TABLE INVENTORY ===' AS section;

SELECT 
    table_schema,
    table_name,
    table_type,
    table_owner,
    row_count,
    bytes,
    ROUND(bytes/1024/1024/1024, 2) AS size_gb,
    clustering_key,
    created,
    last_altered,
    comment
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
    AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- =============================================================================
-- 3. COLUMN DETAILS FOR TARGET TABLES
-- =============================================================================
SELECT 
    '=== COLUMN DETAILS ===' AS section;

SELECT 
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    comment
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
    AND table_name IN (
        -- SRC tables
        'STOCK_METADATA_RAW',
        -- MART tables
        'DIM_STOCKS', 'DIM_PORTFOLIOS', 'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS'
    )
ORDER BY table_schema, table_name, ordinal_position;

-- =============================================================================
-- 4. VIEW INVENTORY
-- =============================================================================
SELECT 
    '=== VIEW INVENTORY ===' AS section;

SELECT 
    table_schema,
    table_name AS view_name,
    table_owner AS view_owner,
    is_secure,
    created,
    last_altered,
    comment
FROM INFORMATION_SCHEMA.VIEWS
WHERE table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name;

-- Get view definition for our target view
SELECT 
    '=== VIEW DEFINITION: VW_CURRENT_HOLDINGS ===' AS section;

SELECT 
    table_schema,
    table_name AS view_name,
    view_definition
FROM INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'MART_INVESTMENTS_BOLT'
    AND table_name = 'VW_CURRENT_HOLDINGS';

-- =============================================================================
-- 5. STORED PROCEDURE INVENTORY
-- =============================================================================
SELECT 
    '=== STORED PROCEDURE INVENTORY ===' AS section;

SELECT 
    procedure_schema,
    procedure_name,
    procedure_owner,
    argument_signature,
    data_type AS return_type,
    procedure_language,
    created,
    last_altered,
    comment
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
    AND procedure_name IN ('SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS')
ORDER BY procedure_schema, procedure_name;

-- =============================================================================
-- 6. OBJECT DEPENDENCIES (ACCOUNT_USAGE)
-- =============================================================================
SELECT 
    '=== OBJECT DEPENDENCIES ===' AS section;

-- Note: ACCOUNT_USAGE has up to 3 hour latency
SELECT 
    referenced_database_name,
    referenced_schema_name,
    referenced_object_name,
    referenced_object_type,
    referencing_database_name,
    referencing_schema_name,
    referencing_object_name,
    referencing_object_type,
    dependency_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referenced_database_name = 'PROD_DB'
    AND referenced_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
ORDER BY 
    referenced_schema_name,
    referenced_object_name,
    referencing_object_name;

-- =============================================================================
-- 7. ROW COUNT VERIFICATION
-- =============================================================================
SELECT 
    '=== ROW COUNT VERIFICATION ===' AS section;

-- Get actual row counts for migration validation
SELECT 'SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW' AS table_name, COUNT(*) AS row_count 
FROM SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT.DIM_STOCKS', COUNT(*) 
FROM MART_INVESTMENTS_BOLT.DIM_STOCKS
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS', COUNT(*) 
FROM MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS', COUNT(*) 
FROM MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS', COUNT(*) 
FROM MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS;

-- =============================================================================
-- 8. PRIMARY KEY AND UNIQUE CONSTRAINTS
-- =============================================================================
SELECT 
    '=== PRIMARY KEY CONSTRAINTS ===' AS section;

SHOW PRIMARY KEYS IN SCHEMA MART_INVESTMENTS_BOLT;
SHOW PRIMARY KEYS IN SCHEMA SRC_INVESTMENTS_BOLT;

-- =============================================================================
-- 9. CLUSTERING KEY INFORMATION
-- =============================================================================
SELECT 
    '=== CLUSTERING KEYS ===' AS section;

SELECT 
    table_schema,
    table_name,
    clustering_key
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
    AND table_type = 'BASE TABLE'
    AND clustering_key IS NOT NULL
ORDER BY table_schema, table_name;

-- =============================================================================
-- 10. SUMMARY
-- =============================================================================
SELECT 
    '=== MIGRATION SUMMARY ===' AS section;

SELECT 
    'Total Objects to Migrate' AS metric,
    COUNT(*) AS count
FROM (
    -- Tables
    SELECT table_name FROM INFORMATION_SCHEMA.TABLES 
    WHERE table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
        AND table_type = 'BASE TABLE'
        AND table_name IN ('STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS', 'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS')
    UNION ALL
    -- Views
    SELECT table_name FROM INFORMATION_SCHEMA.VIEWS 
    WHERE table_schema = 'MART_INVESTMENTS_BOLT'
        AND table_name = 'VW_CURRENT_HOLDINGS'
    UNION ALL
    -- Procedures
    SELECT procedure_name FROM INFORMATION_SCHEMA.PROCEDURES 
    WHERE procedure_schema = 'MART_INVESTMENTS_BOLT'
        AND procedure_name IN ('SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS')
) AS all_objects;
