-- IMCUST Account: Dependency Mapping
-- Purpose: Map all object dependencies for migration order
-- Database: prod_db

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;

-- =============================================================================
-- 1. VIEW DEPENDENCIES
-- =============================================================================
SELECT 
    '=== VIEW DEPENDENCIES ===' AS section;

-- Check what tables the view depends on
SELECT DISTINCT
    'VIEW' AS object_type,
    'MART_INVESTMENTS_BOLT.VW_CURRENT_HOLDINGS' AS object_name,
    referenced_database_name || '.' || referenced_schema_name || '.' || referenced_object_name AS depends_on,
    referenced_object_type AS dependency_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referencing_database_name = 'PROD_DB'
    AND referencing_schema_name = 'MART_INVESTMENTS_BOLT'
    AND referencing_object_name = 'VW_CURRENT_HOLDINGS'
    AND referencing_object_type = 'VIEW'
ORDER BY depends_on;

-- =============================================================================
-- 2. STORED PROCEDURE DEPENDENCIES
-- =============================================================================
SELECT 
    '=== STORED PROCEDURE DEPENDENCIES ===' AS section;

-- SP_LOAD_DIM_STOCKS dependencies
SELECT DISTINCT
    'PROCEDURE' AS object_type,
    'MART_INVESTMENTS_BOLT.SP_LOAD_DIM_STOCKS' AS object_name,
    referenced_database_name || '.' || referenced_schema_name || '.' || referenced_object_name AS depends_on,
    referenced_object_type AS dependency_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referencing_database_name = 'PROD_DB'
    AND referencing_schema_name = 'MART_INVESTMENTS_BOLT'
    AND referencing_object_name = 'SP_LOAD_DIM_STOCKS'
    AND referencing_object_type = 'PROCEDURE'

UNION ALL

-- SP_CALCULATE_DAILY_POSITIONS dependencies
SELECT DISTINCT
    'PROCEDURE' AS object_type,
    'MART_INVESTMENTS_BOLT.SP_CALCULATE_DAILY_POSITIONS' AS object_name,
    referenced_database_name || '.' || referenced_schema_name || '.' || referenced_object_name AS depends_on,
    referenced_object_type AS dependency_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referencing_database_name = 'PROD_DB'
    AND referencing_schema_name = 'MART_INVESTMENTS_BOLT'
    AND referencing_object_name = 'SP_CALCULATE_DAILY_POSITIONS'
    AND referencing_object_type = 'PROCEDURE'
ORDER BY object_name, depends_on;

-- =============================================================================
-- 3. TABLE RELATIONSHIPS (Foreign Key Analysis)
-- =============================================================================
SELECT 
    '=== TABLE RELATIONSHIPS ===' AS section;

-- Check for any foreign key relationships
SHOW IMPORTED KEYS IN SCHEMA MART_INVESTMENTS_BOLT;
SHOW IMPORTED KEYS IN SCHEMA SRC_INVESTMENTS_BOLT;

-- =============================================================================
-- 4. MANUAL DEPENDENCY ANALYSIS FROM VIEW DEFINITION
-- =============================================================================
SELECT 
    '=== MANUAL VIEW DEPENDENCY ANALYSIS ===' AS section;

-- Parse view definition to identify table dependencies
WITH view_def AS (
    SELECT 
        view_definition
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE table_schema = 'MART_INVESTMENTS_BOLT'
        AND table_name = 'VW_CURRENT_HOLDINGS'
)
SELECT 
    'VW_CURRENT_HOLDINGS likely depends on tables referenced in its definition' AS analysis,
    'Manual review required to identify all table dependencies' AS action;

-- =============================================================================
-- 5. PROCEDURE DEFINITION ANALYSIS
-- =============================================================================
SELECT 
    '=== PROCEDURE DEFINITIONS FOR DEPENDENCY ANALYSIS ===' AS section;

-- Get procedure definitions to manually check dependencies
SELECT 
    procedure_schema,
    procedure_name,
    argument_signature,
    procedure_definition
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema = 'MART_INVESTMENTS_BOLT'
    AND procedure_name IN ('SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS');

-- =============================================================================
-- 6. MIGRATION ORDER RECOMMENDATION
-- =============================================================================
SELECT 
    '=== RECOMMENDED MIGRATION ORDER ===' AS section;

WITH migration_objects AS (
    SELECT 1 AS order_seq, 'TABLE' AS object_type, 'SRC_INVESTMENTS_BOLT' AS schema_name, 'STOCK_METADATA_RAW' AS object_name, 'Base table - no dependencies' AS reason
    UNION ALL
    SELECT 2, 'TABLE', 'MART_INVESTMENTS_BOLT', 'DIM_STOCKS', 'Dimension table - may depend on SRC'
    UNION ALL
    SELECT 3, 'TABLE', 'MART_INVESTMENTS_BOLT', 'DIM_PORTFOLIOS', 'Dimension table - independent'
    UNION ALL
    SELECT 4, 'TABLE', 'MART_INVESTMENTS_BOLT', 'FACT_TRANSACTIONS', 'Fact table - depends on dimensions'
    UNION ALL
    SELECT 5, 'TABLE', 'MART_INVESTMENTS_BOLT', 'FACT_DAILY_POSITIONS', 'Fact table - depends on transactions'
    UNION ALL
    SELECT 6, 'VIEW', 'MART_INVESTMENTS_BOLT', 'VW_CURRENT_HOLDINGS', 'View - depends on multiple tables'
    UNION ALL
    SELECT 7, 'PROCEDURE', 'MART_INVESTMENTS_BOLT', 'SP_LOAD_DIM_STOCKS', 'Procedure - depends on tables'
    UNION ALL
    SELECT 8, 'PROCEDURE', 'MART_INVESTMENTS_BOLT', 'SP_CALCULATE_DAILY_POSITIONS', 'Procedure - depends on tables'
)
SELECT 
    order_seq,
    object_type,
    schema_name,
    object_name,
    schema_name || '.' || object_name AS full_name,
    reason
FROM migration_objects
ORDER BY order_seq;

-- =============================================================================
-- 7. DEPENDENCY VALIDATION QUERIES
-- =============================================================================
SELECT 
    '=== DEPENDENCY VALIDATION ===' AS section;

-- Check if any objects reference objects outside our migration scope
SELECT DISTINCT
    referencing_database_name || '.' || referencing_schema_name || '.' || referencing_object_name AS object_using_dependency,
    referenced_database_name || '.' || referenced_schema_name || '.' || referenced_object_name AS external_dependency
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referencing_database_name = 'PROD_DB'
    AND referencing_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
    AND referencing_object_name IN (
        'VW_CURRENT_HOLDINGS', 
        'SP_LOAD_DIM_STOCKS', 
        'SP_CALCULATE_DAILY_POSITIONS'
    )
    AND NOT (
        referenced_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
        AND referenced_object_name IN (
            'STOCK_METADATA_RAW', 
            'DIM_STOCKS', 
            'DIM_PORTFOLIOS', 
            'FACT_TRANSACTIONS', 
            'FACT_DAILY_POSITIONS'
        )
    )
ORDER BY object_using_dependency, external_dependency;
