-- ============================================================================
-- IMCUST - MANUAL DISCOVERY SCRIPT
-- Description: Discover all objects and dependencies for migration
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;

-- ----------------------------------------------------------------------------
-- STEP 1: Base Objects Inventory
-- ----------------------------------------------------------------------------

SELECT
    table_catalog AS database_name,
    table_schema,
    table_name,
    table_type,
    row_count,
    bytes,
    clustering_key,
    created,
    last_altered
FROM PROD_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_type, table_name;

-- ----------------------------------------------------------------------------
-- STEP 2: Views Inventory
-- ----------------------------------------------------------------------------

SELECT
    table_catalog AS database_name,
    table_schema,
    table_name,
    is_secure,
    created,
    last_altered,
    LENGTH(view_definition) AS view_def_length
FROM PROD_DB.INFORMATION_SCHEMA.VIEWS
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name;

-- ----------------------------------------------------------------------------
-- STEP 3: Stored Procedures Inventory
-- ----------------------------------------------------------------------------

SELECT
    procedure_catalog AS database_name,
    procedure_schema,
    procedure_name,
    argument_signature,
    data_type AS return_type,
    procedure_language,
    created,
    last_altered
FROM PROD_DB.INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY procedure_schema, procedure_name;

-- ----------------------------------------------------------------------------
-- STEP 4: Recursive Upstream Dependencies (What base objects depend ON)
-- ----------------------------------------------------------------------------

WITH RECURSIVE upstream_deps AS (
    -- Anchor: Base objects
    SELECT DISTINCT
        REFERENCING_DATABASE,
        REFERENCING_SCHEMA,
        REFERENCING_OBJECT_NAME,
        REFERENCING_OBJECT_DOMAIN,
        REFERENCED_DATABASE,
        REFERENCED_SCHEMA,
        REFERENCED_OBJECT_NAME,
        REFERENCED_OBJECT_DOMAIN,
        REFERENCING_OBJECT_NAME AS start_object,
        1 AS dependency_level,
        REFERENCING_OBJECT_NAME || ' -> ' || REFERENCED_OBJECT_NAME AS dependency_path
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCING_DATABASE = 'PROD_DB'
      AND REFERENCING_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND REFERENCING_OBJECT_NAME IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )

    UNION ALL

    -- Recursive: What those dependencies depend on
    SELECT DISTINCT
        od.REFERENCING_DATABASE,
        od.REFERENCING_SCHEMA,
        od.REFERENCING_OBJECT_NAME,
        od.REFERENCING_OBJECT_DOMAIN,
        od.REFERENCED_DATABASE,
        od.REFERENCED_SCHEMA,
        od.REFERENCED_OBJECT_NAME,
        od.REFERENCED_OBJECT_DOMAIN,
        ud.start_object,
        ud.dependency_level + 1,
        ud.dependency_path || ' -> ' || od.REFERENCED_OBJECT_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
    INNER JOIN upstream_deps ud
        ON od.REFERENCING_DATABASE = ud.REFERENCED_DATABASE
        AND od.REFERENCING_SCHEMA = ud.REFERENCED_SCHEMA
        AND od.REFERENCING_OBJECT_NAME = ud.REFERENCED_OBJECT_NAME
    WHERE ud.dependency_level < 10
)
SELECT DISTINCT
    start_object,
    REFERENCED_DATABASE,
    REFERENCED_SCHEMA,
    REFERENCED_OBJECT_NAME,
    REFERENCED_OBJECT_DOMAIN,
    dependency_level,
    dependency_path
FROM upstream_deps
WHERE REFERENCED_DATABASE = 'PROD_DB'
ORDER BY start_object, dependency_level, REFERENCED_OBJECT_NAME;

-- ----------------------------------------------------------------------------
-- STEP 5: Recursive Downstream Dependencies (What depends ON base objects)
-- ----------------------------------------------------------------------------

WITH RECURSIVE downstream_deps AS (
    -- Anchor: Objects that reference base objects
    SELECT DISTINCT
        REFERENCING_DATABASE,
        REFERENCING_SCHEMA,
        REFERENCING_OBJECT_NAME,
        REFERENCING_OBJECT_DOMAIN,
        REFERENCED_DATABASE,
        REFERENCED_SCHEMA,
        REFERENCED_OBJECT_NAME,
        REFERENCED_OBJECT_DOMAIN,
        REFERENCED_OBJECT_NAME AS start_object,
        1 AS dependency_level,
        REFERENCED_OBJECT_NAME || ' <- ' || REFERENCING_OBJECT_NAME AS dependency_path
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCED_DATABASE = 'PROD_DB'
      AND REFERENCED_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND REFERENCED_OBJECT_NAME IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )

    UNION ALL

    -- Recursive: What depends on those
    SELECT DISTINCT
        od.REFERENCING_DATABASE,
        od.REFERENCING_SCHEMA,
        od.REFERENCING_OBJECT_NAME,
        od.REFERENCING_OBJECT_DOMAIN,
        od.REFERENCED_DATABASE,
        od.REFERENCED_SCHEMA,
        od.REFERENCED_OBJECT_NAME,
        od.REFERENCED_OBJECT_DOMAIN,
        dd.start_object,
        dd.dependency_level + 1,
        dd.dependency_path || ' <- ' || od.REFERENCING_OBJECT_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
    INNER JOIN downstream_deps dd
        ON od.REFERENCED_DATABASE = dd.REFERENCING_DATABASE
        AND od.REFERENCED_SCHEMA = dd.REFERENCING_SCHEMA
        AND od.REFERENCED_OBJECT_NAME = dd.REFERENCING_OBJECT_NAME
    WHERE dd.dependency_level < 10
)
SELECT DISTINCT
    start_object,
    REFERENCING_DATABASE,
    REFERENCING_SCHEMA,
    REFERENCING_OBJECT_NAME,
    REFERENCING_OBJECT_DOMAIN,
    dependency_level,
    dependency_path
FROM downstream_deps
WHERE REFERENCING_DATABASE = 'PROD_DB'
ORDER BY start_object, dependency_level, REFERENCING_OBJECT_NAME;

-- ----------------------------------------------------------------------------
-- STEP 6: External Dependencies Warning
-- ----------------------------------------------------------------------------

WITH all_refs AS (
    SELECT DISTINCT
        REFERENCING_OBJECT_NAME,
        REFERENCING_OBJECT_DOMAIN,
        REFERENCED_DATABASE,
        REFERENCED_SCHEMA,
        REFERENCED_OBJECT_NAME,
        REFERENCED_OBJECT_DOMAIN
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCING_DATABASE = 'PROD_DB'
      AND REFERENCING_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
)
SELECT
    'EXTERNAL DEPENDENCY WARNING' AS alert_type,
    REFERENCING_OBJECT_NAME,
    REFERENCING_OBJECT_DOMAIN,
    REFERENCED_DATABASE,
    REFERENCED_SCHEMA,
    REFERENCED_OBJECT_NAME,
    REFERENCED_OBJECT_DOMAIN
FROM all_refs
WHERE REFERENCED_DATABASE != 'PROD_DB'
   OR REFERENCED_SCHEMA NOT IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT');

-- ----------------------------------------------------------------------------
-- STEP 7: Complete Migration Object List
-- ----------------------------------------------------------------------------

WITH all_dependencies AS (
    -- Upstream dependencies
    SELECT DISTINCT
        REFERENCED_DATABASE AS database_name,
        REFERENCED_SCHEMA AS schema_name,
        REFERENCED_OBJECT_NAME AS object_name,
        REFERENCED_OBJECT_DOMAIN AS object_type,
        0 AS priority
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCING_DATABASE = 'PROD_DB'
      AND REFERENCING_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND REFERENCING_OBJECT_NAME IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )
      AND REFERENCED_DATABASE = 'PROD_DB'

    UNION

    -- Base objects
    SELECT DISTINCT
        'PROD_DB' AS database_name,
        schema_name,
        object_name,
        'TABLE' AS object_type,
        1 AS priority
    FROM (
        SELECT 'SRC_INVESTMENTS_BOLT' AS schema_name, 'STOCK_METADATA_RAW' AS object_name
        UNION ALL SELECT 'MART_INVESTMENTS_BOLT', 'DIM_STOCKS'
        UNION ALL SELECT 'MART_INVESTMENTS_BOLT', 'DIM_PORTFOLIOS'
        UNION ALL SELECT 'MART_INVESTMENTS_BOLT', 'FACT_TRANSACTIONS'
        UNION ALL SELECT 'MART_INVESTMENTS_BOLT', 'FACT_DAILY_POSITIONS'
    )

    UNION

    SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'VW_CURRENT_HOLDINGS', 'VIEW', 1
    UNION
    SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'SP_LOAD_DIM_STOCKS', 'PROCEDURE', 1
    UNION
    SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'SP_CALCULATE_DAILY_POSITIONS', 'PROCEDURE', 1

    UNION

    -- Downstream dependencies
    SELECT DISTINCT
        REFERENCING_DATABASE AS database_name,
        REFERENCING_SCHEMA AS schema_name,
        REFERENCING_OBJECT_NAME AS object_name,
        REFERENCING_OBJECT_DOMAIN AS object_type,
        2 AS priority
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCED_DATABASE = 'PROD_DB'
      AND REFERENCED_SCHEMA IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND REFERENCED_OBJECT_NAME IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )
      AND REFERENCING_DATABASE = 'PROD_DB'
)
SELECT
    priority,
    database_name,
    schema_name,
    object_name,
    object_type
FROM all_dependencies
WHERE database_name = 'PROD_DB'
  AND schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY priority, schema_name, object_type, object_name;

-- ----------------------------------------------------------------------------
-- STEP 8: Migration Summary
-- ----------------------------------------------------------------------------

WITH migration_objects AS (
    SELECT 'TABLE' AS object_type FROM (VALUES (1),(2),(3),(4),(5)) UNION ALL
    SELECT 'VIEW' UNION ALL
    SELECT 'PROCEDURE' FROM (VALUES (1),(2))
)
SELECT
    'MIGRATION SUMMARY' AS report_section,
    COUNT(*) AS total_base_objects,
    SUM(CASE WHEN object_type = 'TABLE' THEN 1 ELSE 0 END) AS base_tables,
    SUM(CASE WHEN object_type = 'VIEW' THEN 1 ELSE 0 END) AS base_views,
    SUM(CASE WHEN object_type = 'PROCEDURE' THEN 1 ELSE 0 END) AS base_procedures
FROM migration_objects;
