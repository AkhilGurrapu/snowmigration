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
        referencing_database_name,
        referencing_schema_name,
        referencing_object_name,
        referencing_object_domain,
        referenced_database_name,
        referenced_schema_name,
        referenced_object_name,
        referenced_object_domain,
        referencing_object_name AS start_object,
        1 AS dependency_level,
        referencing_object_name || ' -> ' || referenced_object_name AS dependency_path
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE referencing_database_name = 'PROD_DB'
      AND referencing_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND referencing_object_name IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )

    UNION ALL

    -- Recursive: What those dependencies depend on
    SELECT DISTINCT
        od.referencing_database_name,
        od.referencing_schema_name,
        od.referencing_object_name,
        od.referencing_object_domain,
        od.referenced_database_name,
        od.referenced_schema_name,
        od.referenced_object_name,
        od.referenced_object_domain,
        ud.start_object,
        ud.dependency_level + 1,
        ud.dependency_path || ' -> ' || od.referenced_object_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
    INNER JOIN upstream_deps ud
        ON od.referencing_database_name = ud.referenced_database_name
        AND od.referencing_schema_name = ud.referenced_schema_name
        AND od.referencing_object_name = ud.referenced_object_name
    WHERE ud.dependency_level < 10
)
SELECT DISTINCT
    start_object,
    referenced_database_name,
    referenced_schema_name,
    referenced_object_name,
    referenced_object_domain,
    dependency_level,
    dependency_path
FROM upstream_deps
WHERE referenced_database_name = 'PROD_DB'
ORDER BY start_object, dependency_level, referenced_object_name;

-- ----------------------------------------------------------------------------
-- STEP 5: Recursive Downstream Dependencies (What depends ON base objects)
-- ----------------------------------------------------------------------------

WITH RECURSIVE downstream_deps AS (
    -- Anchor: Objects that reference base objects
    SELECT DISTINCT
        referencing_database_name,
        referencing_schema_name,
        referencing_object_name,
        referencing_object_domain,
        referenced_database_name,
        referenced_schema_name,
        referenced_object_name,
        referenced_object_domain,
        referenced_object_name AS start_object,
        1 AS dependency_level,
        referenced_object_name || ' <- ' || referencing_object_name AS dependency_path
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE referenced_database_name = 'PROD_DB'
      AND referenced_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND referenced_object_name IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )

    UNION ALL

    -- Recursive: What depends on those
    SELECT DISTINCT
        od.referencing_database_name,
        od.referencing_schema_name,
        od.referencing_object_name,
        od.referencing_object_domain,
        od.referenced_database_name,
        od.referenced_schema_name,
        od.referenced_object_name,
        od.referenced_object_domain,
        dd.start_object,
        dd.dependency_level + 1,
        dd.dependency_path || ' <- ' || od.referencing_object_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
    INNER JOIN downstream_deps dd
        ON od.referenced_database_name = dd.referencing_database_name
        AND od.referenced_schema_name = dd.referencing_schema_name
        AND od.referenced_object_name = dd.referencing_object_name
    WHERE dd.dependency_level < 10
)
SELECT DISTINCT
    start_object,
    referencing_database_name,
    referencing_schema_name,
    referencing_object_name,
    referencing_object_domain,
    dependency_level,
    dependency_path
FROM downstream_deps
WHERE referencing_database_name = 'PROD_DB'
ORDER BY start_object, dependency_level, referencing_object_name;

-- ----------------------------------------------------------------------------
-- STEP 6: External Dependencies Warning
-- ----------------------------------------------------------------------------

WITH all_refs AS (
    SELECT DISTINCT
        referencing_object_name,
        referencing_object_domain,
        referenced_database_name,
        referenced_schema_name,
        referenced_object_name,
        referenced_object_domain
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE referencing_database_name = 'PROD_DB'
      AND referencing_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
)
SELECT
    'EXTERNAL DEPENDENCY WARNING' AS alert_type,
    referencing_object_name,
    referencing_object_domain,
    referenced_database_name,
    referenced_schema_name,
    referenced_object_name,
    referenced_object_domain
FROM all_refs
WHERE referenced_database_name != 'PROD_DB'
   OR referenced_schema_name NOT IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT');

-- ----------------------------------------------------------------------------
-- STEP 7: Complete Migration Object List
-- ----------------------------------------------------------------------------

WITH all_dependencies AS (
    -- Upstream dependencies
    SELECT DISTINCT
        referenced_database_name AS database_name,
        referenced_schema_name AS schema_name,
        referenced_object_name AS object_name,
        referenced_object_domain AS object_type,
        0 AS priority
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE referencing_database_name = 'PROD_DB'
      AND referencing_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND referencing_object_name IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )
      AND referenced_database_name = 'PROD_DB'

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
        referencing_database_name AS database_name,
        referencing_schema_name AS schema_name,
        referencing_object_name AS object_name,
        referencing_object_domain AS object_type,
        2 AS priority
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE referenced_database_name = 'PROD_DB'
      AND referenced_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
      AND referenced_object_name IN (
          'STOCK_METADATA_RAW', 'DIM_STOCKS', 'DIM_PORTFOLIOS',
          'FACT_TRANSACTIONS', 'FACT_DAILY_POSITIONS', 'VW_CURRENT_HOLDINGS',
          'SP_LOAD_DIM_STOCKS', 'SP_CALCULATE_DAILY_POSITIONS'
      )
      AND referencing_database_name = 'PROD_DB'
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
