-- =============================================================================
-- IMCUST Account: COMPLETE Object Discovery with ALL Dependencies
-- =============================================================================
-- Purpose: Discover ALL objects and their downstream dependencies starting from:
--   - SRC: stock_metadata_raw
--   - MART: dim_stocks, dim_portfolios, fact_transactions, fact_daily_positions
--   - VIEW: vw_current_holdings
--   - PROCEDURES: sp_load_dim_stocks, sp_calculate_daily_positions
--
-- This script uses RECURSIVE CTEs to find ALL downstream dependencies including:
--   - Tables that depend on source tables
--   - Views that depend on tables/views
--   - Procedures that reference tables/views
--   - Dynamic Tables (if any)
--   - UDFs that reference objects
--   - Materialized Views (if any)
--
-- Database: prod_db
-- Schemas: mart_investments_bolt, src_investments_bolt
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;

-- Set context for better visibility
ALTER SESSION SET TIMEZONE = 'UTC';

-- =============================================================================
-- SECTION 1: BASE OBJECTS (Starting Point)
-- =============================================================================
SELECT '=== SECTION 1: BASE MIGRATION OBJECTS ===' AS section;

CREATE OR REPLACE TEMPORARY TABLE base_migration_objects AS
SELECT
    'PROD_DB' AS database_name,
    'SRC_INVESTMENTS_BOLT' AS schema_name,
    'STOCK_METADATA_RAW' AS object_name,
    'TABLE' AS object_type
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'DIM_STOCKS', 'TABLE'
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'DIM_PORTFOLIOS', 'TABLE'
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'FACT_TRANSACTIONS', 'TABLE'
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'FACT_DAILY_POSITIONS', 'TABLE'
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'VW_CURRENT_HOLDINGS', 'VIEW'
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'SP_LOAD_DIM_STOCKS', 'PROCEDURE'
UNION ALL
SELECT 'PROD_DB', 'MART_INVESTMENTS_BOLT', 'SP_CALCULATE_DAILY_POSITIONS', 'PROCEDURE';

SELECT
    schema_name,
    object_name,
    object_type,
    database_name || '.' || schema_name || '.' || object_name AS fully_qualified_name
FROM base_migration_objects
ORDER BY
    CASE object_type
        WHEN 'TABLE' THEN 1
        WHEN 'VIEW' THEN 2
        WHEN 'PROCEDURE' THEN 3
        ELSE 4
    END,
    schema_name,
    object_name;

-- =============================================================================
-- SECTION 2: UPSTREAM DEPENDENCIES (What Base Objects Depend On)
-- =============================================================================
SELECT '=== SECTION 2: UPSTREAM DEPENDENCIES (BASE OBJECTS DEPEND ON THESE) ===' AS section;

-- Recursive CTE to find ALL upstream dependencies
-- This discovers what our base objects reference
WITH RECURSIVE upstream_deps (
    level,
    path,
    referenced_database,
    referenced_schema,
    referenced_object,
    referenced_type,
    referencing_database,
    referencing_schema,
    referencing_object,
    referencing_type,
    referenced_object_id
) AS (
    -- Anchor: Direct dependencies of base objects
    SELECT
        1 AS level,
        od.referencing_object_name || ' --> ' || od.referenced_object_name AS path,
        od.referenced_database_name,
        od.referenced_schema_name,
        od.referenced_object_name,
        od.referenced_object_domain,
        od.referencing_database_name,
        od.referencing_schema_name,
        od.referencing_object_name,
        od.referencing_object_domain,
        od.referenced_object_id
    FROM snowflake.account_usage.object_dependencies od
    INNER JOIN base_migration_objects bmo
        ON od.referencing_database_name = bmo.database_name
        AND od.referencing_schema_name = bmo.schema_name
        AND od.referencing_object_name = bmo.object_name
    WHERE od.referenced_database_name = 'PROD_DB'
        AND od.referenced_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')

    UNION ALL

    -- Recursive: Find what those dependencies depend on
    SELECT
        ud.level + 1,
        ud.path || ' --> ' || od.referenced_object_name,
        od.referenced_database_name,
        od.referenced_schema_name,
        od.referenced_object_name,
        od.referenced_object_domain,
        od.referencing_database_name,
        od.referencing_schema_name,
        od.referencing_object_name,
        od.referencing_object_domain,
        od.referenced_object_id
    FROM snowflake.account_usage.object_dependencies od
    INNER JOIN upstream_deps ud
        ON od.referencing_object_id = ud.referenced_object_id
        AND od.referencing_database_name = ud.referenced_database
        AND od.referencing_schema_name = ud.referenced_schema
    WHERE od.referenced_database_name = 'PROD_DB'
        AND od.referenced_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
        AND ud.level < 10  -- Prevent infinite loops
)
SELECT DISTINCT
    level,
    referenced_schema AS schema_name,
    referenced_object AS object_name,
    referenced_type AS object_type,
    referenced_database || '.' || referenced_schema || '.' || referenced_object AS fully_qualified_name,
    path AS dependency_path
FROM upstream_deps
WHERE NOT EXISTS (
    -- Exclude objects already in base list
    SELECT 1 FROM base_migration_objects bmo
    WHERE bmo.schema_name = upstream_deps.referenced_schema
        AND bmo.object_name = upstream_deps.referenced_object
)
ORDER BY level, schema_name, object_name;

-- Store upstream dependencies for later use
CREATE OR REPLACE TEMPORARY TABLE upstream_dependencies AS
SELECT DISTINCT
    referenced_database AS database_name,
    referenced_schema AS schema_name,
    referenced_object AS object_name,
    referenced_type AS object_type,
    'UPSTREAM' AS dependency_direction
FROM upstream_deps
WHERE NOT EXISTS (
    SELECT 1 FROM base_migration_objects bmo
    WHERE bmo.schema_name = upstream_deps.referenced_schema
        AND bmo.object_name = upstream_deps.referenced_object
);

-- =============================================================================
-- SECTION 3: DOWNSTREAM DEPENDENCIES (What Depends On Base Objects)
-- =============================================================================
SELECT '=== SECTION 3: DOWNSTREAM DEPENDENCIES (THESE DEPEND ON BASE OBJECTS) ===' AS section;

-- Recursive CTE to find ALL downstream dependencies
-- This discovers what references our base objects
WITH RECURSIVE downstream_deps (
    level,
    path,
    referenced_database,
    referenced_schema,
    referenced_object,
    referenced_type,
    referencing_database,
    referencing_schema,
    referencing_object,
    referencing_type,
    referencing_object_id
) AS (
    -- Anchor: Objects that directly reference base objects
    SELECT
        1 AS level,
        od.referenced_object_name || ' <-- ' || od.referencing_object_name AS path,
        od.referenced_database_name,
        od.referenced_schema_name,
        od.referenced_object_name,
        od.referenced_object_domain,
        od.referencing_database_name,
        od.referencing_schema_name,
        od.referencing_object_name,
        od.referencing_object_domain,
        od.referencing_object_id
    FROM snowflake.account_usage.object_dependencies od
    INNER JOIN base_migration_objects bmo
        ON od.referenced_database_name = bmo.database_name
        AND od.referenced_schema_name = bmo.schema_name
        AND od.referenced_object_name = bmo.object_name
    WHERE od.referencing_database_name = 'PROD_DB'
        AND od.referencing_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')

    UNION ALL

    -- Recursive: Find what references those dependencies
    SELECT
        dd.level + 1,
        dd.path || ' <-- ' || od.referencing_object_name,
        od.referenced_database_name,
        od.referenced_schema_name,
        od.referenced_object_name,
        od.referenced_object_domain,
        od.referencing_database_name,
        od.referencing_schema_name,
        od.referencing_object_name,
        od.referencing_object_domain,
        od.referencing_object_id
    FROM snowflake.account_usage.object_dependencies od
    INNER JOIN downstream_deps dd
        ON od.referenced_object_id = dd.referencing_object_id
        AND od.referenced_database_name = dd.referencing_database
        AND od.referenced_schema_name = dd.referencing_schema
    WHERE od.referencing_database_name = 'PROD_DB'
        AND od.referencing_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
        AND dd.level < 10  -- Prevent infinite loops
)
SELECT DISTINCT
    level,
    referencing_schema AS schema_name,
    referencing_object AS object_name,
    referencing_type AS object_type,
    referencing_database || '.' || referencing_schema || '.' || referencing_object AS fully_qualified_name,
    path AS dependency_path
FROM downstream_deps
WHERE NOT EXISTS (
    -- Exclude objects already in base list
    SELECT 1 FROM base_migration_objects bmo
    WHERE bmo.schema_name = downstream_deps.referencing_schema
        AND bmo.object_name = downstream_deps.referencing_object
)
ORDER BY level, schema_name, object_name;

-- Store downstream dependencies for later use
CREATE OR REPLACE TEMPORARY TABLE downstream_dependencies AS
SELECT DISTINCT
    referencing_database AS database_name,
    referencing_schema AS schema_name,
    referencing_object AS object_name,
    referencing_type AS object_type,
    'DOWNSTREAM' AS dependency_direction
FROM downstream_deps
WHERE NOT EXISTS (
    SELECT 1 FROM base_migration_objects bmo
    WHERE bmo.schema_name = downstream_deps.referencing_schema
        AND bmo.object_name = downstream_deps.referencing_object
);

-- =============================================================================
-- SECTION 4: COMPLETE MIGRATION OBJECT LIST
-- =============================================================================
SELECT '=== SECTION 4: COMPLETE MIGRATION OBJECT LIST (BASE + ALL DEPENDENCIES) ===' AS section;

-- Combine all objects into a single comprehensive list
CREATE OR REPLACE TEMPORARY TABLE complete_migration_objects AS
SELECT
    database_name,
    schema_name,
    object_name,
    object_type,
    'BASE' AS object_category,
    1 AS migration_priority
FROM base_migration_objects

UNION ALL

SELECT
    database_name,
    schema_name,
    object_name,
    object_type,
    dependency_direction AS object_category,
    CASE
        WHEN dependency_direction = 'UPSTREAM' THEN 0  -- Migrate first
        WHEN dependency_direction = 'DOWNSTREAM' THEN 2  -- Migrate last
    END AS migration_priority
FROM upstream_dependencies

UNION ALL

SELECT
    database_name,
    schema_name,
    object_name,
    object_type,
    dependency_direction AS object_category,
    CASE
        WHEN dependency_direction = 'UPSTREAM' THEN 0
        WHEN dependency_direction = 'DOWNSTREAM' THEN 2
    END AS migration_priority
FROM downstream_dependencies;

-- Display complete list with migration order
SELECT
    ROW_NUMBER() OVER (ORDER BY
        migration_priority,
        CASE object_type
            WHEN 'TABLE' THEN 1
            WHEN 'VIEW' THEN 2
            WHEN 'MATERIALIZED VIEW' THEN 3
            WHEN 'DYNAMIC TABLE' THEN 4
            WHEN 'PROCEDURE' THEN 5
            WHEN 'FUNCTION' THEN 6
            ELSE 7
        END,
        schema_name,
        object_name
    ) AS migration_sequence,
    object_category,
    schema_name,
    object_name,
    object_type,
    database_name || '.' || schema_name || '.' || object_name AS fully_qualified_name,
    CASE migration_priority
        WHEN 0 THEN 'Phase 1: Upstream Dependencies'
        WHEN 1 THEN 'Phase 2: Base Objects'
        WHEN 2 THEN 'Phase 3: Downstream Dependencies'
    END AS migration_phase
FROM complete_migration_objects
ORDER BY migration_sequence;

-- =============================================================================
-- SECTION 5: DETAILED METADATA FOR ALL OBJECTS
-- =============================================================================
SELECT '=== SECTION 5: DETAILED TABLE METADATA ===' AS section;

-- Tables (including all discovered dependencies)
SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    t.row_count,
    t.bytes,
    ROUND(t.bytes/1024/1024/1024, 2) AS size_gb,
    t.clustering_key,
    t.created,
    t.last_altered,
    CASE
        WHEN cmo.object_category = 'BASE' THEN '✓ BASE'
        WHEN cmo.object_category = 'UPSTREAM' THEN '↑ UPSTREAM'
        WHEN cmo.object_category = 'DOWNSTREAM' THEN '↓ DOWNSTREAM'
    END AS migration_category,
    t.comment
FROM information_schema.tables t
INNER JOIN complete_migration_objects cmo
    ON t.table_schema = cmo.schema_name
    AND t.table_name = cmo.object_name
    AND cmo.object_type = 'TABLE'
WHERE t.table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
    AND t.table_type = 'BASE TABLE'
ORDER BY
    CASE cmo.object_category
        WHEN 'UPSTREAM' THEN 1
        WHEN 'BASE' THEN 2
        WHEN 'DOWNSTREAM' THEN 3
    END,
    t.table_schema,
    t.table_name;

-- =============================================================================
-- SECTION 6: VIEW DETAILS
-- =============================================================================
SELECT '=== SECTION 6: VIEW DETAILS ===' AS section;

SELECT
    v.table_schema,
    v.table_name AS view_name,
    v.is_secure,
    CASE
        WHEN cmo.object_category = 'BASE' THEN '✓ BASE'
        WHEN cmo.object_category = 'UPSTREAM' THEN '↑ UPSTREAM'
        WHEN cmo.object_category = 'DOWNSTREAM' THEN '↓ DOWNSTREAM'
    END AS migration_category,
    v.created,
    v.last_altered,
    LEFT(v.view_definition, 200) AS view_definition_preview,
    v.comment
FROM information_schema.views v
INNER JOIN complete_migration_objects cmo
    ON v.table_schema = cmo.schema_name
    AND v.table_name = cmo.object_name
    AND cmo.object_type = 'VIEW'
WHERE v.table_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
ORDER BY
    CASE cmo.object_category
        WHEN 'UPSTREAM' THEN 1
        WHEN 'BASE' THEN 2
        WHEN 'DOWNSTREAM' THEN 3
    END,
    v.table_schema,
    v.table_name;

-- =============================================================================
-- SECTION 7: STORED PROCEDURE DETAILS
-- =============================================================================
SELECT '=== SECTION 7: STORED PROCEDURE DETAILS ===' AS section;

SELECT
    p.procedure_schema,
    p.procedure_name,
    p.argument_signature,
    p.data_type AS return_type,
    p.procedure_language,
    CASE
        WHEN cmo.object_category = 'BASE' THEN '✓ BASE'
        WHEN cmo.object_category = 'UPSTREAM' THEN '↑ UPSTREAM'
        WHEN cmo.object_category = 'DOWNSTREAM' THEN '↓ DOWNSTREAM'
    END AS migration_category,
    p.created,
    p.last_altered,
    p.comment
FROM information_schema.procedures p
INNER JOIN complete_migration_objects cmo
    ON p.procedure_schema = cmo.schema_name
    AND p.procedure_name = cmo.object_name
    AND cmo.object_type = 'PROCEDURE'
WHERE p.procedure_schema IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
ORDER BY
    CASE cmo.object_category
        WHEN 'UPSTREAM' THEN 1
        WHEN 'BASE' THEN 2
        WHEN 'DOWNSTREAM' THEN 3
    END,
    p.procedure_schema,
    p.procedure_name;

-- =============================================================================
-- SECTION 8: EXTERNAL DEPENDENCIES WARNING
-- =============================================================================
SELECT '=== SECTION 8: EXTERNAL DEPENDENCIES (OUTSIDE MIGRATION SCOPE) ===' AS section;

-- Find any dependencies that reference objects OUTSIDE our schemas
SELECT DISTINCT
    od.referencing_database_name || '.' || od.referencing_schema_name || '.' || od.referencing_object_name AS dependent_object,
    od.referencing_object_domain AS dependent_type,
    od.referenced_database_name || '.' || od.referenced_schema_name || '.' || od.referenced_object_name AS external_dependency,
    od.referenced_object_domain AS external_type,
    '⚠️ REQUIRES MANUAL REVIEW' AS warning
FROM snowflake.account_usage.object_dependencies od
INNER JOIN complete_migration_objects cmo
    ON od.referencing_database_name = cmo.database_name
    AND od.referencing_schema_name = cmo.schema_name
    AND od.referencing_object_name = cmo.object_name
WHERE NOT (
    od.referenced_database_name = 'PROD_DB'
    AND od.referenced_schema_name IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
)
ORDER BY dependent_object, external_dependency;

-- =============================================================================
-- SECTION 9: ROW COUNTS FOR ALL TABLES
-- =============================================================================
SELECT '=== SECTION 9: ROW COUNTS FOR VALIDATION ===' AS section;

-- Generate dynamic SQL to count all tables
SELECT
    'SELECT ''' || schema_name || '.' || object_name || ''' AS table_name, COUNT(*) AS row_count FROM '
    || database_name || '.' || schema_name || '.' || object_name || ' UNION ALL'
FROM complete_migration_objects
WHERE object_type = 'TABLE'
ORDER BY migration_priority, schema_name, object_name;

-- Note: Copy the output above and execute manually to get actual row counts

-- =============================================================================
-- SECTION 10: MIGRATION SUMMARY
-- =============================================================================
SELECT '=== SECTION 10: MIGRATION SUMMARY ===' AS section;

SELECT
    object_category,
    object_type,
    COUNT(*) AS object_count,
    STRING_AGG(object_name, ', ') AS object_names
FROM complete_migration_objects
GROUP BY object_category, object_type
ORDER BY
    CASE object_category
        WHEN 'UPSTREAM' THEN 1
        WHEN 'BASE' THEN 2
        WHEN 'DOWNSTREAM' THEN 3
    END,
    object_type;

-- Final summary
SELECT
    'Total Objects to Migrate' AS metric,
    COUNT(DISTINCT object_name) AS count
FROM complete_migration_objects

UNION ALL

SELECT
    'Base Objects' AS metric,
    COUNT(*) AS count
FROM complete_migration_objects
WHERE object_category = 'BASE'

UNION ALL

SELECT
    'Upstream Dependencies' AS metric,
    COUNT(*) AS count
FROM complete_migration_objects
WHERE object_category = 'UPSTREAM'

UNION ALL

SELECT
    'Downstream Dependencies' AS metric,
    COUNT(*) AS count
FROM complete_migration_objects
WHERE object_category = 'DOWNSTREAM'

UNION ALL

SELECT
    'Tables' AS metric,
    COUNT(*) AS count
FROM complete_migration_objects
WHERE object_type = 'TABLE'

UNION ALL

SELECT
    'Views' AS metric,
    COUNT(*) AS count
FROM complete_migration_objects
WHERE object_type = 'VIEW'

UNION ALL

SELECT
    'Procedures' AS metric,
    COUNT(*) AS count
FROM complete_migration_objects
WHERE object_type = 'PROCEDURE';

-- =============================================================================
-- SECTION 11: EXPORT COMPLETE OBJECT LIST FOR NEXT PHASES
-- =============================================================================
SELECT '=== SECTION 11: COMPLETE OBJECT LIST FOR MIGRATION SCRIPTS ===' AS section;

-- This output will be used to generate data share, DDL extraction, and migration scripts
SELECT
    ROW_NUMBER() OVER (ORDER BY
        migration_priority,
        CASE object_type
            WHEN 'TABLE' THEN 1
            WHEN 'VIEW' THEN 2
            WHEN 'PROCEDURE' THEN 3
            ELSE 4
        END,
        schema_name,
        object_name
    ) AS seq,
    database_name,
    schema_name,
    object_name,
    object_type,
    object_category,
    database_name || '.' || schema_name || '.' || object_name AS fully_qualified_name,
    'prod_db' AS source_db,
    'dev_db' AS target_db,
    REPLACE(database_name || '.' || schema_name || '.' || object_name, 'PROD_DB', 'DEV_DB') AS target_fully_qualified_name
FROM complete_migration_objects
ORDER BY seq;

-- =============================================================================
-- SCRIPT COMPLETE
-- =============================================================================
SELECT '=== ✅ DISCOVERY COMPLETE ===' AS status,
       CURRENT_TIMESTAMP() AS completed_at,
       (SELECT COUNT(*) FROM complete_migration_objects) AS total_objects_discovered;
