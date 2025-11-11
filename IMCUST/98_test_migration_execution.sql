-- ============================================
-- IMCUST (SOURCE) - Test Migration Execution
-- ============================================
-- Purpose: Execute test migration for 2 tables and 1 view
-- Tests the complete migration flow with cross-schema dependencies

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;
USE SCHEMA ADMIN_SCHEMA;

-- ============================================
-- Test Migration: Migrate vw_transaction_analysis (VIEW)
-- This view depends on multiple tables across both schemas
-- ============================================

-- Objects being requested for migration:
-- 1. vw_transaction_analysis (VIEW) - from MART_INVESTMENTS_BOLT
-- 2. fact_transactions (TABLE) - from MART_INVESTMENTS_BOLT
-- 3. dim_stocks (TABLE) - from MART_INVESTMENTS_BOLT

-- Expected upstream dependencies to be discovered:
-- - dim_brokers (TABLE) - from MART_INVESTMENTS_BOLT
-- - daily_stock_performance (TABLE) - from MART_INVESTMENTS_BOLT
-- - stock_master (TABLE) - from SRC_INVESTMENTS_BOLT
-- - transactions_raw (TABLE) - from SRC_INVESTMENTS_BOLT
-- - broker_master (TABLE) - from SRC_INVESTMENTS_BOLT
-- - stock_prices_raw (TABLE) - from SRC_INVESTMENTS_BOLT

CALL PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
    'PROD_DB',                                              -- source database
    'MART_INVESTMENTS_BOLT',                                -- source schema (starting point)
    'DEV_DB',                                               -- target database
    ARRAY_CONSTRUCT(
        'fact_transactions',       -- Table 1: Fact table with dependencies
        'dim_stocks',              -- Table 2: Dimension table with dependencies
        'vw_transaction_analysis'  -- View 1: View with complex dependencies
    ),
    'MIGRATION_SHARE_TEST_E2E',                             -- share name
    'IMSDLC'                                                -- target account
);

-- ============================================
-- View Migration Results
-- ============================================

-- Get the migration_id (should be 1 if this is first run)
SELECT
    migration_id,
    source_database,
    source_schema,
    target_database,
    object_list,
    status,
    created_ts
FROM PROD_DB.ADMIN_SCHEMA.migration_config
ORDER BY migration_id DESC
LIMIT 1;

-- View all objects discovered (requested + dependencies)
SELECT
    migration_id,
    source_database,
    source_schema,
    object_name,
    object_type,
    dependency_level,
    fully_qualified_name
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = (SELECT MAX(migration_id) FROM PROD_DB.ADMIN_SCHEMA.migration_config)
ORDER BY dependency_level DESC, source_schema, object_name;

-- Count objects by type and schema
SELECT
    source_schema,
    object_type,
    COUNT(*) as object_count
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = (SELECT MAX(migration_id) FROM PROD_DB.ADMIN_SCHEMA.migration_config)
GROUP BY source_schema, object_type
ORDER BY source_schema, object_type;

-- View generated DDL scripts
SELECT
    migration_id,
    source_database,
    source_schema,
    object_name,
    object_type,
    dependency_level,
    LENGTH(target_ddl) as ddl_length_chars
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = (SELECT MAX(migration_id) FROM PROD_DB.ADMIN_SCHEMA.migration_config)
ORDER BY dependency_level DESC, source_schema, object_name;

-- View CTAS scripts (should only have TABLEs, not VIEWs)
SELECT
    migration_id,
    source_database,
    source_schema,
    object_name,
    execution_order,
    LENGTH(ctas_script) as script_length_chars
FROM PROD_DB.ADMIN_SCHEMA.migration_ctas_scripts
WHERE migration_id = (SELECT MAX(migration_id) FROM PROD_DB.ADMIN_SCHEMA.migration_config)
ORDER BY execution_order;

-- Verify share was created
SHOW SHARES LIKE 'MIGRATION_SHARE_TEST_E2E';

-- Verify database role was created
SHOW DATABASE ROLES IN DATABASE PROD_DB;

-- Verify grants to share
SHOW GRANTS TO SHARE MIGRATION_SHARE_TEST_E2E;

-- ============================================
-- Expected Results Summary
-- ============================================

-- Expected counts:
-- - Total objects: ~10 (3 requested + ~7 dependencies)
-- - DDL scripts: ~10 (all objects)
-- - CTAS scripts: ~9 (only TABLEs, not VIEW)
-- - Schemas involved: SRC_INVESTMENTS_BOLT, MART_INVESTMENTS_BOLT
-- - Object types: TABLE, VIEW

-- Sample expected objects:
-- Level 0 (requested):
--   - MART_INVESTMENTS_BOLT.fact_transactions (TABLE)
--   - MART_INVESTMENTS_BOLT.dim_stocks (TABLE)
--   - MART_INVESTMENTS_BOLT.vw_transaction_analysis (VIEW)
--
-- Level 1+ (dependencies):
--   - MART_INVESTMENTS_BOLT.dim_brokers (TABLE)
--   - MART_INVESTMENTS_BOLT.daily_stock_performance (TABLE)
--   - SRC_INVESTMENTS_BOLT.stock_master (TABLE)
--   - SRC_INVESTMENTS_BOLT.transactions_raw (TABLE)
--   - SRC_INVESTMENTS_BOLT.broker_master (TABLE)
--   - SRC_INVESTMENTS_BOLT.stock_prices_raw (TABLE)
