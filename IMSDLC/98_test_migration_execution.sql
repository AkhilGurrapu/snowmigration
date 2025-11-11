-- ============================================
-- IMSDLC (TARGET) - Test Migration Execution
-- ============================================
-- Purpose: Execute complete migration on target side using sp_execute_full_migration
-- This script tests the full automation flow

USE ROLE ACCOUNTADMIN;

-- ============================================
-- Step 1: Create database from share
-- ============================================

-- Drop if exists (for clean testing)
DROP DATABASE IF EXISTS shared_prod_db;

-- Create from share
CREATE DATABASE shared_prod_db
FROM SHARE NFMYIZV.IMCUST.MIGRATION_SHARE_TEST_E2E;

-- Grant privileges to ACCOUNTADMIN
GRANT IMPORTED PRIVILEGES ON DATABASE shared_prod_db TO ROLE ACCOUNTADMIN;

-- ============================================
-- Step 2: Verify shared database and metadata
-- ============================================

-- Verify we can see the migration metadata
SELECT
    migration_id,
    source_database,
    source_schema,
    target_database,
    object_list,
    status,
    created_ts
FROM shared_prod_db.ADMIN_SCHEMA.migration_config
ORDER BY migration_id DESC;

-- Verify shared objects are accessible
SELECT
    source_schema,
    object_name,
    object_type,
    dependency_level
FROM shared_prod_db.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = (SELECT MAX(migration_id) FROM shared_prod_db.ADMIN_SCHEMA.migration_config)
ORDER BY dependency_level DESC, source_schema, object_name;

-- Count objects by schema and type
SELECT
    source_schema,
    object_type,
    COUNT(*) as count
FROM shared_prod_db.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = (SELECT MAX(migration_id) FROM shared_prod_db.ADMIN_SCHEMA.migration_config)
GROUP BY source_schema, object_type
ORDER BY source_schema, object_type;

-- Verify we can access shared source data (sample query)
SELECT 'stock_master' as source_table, COUNT(*) as row_count
FROM shared_prod_db.SRC_INVESTMENTS_BOLT.stock_master
UNION ALL
SELECT 'transactions_raw', COUNT(*)
FROM shared_prod_db.SRC_INVESTMENTS_BOLT.transactions_raw
UNION ALL
SELECT 'fact_transactions', COUNT(*)
FROM shared_prod_db.MART_INVESTMENTS_BOLT.fact_transactions;

-- ============================================
-- Step 3: Execute Complete Migration
-- ============================================

-- This single call will:
-- 1. Read DDL scripts from shared database
-- 2. Execute DDLs in dependency order
-- 3. Execute CTAS scripts to copy data
-- 4. Log all results in migration_execution_log

CALL dev_db.mart_investments_bolt.sp_execute_full_migration(
    (SELECT MAX(migration_id) FROM shared_prod_db.ADMIN_SCHEMA.migration_config),  -- migration_id from source
    'shared_prod_db',                                                               -- shared database name
    'ADMIN_SCHEMA',                                                                 -- schema where metadata tables are
    TRUE                                                                            -- validate before CTAS
);

-- ============================================
-- Step 4: View Execution Results
-- ============================================

-- Summary of execution
SELECT
    execution_phase,
    script_type,
    status,
    COUNT(*) as count,
    ROUND(AVG(execution_time_ms), 2) as avg_time_ms,
    ROUND(SUM(execution_time_ms), 2) as total_time_ms
FROM dev_db.mart_investments_bolt.migration_execution_log
WHERE migration_id = (SELECT MAX(migration_id) FROM shared_prod_db.ADMIN_SCHEMA.migration_config)
GROUP BY execution_phase, script_type, status
ORDER BY execution_phase, script_type, status;

-- Detailed execution log
SELECT
    log_id,
    execution_phase,
    object_name,
    script_type,
    status,
    error_message,
    execution_time_ms,
    created_ts
FROM dev_db.mart_investments_bolt.migration_execution_log
WHERE migration_id = (SELECT MAX(migration_id) FROM shared_prod_db.ADMIN_SCHEMA.migration_config)
ORDER BY log_id;

-- View only failures (if any)
SELECT
    execution_phase,
    object_name,
    script_type,
    error_message,
    sql_statement
FROM dev_db.mart_investments_bolt.migration_execution_log
WHERE migration_id = (SELECT MAX(migration_id) FROM shared_prod_db.ADMIN_SCHEMA.migration_config)
  AND status = 'FAILED'
ORDER BY log_id;

-- ============================================
-- Step 5: Validate Migration Results
-- ============================================

-- Check that objects were created in target
SHOW TABLES IN SCHEMA DEV_DB.SRC_INVESTMENTS_BOLT;
SHOW TABLES IN SCHEMA DEV_DB.MART_INVESTMENTS_BOLT;
SHOW VIEWS IN SCHEMA DEV_DB.MART_INVESTMENTS_BOLT;

-- Row count comparison: Source vs Target (SRC schema)
SELECT 'stock_master' as table_name,
       'SRC_INVESTMENTS_BOLT' as schema_name,
       (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.stock_master) as source_count,
       (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.stock_master) as target_count,
       CASE
           WHEN (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.stock_master) =
                (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.stock_master)
           THEN '✓ MATCH'
           ELSE '✗ MISMATCH'
       END as validation_status
UNION ALL
SELECT 'transactions_raw', 'SRC_INVESTMENTS_BOLT',
       (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.transactions_raw),
       (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.transactions_raw),
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.transactions_raw) =
                 (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.transactions_raw)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT 'broker_master', 'SRC_INVESTMENTS_BOLT',
       (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.broker_master),
       (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.broker_master),
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.broker_master) =
                 (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.broker_master)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT 'stock_prices_raw', 'SRC_INVESTMENTS_BOLT',
       (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.stock_prices_raw),
       (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.stock_prices_raw),
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.SRC_INVESTMENTS_BOLT.stock_prices_raw) =
                 (SELECT COUNT(*) FROM dev_db.SRC_INVESTMENTS_BOLT.stock_prices_raw)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END;

-- Row count comparison: Source vs Target (MART schema)
SELECT 'dim_stocks' as table_name,
       'MART_INVESTMENTS_BOLT' as schema_name,
       (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.dim_stocks) as source_count,
       (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.dim_stocks) as target_count,
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.dim_stocks) =
                 (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.dim_stocks)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END as validation_status
UNION ALL
SELECT 'dim_brokers', 'MART_INVESTMENTS_BOLT',
       (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.dim_brokers),
       (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.dim_brokers),
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.dim_brokers) =
                 (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.dim_brokers)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT 'fact_transactions', 'MART_INVESTMENTS_BOLT',
       (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.fact_transactions),
       (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.fact_transactions),
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.fact_transactions) =
                 (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.fact_transactions)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT 'daily_stock_performance', 'MART_INVESTMENTS_BOLT',
       (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.daily_stock_performance),
       (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.daily_stock_performance),
       CASE WHEN (SELECT COUNT(*) FROM shared_prod_db.MART_INVESTMENTS_BOLT.daily_stock_performance) =
                 (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.daily_stock_performance)
            THEN '✓ MATCH' ELSE '✗ MISMATCH' END;

-- Validate VIEW is working (should return data)
SELECT 'vw_transaction_analysis' as view_name,
       COUNT(*) as row_count,
       MIN(transaction_date) as earliest_transaction,
       MAX(transaction_date) as latest_transaction
FROM dev_db.MART_INVESTMENTS_BOLT.vw_transaction_analysis;

-- Sample data verification
SELECT
    transaction_date,
    transaction_type,
    ticker,
    company_name,
    broker_name,
    quantity,
    total_amount
FROM dev_db.MART_INVESTMENTS_BOLT.vw_transaction_analysis
ORDER BY transaction_date, ticker
LIMIT 10;

-- ============================================
-- Expected Results Summary
-- ============================================

-- Expected execution counts:
-- - DDL executions: ~10 (all objects)
-- - CTAS executions: ~9 (only TABLEs, VIEW skipped)
-- - All statuses should be 'SUCCESS'
-- - Row counts should match between source and target

-- Expected objects in DEV_DB:
-- SRC_INVESTMENTS_BOLT:
--   - stock_master (TABLE)
--   - transactions_raw (TABLE)
--   - broker_master (TABLE)
--   - stock_prices_raw (TABLE)
--
-- MART_INVESTMENTS_BOLT:
--   - dim_stocks (TABLE)
--   - dim_brokers (TABLE)
--   - fact_transactions (TABLE)
--   - daily_stock_performance (TABLE)
--   - vw_transaction_analysis (VIEW)

-- Validation checks:
-- ✓ All row counts match between source and target
-- ✓ VIEW is functional and returns data
-- ✓ Cross-schema references work correctly
-- ✓ No failed executions in log
