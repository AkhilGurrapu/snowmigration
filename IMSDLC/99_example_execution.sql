-- ============================================
-- IMSDLC (TARGET) - Example Execution
-- ============================================
-- Purpose: Example of how to execute migration on target side
-- Modify the parameters according to your needs

USE ROLE ACCOUNTADMIN;

-- Step 1: Create database from share
-- Replace with your actual share name from source account
CREATE DATABASE IF NOT EXISTS shared_prod_db
FROM SHARE IMCUST.MIGRATION_SHARE_001;

-- Grant privileges to ACCOUNTADMIN role
GRANT IMPORTED PRIVILEGES ON DATABASE shared_prod_db TO ROLE ACCOUNTADMIN;

-- Step 2: Verify you can see the migration metadata
SELECT
    migration_id,
    source_database,
    target_database,
    status,
    created_ts
FROM shared_prod_db.mart_investments_bolt.migration_config
ORDER BY migration_id DESC;

-- Step 3: Execute complete migration (DDL + CTAS)
-- Replace migration_id with the actual migration ID from source
CALL dev_db.mart_investments_bolt.sp_execute_full_migration(
    1,                      -- migration_id from source
    'shared_prod_db',       -- shared database name
    TRUE                    -- validate before CTAS
);

-- Step 4: Review execution results
SELECT
    execution_phase,
    object_name,
    script_type,
    status,
    error_message,
    execution_time_ms,
    created_ts
FROM dev_db.mart_investments_bolt.migration_execution_log
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY log_id;

-- Step 5: Get summary statistics
SELECT
    execution_phase,
    script_type,
    status,
    COUNT(*) as count,
    AVG(execution_time_ms) as avg_time_ms,
    SUM(execution_time_ms) as total_time_ms
FROM dev_db.mart_investments_bolt.migration_execution_log
WHERE migration_id = 1  -- Replace with your migration_id
GROUP BY execution_phase, script_type, status
ORDER BY execution_phase, script_type, status;

-- Step 6: View only failures (if any)
SELECT
    execution_phase,
    object_name,
    script_type,
    error_message,
    created_ts
FROM dev_db.mart_investments_bolt.migration_execution_log
WHERE migration_id = 1  -- Replace with your migration_id
  AND status = 'FAILED'
ORDER BY log_id;

-- Optional: Generate row count validation queries
SELECT
    'SELECT ''' || object_name || ''' as table_name, ' ||
    '(SELECT COUNT(*) FROM shared_prod_db.mart_investments_bolt.' || object_name || ') as source_count, ' ||
    '(SELECT COUNT(*) FROM dev_db.mart_investments_bolt.' || object_name || ') as target_count;'
    as validation_query
FROM shared_prod_db.mart_investments_bolt.migration_ctas_scripts
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY execution_order;
