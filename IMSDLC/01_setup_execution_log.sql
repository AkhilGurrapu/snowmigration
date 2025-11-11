-- ============================================
-- IMSDLC (TARGET) - Setup Execution Log Table
-- ============================================
-- Purpose: Create table to track migration execution on target side
-- Run this first to set up the infrastructure

USE ROLE ACCOUNTADMIN;

USE DATABASE dev_db;
USE SCHEMA admin_schema;

-- Create execution log table to track DDL and CTAS execution
CREATE OR REPLACE TABLE migration_execution_log (
    log_id NUMBER AUTOINCREMENT,
    migration_id NUMBER,
    execution_phase VARCHAR,
    object_name VARCHAR,
    script_type VARCHAR,
    sql_statement VARCHAR,
    status VARCHAR,
    error_message VARCHAR,
    execution_time_ms NUMBER,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (log_id)
);

-- Verify table created
SHOW TABLES LIKE 'migration_%';
