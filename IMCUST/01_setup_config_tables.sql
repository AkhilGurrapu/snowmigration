-- ============================================
-- IMCUST (SOURCE) - Setup Configuration Tables
-- ============================================
-- Purpose: Create tables to store migration metadata
-- Run this first to set up the infrastructure

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

-- Table to store migration requests and track status
CREATE OR REPLACE TABLE migration_config (
    migration_id NUMBER AUTOINCREMENT,
    source_database VARCHAR,
    source_schema VARCHAR,
    target_database VARCHAR,
    target_schema VARCHAR,
    object_list ARRAY,
    status VARCHAR DEFAULT 'PENDING',
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (migration_id)
);

-- Table to store DDL scripts for each object
CREATE OR REPLACE TABLE migration_ddl_scripts (
    migration_id NUMBER,
    object_name VARCHAR,
    object_type VARCHAR,
    dependency_level NUMBER,
    source_ddl VARCHAR,
    target_ddl VARCHAR,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to store CTAS scripts for data migration
CREATE OR REPLACE TABLE migration_ctas_scripts (
    migration_id NUMBER,
    object_name VARCHAR,
    ctas_script VARCHAR,
    execution_order NUMBER,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to store dependency objects that need to be shared
CREATE OR REPLACE TABLE migration_share_objects (
    migration_id NUMBER,
    object_name VARCHAR,
    object_type VARCHAR,
    fully_qualified_name VARCHAR,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify tables created
SHOW TABLES LIKE 'migration_%';
