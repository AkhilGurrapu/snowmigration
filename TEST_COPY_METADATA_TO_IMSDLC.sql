-- ============================================
-- COPY MIGRATION METADATA TO IMSDLC (Simulate Share)
-- ============================================
-- Purpose: Copy migration 701 metadata from IMCUST to IMSDLC
--          This simulates what would come from the data share
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE shared_prod_db;
USE SCHEMA mart_investments_bolt;

-- ============================================
-- STEP 1: Copy DDL Scripts
-- ============================================
-- Note: In real scenario, these come FROM SHARE
-- For testing, we manually insert them

INSERT INTO migration_ddl_scripts (
    migration_id,
    source_database,
    source_schema,
    object_name,
    object_type,
    dependency_level,
    source_ddl,
    target_ddl
)
SELECT 
    migration_id,
    source_database,
    source_schema,
    object_name,
    object_type,
    dependency_level,
    source_ddl,
    target_ddl
FROM prod_db.mart_investments_bolt.migration_ddl_scripts
WHERE migration_id = 701;

-- ============================================
-- STEP 2: Copy CTAS Scripts
-- ============================================
INSERT INTO migration_ctas_scripts (
    migration_id,
    source_database,
    source_schema,
    object_name,
    ctas_script,
    execution_order
)
SELECT 
    migration_id,
    source_database,
    source_schema,
    object_name,
    ctas_script,
    execution_order
FROM prod_db.mart_investments_bolt.migration_ctas_scripts
WHERE migration_id = 701;

-- ============================================
-- STEP 3: Copy Source Data Tables
-- ============================================
-- Copy DIM_MEMBERS (dependency)
CREATE OR REPLACE TABLE src_investments_bolt.dim_members AS
SELECT * FROM prod_db.src_investments_bolt.dim_members;

-- Copy FACT_MEMBER_ACTIVITY (dependency - uppercase version)
CREATE OR REPLACE TABLE mart_investments_bolt.fact_member_activity AS
SELECT * FROM prod_db.mart_investments_bolt.fact_member_activity;

-- Copy FACT_LIBRARY_TRANSACTIONS (requested)
CREATE OR REPLACE TABLE mart_investments_bolt.fact_library_transactions AS
SELECT * FROM prod_db.mart_investments_bolt.fact_library_transactions;

-- ============================================
-- VALIDATION
-- ============================================
SELECT '=== METADATA COPYED TO IMSDLC ===' as status;

SELECT 'DDL Scripts:' as section, COUNT(*) as count
FROM migration_ddl_scripts
WHERE migration_id = 701;

SELECT 'CTAS Scripts:' as section, COUNT(*) as count
FROM migration_ctas_scripts
WHERE migration_id = 701;

SELECT 'Source Data - DIM_MEMBERS:' as section, COUNT(*) as row_count
FROM src_investments_bolt.dim_members;

SELECT 'Source Data - FACT_MEMBER_ACTIVITY:' as section, COUNT(*) as row_count
FROM mart_investments_bolt.fact_member_activity;

SELECT 'Source Data - FACT_LIBRARY_TRANSACTIONS:' as section, COUNT(*) as row_count
FROM mart_investments_bolt.fact_library_transactions;

