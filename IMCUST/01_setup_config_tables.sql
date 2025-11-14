-- ============================================
-- IMCUST (SOURCE) - Setup Configuration Tables
-- ============================================
-- Purpose: Create tables to store migration metadata
-- Run this first to set up the infrastructure

USE ROLE ACCOUNTADMIN;

-- Table to store migration requests and track status
CREATE OR REPLACE TABLE PROD_DB.ADMIN_SCHEMA.migration_config (
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
CREATE OR REPLACE TABLE PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts (
    migration_id NUMBER,
    source_database VARCHAR,
    source_schema VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    dependency_level NUMBER,
    source_ddl VARCHAR,
    target_ddl VARCHAR,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to store CTAS scripts for data migration
CREATE OR REPLACE TABLE PROD_DB.ADMIN_SCHEMA.migration_ctas_scripts (
    migration_id NUMBER,
    source_database VARCHAR,
    source_schema VARCHAR,
    object_name VARCHAR,
    ctas_script VARCHAR,
    execution_order NUMBER,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to store dependency objects that need to be shared
CREATE OR REPLACE TABLE PROD_DB.ADMIN_SCHEMA.migration_share_objects (
    migration_id NUMBER,
    source_database VARCHAR,
    source_schema VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    fully_qualified_name VARCHAR,
    dependency_level NUMBER,  -- Distance from GET_LINEAGE (0=requested object, 1+=dependencies)
    object_classification VARCHAR,  -- NEW: BASE_TABLE, DERIVED_TABLE, VIEW
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- NEW: Table to store captured transformation SQL from query history and metadata
CREATE OR REPLACE TABLE PROD_DB.ADMIN_SCHEMA.migration_transformation_sql (
    migration_id NUMBER,
    source_database VARCHAR,
    source_schema VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    transformation_sql VARCHAR,  -- Original INSERT/MERGE/CTAS SQL
    capture_method VARCHAR,  -- QUERY_HISTORY, ACCESS_HISTORY, COMMENT, TAG, NONE
    query_id VARCHAR,  -- Reference to source query (if from QUERY_HISTORY)
    confidence_score NUMBER,  -- 1.0=exact match, 0.5=reconstructed, 0.0=fallback
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- NEW: Table to store hybrid migration scripts (lineage-preserving approach)
CREATE OR REPLACE TABLE PROD_DB.ADMIN_SCHEMA.migration_hybrid_scripts (
    migration_id NUMBER,
    source_database VARCHAR,
    source_schema VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    object_classification VARCHAR,
    migration_strategy VARCHAR,  -- CTAS_FROM_SHARED, INSERT_WITH_TRANSFORMATION, VIEW_ONLY
    migration_script VARCHAR,  -- The actual SQL to execute
    execution_order NUMBER,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify tables created
SHOW TABLES LIKE 'migration_%' in schema PROD_DB.ADMIN_SCHEMA;
