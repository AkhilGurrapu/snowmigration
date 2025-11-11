-- ============================================
-- IMCUST (SOURCE) - Example Execution
-- ============================================
-- Purpose: Example of how to use the migration automation
-- Modify the parameters according to your needs

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

-- Example: Execute migration for specific objects
-- NOTE: Schema mapping is AUTOMATIC based on source schema from GET_LINEAGE
CALL sp_orchestrate_migration(
    'PROD_DB',                                    -- source database
    'MART_INVESTMENTS_BOLT',                      -- source schema
    'DEV_DB',                                     -- target database
    ARRAY_CONSTRUCT('TABLE1', 'TABLE2', 'VIEW1'), -- objects to migrate (replace with your object names)
    'MIGRATION_SHARE_001',                        -- share name
    'IMSDLC'                                      -- target account identifier (e.g., 'IMSDLC', 'ORG123.ACCT456')
);

-- View migration status
SELECT
    migration_id,
    source_database,
    source_schema,
    target_database,
    target_schema,
    object_list,
    status,
    created_ts
FROM migration_config
ORDER BY migration_id DESC;

-- View generated DDL scripts
SELECT
    migration_id,
    object_name,
    object_type,
    dependency_level,
    target_ddl
FROM migration_ddl_scripts
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY dependency_level, object_name;

-- View CTAS scripts
SELECT
    migration_id,
    object_name,
    ctas_script,
    execution_order
FROM migration_ctas_scripts
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY execution_order;

-- View all dependency objects that were shared
SELECT
    migration_id,
    object_name,
    object_type,
    fully_qualified_name
FROM migration_share_objects
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY object_name;

-- Verify share was created
SHOW SHARES LIKE 'MIGRATION_SHARE_001';

-- Verify database role was created (format: <schema>_VIEWER)
SHOW DATABASE ROLES IN DATABASE prod_db;

-- Verify grants to share
SHOW GRANTS TO SHARE MIGRATION_SHARE_001;
