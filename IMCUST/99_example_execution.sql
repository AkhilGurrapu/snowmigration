-- ============================================
-- IMCUST (SOURCE) - Example Execution
-- ============================================
-- Purpose: Example of how to use the migration automation
-- Modify the parameters according to your needs

USE ROLE ACCOUNTADMIN;

-- Example: Execute migration for specific objects
-- NOTE: Schema mapping is AUTOMATIC based on source schema from GET_LINEAGE
CALL PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
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
FROM PROD_DB.ADMIN_SCHEMA.migration_config
ORDER BY migration_id DESC;

-- View generated DDL scripts
SELECT
    migration_id,
    object_name,
    object_type,
    dependency_level,
    target_ddl
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY dependency_level, object_name;

-- View CTAS scripts
SELECT
    migration_id,
    object_name,
    ctas_script,
    execution_order
FROM PROD_DB.ADMIN_SCHEMA.migration_ctas_scripts
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY execution_order;

-- View all dependency objects that were shared
SELECT
    migration_id,
    object_name,
    object_type,
    fully_qualified_name
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = 1  -- Replace with your migration_id
ORDER BY object_name;

-- Verify share was created
SHOW SHARES LIKE 'MIGRATION_SHARE_001';

-- Verify database role was created (format: <schema>_VIEWER)
SHOW DATABASE ROLES IN DATABASE prod_db;

-- Verify grants to share
SHOW GRANTS TO SHARE MIGRATION_SHARE_001;


export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat) && snow sql -q "CALL PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
    'PROD_DB',
    'MART_INVESTMENTS_BOLT',
    'ADMIN_SCHEMA',
    'DEV_DB',
    ARRAY_CONSTRUCT('dim_stocks', 'fact_transactions', 'vw_transaction_analysis'),
    'IMCUST_TO_IMSDLC_SHARE',
    'IMSDLC'
);" -c imcust


export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat) && snow sql -q "SELECT object_name, object_type, dependency_level FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts WHERE migration_id = 2 ORDER BY dependency_level DESC, object_name;" -c imcust