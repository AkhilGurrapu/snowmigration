-- Test cross-schema dependency handling
USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

-- Execute migration
CALL sp_orchestrate_migration(
    'PROD_DB',
    'MART_INVESTMENTS_BOLT',
    'DEV_DB',
    'MART_INVESTMENTS_BOLT',
    ARRAY_CONSTRUCT('VW_TRANSACTION_ANALYSIS'),
    'MIGRATION_SHARE_CROSS_SCHEMA',
    'IMSDLC'
);

-- View dependencies captured with schema information
SELECT
    migration_id,
    source_database,
    source_schema,
    object_name,
    object_type,
    dependency_level
FROM migration_share_objects
WHERE migration_id = (SELECT MAX(migration_id) FROM migration_config)
ORDER BY dependency_level DESC, source_schema, object_name;

-- View generated CTAS scripts with schema preservation
SELECT
    migration_id,
    source_schema,
    object_name,
    LEFT(ctas_script, 200) as ctas_preview
FROM migration_ctas_scripts
WHERE migration_id = (SELECT MAX(migration_id) FROM migration_config)
ORDER BY execution_order;
