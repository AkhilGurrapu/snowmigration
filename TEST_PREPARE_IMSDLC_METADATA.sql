-- ============================================
-- PREPARE MIGRATION METADATA FOR IMSDLC
-- ============================================
-- Run this on IMCUST to prepare metadata for IMSDLC
-- Migration ID: 701
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

-- Show all metadata that needs to be copied
SELECT '=== MIGRATION 701 METADATA SUMMARY ===' as info;

SELECT 'DDL Scripts Count:' as metric, COUNT(*) as value
FROM migration_ddl_scripts WHERE migration_id = 701
UNION ALL
SELECT 'CTAS Scripts Count:', COUNT(*) FROM migration_ctas_scripts WHERE migration_id = 701
UNION ALL
SELECT 'Objects Count:', COUNT(*) FROM migration_share_objects WHERE migration_id = 701;

-- List all objects
SELECT '=== ALL OBJECTS ===' as info;
SELECT 
    source_schema,
    object_name,
    object_type,
    dependency_level
FROM migration_share_objects
WHERE migration_id = 701
ORDER BY dependency_level, source_schema, object_name;

