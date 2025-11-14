-- ============================================
-- IMCUST (SOURCE) - Classify Migration Objects
-- ============================================
-- Purpose: Classify each discovered object as BASE_TABLE, DERIVED_TABLE, or VIEW
-- This determines the migration strategy for lineage preservation
--
-- Classification Logic:
--   VIEW: Object type is VIEW (no classification needed, uses DDL)
--   BASE_TABLE: Table with dependency_level > 0 (is a dependency of another object) AND has no own dependencies
--   DERIVED_TABLE: Table with dependency_level = 0 (requested object) OR has upstream dependencies
--
-- Author: Enhanced Migration Framework v2.0
-- ============================================

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_classify_migration_objects(
    p_migration_id FLOAT,
    p_database VARCHAR,
    p_admin_schema VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        var migration_id = P_MIGRATION_ID;
        var database = P_DATABASE;
        var admin_schema = P_ADMIN_SCHEMA;

        // Step 1: Classify VIEWs (simple - based on object_type)
        var view_sql = `
            UPDATE ${database}.${admin_schema}.migration_share_objects
            SET object_classification = 'VIEW'
            WHERE migration_id = ${migration_id}
              AND object_type = 'VIEW'
        `;
        snowflake.execute({sqlText: view_sql});

        // Step 2: Classify BASE_TABLEs
        // Definition: Tables that have no upstream dependencies themselves
        // AND have dependency_level > 0 (meaning they are dependencies of other objects)
        // OR tables with dependency_level = max(dependency_level) for the migration
        var base_table_sql = `
            UPDATE ${database}.${admin_schema}.migration_share_objects t1
            SET object_classification = 'BASE_TABLE'
            WHERE migration_id = ${migration_id}
              AND object_type = 'TABLE'
              AND dependency_level = (
                  SELECT MAX(dependency_level)
                  FROM ${database}.${admin_schema}.migration_share_objects
                  WHERE migration_id = ${migration_id}
                    AND object_type = 'TABLE'
              )
        `;
        snowflake.execute({sqlText: base_table_sql});

        // Step 3: Classify DERIVED_TABLEs
        // Definition: All remaining tables (have transformations/dependencies)
        var derived_table_sql = `
            UPDATE ${database}.${admin_schema}.migration_share_objects
            SET object_classification = 'DERIVED_TABLE'
            WHERE migration_id = ${migration_id}
              AND object_type = 'TABLE'
              AND object_classification IS NULL
        `;
        snowflake.execute({sqlText: derived_table_sql});

        // Step 4: Get classification summary
        var summary_sql = `
            SELECT
                object_classification,
                COUNT(*) as object_count
            FROM ${database}.${admin_schema}.migration_share_objects
            WHERE migration_id = ${migration_id}
            GROUP BY object_classification
            ORDER BY object_classification
        `;
        var result = snowflake.execute({sqlText: summary_sql});

        var summary = "Object Classification Summary:\n";
        while (result.next()) {
            summary += "  " + result.getColumnValue(1) + ": " + result.getColumnValue(2) + " objects\n";
        }

        return summary;

    } catch (err) {
        return "ERROR in sp_classify_migration_objects: " + err.message + "\nSQL: " + err.stackTraceTxt;
    }
$$;

-- Example Usage:
-- CALL PROD_DB.ADMIN_SCHEMA.sp_classify_migration_objects(
--     2,                -- migration_id
--     'PROD_DB',        -- database
--     'ADMIN_SCHEMA'    -- admin_schema
-- );
