-- ============================================
-- IMSDLC (TARGET) - Execute Hybrid Migration Scripts
-- ============================================
-- Purpose: Execute hybrid migration scripts that preserve native Snowflake lineage
-- Uses INSERT...SELECT with transformation SQL instead of CTAS from shared DB
--
-- Migration Strategies:
--   VIEW_ONLY: Skip (already handled by DDL execution)
--   CTAS_FROM_SHARED: Create table from shared DB (for base tables)
--   INSERT_WITH_TRANSFORMATION: Execute transformation SQL (PRESERVES LINEAGE!)
--
-- This is the KEY execution step that establishes organic lineage relationships
-- by using INSERT...SELECT from already-migrated upstream tables
--
-- Author: Enhanced Migration Framework v2.0
-- ============================================

CREATE OR REPLACE PROCEDURE DEV_DB.ADMIN_SCHEMA.sp_execute_hybrid_migration(
    p_migration_id FLOAT,
    p_shared_database VARCHAR,  -- Shared DB name (e.g., IMCUST_SHARED_DB)
    p_shared_schema VARCHAR,    -- Admin schema in shared DB
    p_target_database VARCHAR,  -- Target database for execution
    p_admin_schema VARCHAR      -- Admin schema for execution log
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    try {
        var migration_id = P_MIGRATION_ID;
        var shared_db = P_SHARED_DATABASE;
        var shared_schema = P_SHARED_SCHEMA;
        var target_db = P_TARGET_DATABASE;
        var admin_schema = P_ADMIN_SCHEMA;

        var success_count = 0;
        var failed_count = 0;
        var skipped_count = 0;

        // Get all hybrid migration scripts ordered by execution_order (highest first)
        var get_scripts_sql = `
            SELECT
                source_schema,
                object_name,
                object_type,
                object_classification,
                migration_strategy,
                migration_script,
                execution_order
            FROM ${shared_db}.${shared_schema}.migration_hybrid_scripts
            WHERE migration_id = ${migration_id}
            ORDER BY execution_order DESC, object_name
        `;

        var scripts = snowflake.execute({sqlText: get_scripts_sql});

        while (scripts.next()) {
            var src_schema = scripts.getColumnValue('SOURCE_SCHEMA');
            var obj_name = scripts.getColumnValue('OBJECT_NAME');
            var obj_type = scripts.getColumnValue('OBJECT_TYPE');
            var obj_class = scripts.getColumnValue('OBJECT_CLASSIFICATION');
            var strategy = scripts.getColumnValue('MIGRATION_STRATEGY');
            var script = scripts.getColumnValue('MIGRATION_SCRIPT');
            var exec_order = scripts.getColumnValue('EXECUTION_ORDER');

            var start_time = Date.now();
            var status = 'SUCCESS';
            var error_message = null;

            try {
                if (strategy == 'VIEW_ONLY') {
                    // Skip - views are handled by DDL execution
                    skipped_count++;
                    status = 'SKIPPED';

                } else if (strategy == 'CTAS_FROM_SHARED') {
                    // Replace placeholder with actual shared DB name
                    var exec_script = script.replace(/<SHARED_DB_NAME>/g, shared_db);
                    snowflake.execute({sqlText: exec_script});
                    success_count++;

                } else if (strategy == 'INSERT_WITH_TRANSFORMATION') {
                    // Execute transformation SQL - THIS CREATES LINEAGE!
                    snowflake.execute({sqlText: script});
                    success_count++;

                } else {
                    error_message = 'Unknown migration strategy: ' + strategy;
                    status = 'FAILED';
                    failed_count++;
                }

            } catch (err) {
                status = 'FAILED';
                error_message = err.message;
                failed_count++;
            }

            var end_time = Date.now();
            var execution_time = end_time - start_time;

            // Log execution result
            var log_sql = `
                INSERT INTO ${target_db}.${admin_schema}.migration_execution_log (
                    migration_id,
                    execution_phase,
                    object_name,
                    script_type,
                    sql_statement,
                    status,
                    error_message,
                    execution_time_ms
                ) VALUES (
                    ${migration_id},
                    'HYBRID_MIGRATION',
                    '${obj_name}',
                    '${strategy}',
                    '${script.replace(/'/g, "''")}',
                    '${status}',
                    ${error_message ? "'" + error_message.replace(/'/g, "''") + "'" : "NULL"},
                    ${execution_time}
                )
            `;
            snowflake.execute({sqlText: log_sql});
        }

        var summary = "Hybrid Migration Execution Complete:\n";
        summary += "  " + success_count + " succeeded\n";
        summary += "  " + failed_count + " failed\n";
        summary += "  " + skipped_count + " skipped (views)\n";
        summary += "\nCheck migration_execution_log for details.\n";

        if (success_count > 0) {
            summary += "\n✓ SUCCESS: Native Snowflake lineage should now be preserved!";
            summary += "\n  Verify with: SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(...))";
        }

        return summary;

    } catch (err) {
        return "ERROR in sp_execute_hybrid_migration: " + err.message;
    }
$$;

-- Example Usage:
-- CALL DEV_DB.ADMIN_SCHEMA.sp_execute_hybrid_migration(
--     2,                      -- migration_id
--     'IMCUST_SHARED_DB',     -- Shared database name
--     'ADMIN_SCHEMA',         -- Admin schema in shared DB
--     'DEV_DB',              -- Target database
--     'ADMIN_SCHEMA'         -- Admin schema for execution log
-- );
