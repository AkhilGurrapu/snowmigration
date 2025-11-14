-- ============================================
-- IMCUST (SOURCE) - Generate Hybrid Migration Scripts
-- ============================================
-- Purpose: Generate migration scripts that preserve native Snowflake lineage
-- Uses captured transformation SQL to recreate organic data flow on target
--
-- Migration Strategies:
--   VIEW_ONLY: Views don't need data migration (DDL contains query logic)
--   CTAS_FROM_SHARED: Base tables with no dependencies (use CTAS from shared DB)
--   INSERT_WITH_TRANSFORMATION: Derived tables (use captured transformation SQL)
--
-- This is the KEY innovation that preserves lineage by using INSERT...SELECT
-- with the original transformation logic instead of simple CTAS from shared DB
--
-- Author: Enhanced Migration Framework v2.0
-- ============================================

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_generate_hybrid_migration_scripts(
    p_migration_id FLOAT,
    p_database VARCHAR,
    p_target_database VARCHAR,
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
        var target_database = P_TARGET_DATABASE;
        var admin_schema = P_ADMIN_SCHEMA;

        // Clear existing hybrid scripts for this migration
        var clear_sql = `
            DELETE FROM ${database}.${admin_schema}.migration_hybrid_scripts
            WHERE migration_id = ${migration_id}
        `;
        snowflake.execute({sqlText: clear_sql});

        // Get all objects with their classification and transformation SQL
        var get_objects_sql = `
            SELECT
                mso.source_database,
                mso.source_schema,
                mso.object_name,
                mso.object_type,
                mso.object_classification,
                mso.dependency_level,
                mso.fully_qualified_name,
                mts.transformation_sql,
                mts.capture_method,
                mts.confidence_score
            FROM ${database}.${admin_schema}.migration_share_objects mso
            LEFT JOIN ${database}.${admin_schema}.migration_transformation_sql mts
                ON mso.migration_id = mts.migration_id
                AND UPPER(mso.object_name) = UPPER(mts.object_name)  -- FIX: Case-insensitive JOIN
            WHERE mso.migration_id = ${migration_id}
            ORDER BY mso.dependency_level DESC, mso.object_name
        `;

        var objects = snowflake.execute({sqlText: get_objects_sql});
        var view_count = 0;
        var ctas_count = 0;
        var insert_count = 0;

        while (objects.next()) {
            var src_database = objects.getColumnValue('SOURCE_DATABASE');
            var src_schema = objects.getColumnValue('SOURCE_SCHEMA');
            var obj_name = objects.getColumnValue('OBJECT_NAME');
            var obj_type = objects.getColumnValue('OBJECT_TYPE');
            var obj_class = objects.getColumnValue('OBJECT_CLASSIFICATION');
            var dep_level = objects.getColumnValue('DEPENDENCY_LEVEL');
            var fqn = objects.getColumnValue('FULLY_QUALIFIED_NAME');
            var transform_sql = objects.getColumnValue('TRANSFORMATION_SQL');
            var capture_method = objects.getColumnValue('CAPTURE_METHOD');
            var confidence = objects.getColumnValue('CONFIDENCE_SCORE');

            var migration_strategy = '';
            var migration_script = '';
            var execution_order = dep_level; // Higher dependency = execute first

            // Determine migration strategy based on object classification
            if (obj_class == 'VIEW') {
                // Strategy: VIEW_ONLY - No data migration needed
                migration_strategy = 'VIEW_ONLY';
                migration_script = '-- Views are migrated via DDL only (no data migration needed)';
                view_count++;

            } else if (obj_class == 'BASE_TABLE') {
                // Strategy: CTAS_FROM_SHARED - Simple CTAS (no lineage needed for base tables)
                migration_strategy = 'CTAS_FROM_SHARED';
                migration_script = `CREATE OR REPLACE TABLE ${target_database}.${src_schema}.${obj_name} AS\\n`;
                migration_script += `SELECT * FROM <SHARED_DB_NAME>.${src_schema}.${obj_name};`;
                ctas_count++;

            } else if (obj_class == 'DERIVED_TABLE') {
                // Strategy: INSERT_WITH_TRANSFORMATION - Use captured SQL to preserve lineage
                if (transform_sql != null && transform_sql != '' && confidence != null && confidence >= 0.5) {
                    // We have good transformation SQL - adapt it for target database
                    migration_strategy = 'INSERT_WITH_TRANSFORMATION';

                    // Replace source database references with target database
                    var adapted_sql = transform_sql.replace(new RegExp(src_database, 'gi'), target_database);

                    // Extract just the INSERT...SELECT or MERGE portion
                    // (remove CREATE TABLE if it's a CTAS)
                    if (adapted_sql.toUpperCase().includes('CREATE') && adapted_sql.toUpperCase().includes('AS SELECT')) {
                        // Convert CTAS to INSERT INTO
                        // Find the SELECT part after "AS SELECT" (case-insensitive)
                        var upper_sql = adapted_sql.toUpperCase();
                        var as_select_idx = upper_sql.indexOf('AS SELECT');
                        if (as_select_idx > 0) {
                            // Extract everything after "AS SELECT" (add 9 for "AS SELECT" length)
                            var select_part = adapted_sql.substring(as_select_idx + 9).trim();
                            migration_script = `-- Original transformation from ${capture_method} (confidence: ${confidence})\\n`;
                            migration_script += `INSERT INTO ${target_database}.${src_schema}.${obj_name}\\n`;
                            migration_script += `SELECT ${select_part}`;
                        } else {
                            // Fallback: try to find SELECT directly
                            var select_idx = upper_sql.indexOf('SELECT');
                            if (select_idx > 0) {
                                var select_part = adapted_sql.substring(select_idx).trim();
                                migration_script = `-- Original transformation from ${capture_method} (confidence: ${confidence})\\n`;
                                migration_script += `INSERT INTO ${target_database}.${src_schema}.${obj_name}\\n`;
                                migration_script += select_part;
                            } else {
                                // Can't convert, use as-is
                                migration_script = `-- Original transformation from ${capture_method} (confidence: ${confidence})\\n`;
                                migration_script += adapted_sql;
                            }
                        }
                    } else {
                        // Already an INSERT or MERGE - adapt database names AND fix INSERT INTO table name
                        migration_script = `-- Original transformation from ${capture_method} (confidence: ${confidence})\\n`;
                        
                        // Replace INSERT INTO <table_name> with fully qualified target name
                        var insert_pattern = new RegExp(`INSERT\\s+INTO\\s+${obj_name}\\s+`, 'gi');
                        var target_table_name = `${target_database}.${src_schema}.${obj_name}`;
                        adapted_sql = adapted_sql.replace(insert_pattern, `INSERT INTO ${target_table_name} `);
                        
                        // Also handle MERGE INTO
                        var merge_pattern = new RegExp(`MERGE\\s+INTO\\s+${obj_name}\\s+`, 'gi');
                        adapted_sql = adapted_sql.replace(merge_pattern, `MERGE INTO ${target_table_name} `);
                        
                        // Fully qualify table references in FROM/JOIN clauses for tables in this migration
                        // Get list of all migrated table names (case-insensitive)
                        var get_migrated_tables_sql = `
                            SELECT DISTINCT UPPER(object_name) as obj_name_upper, object_name, source_schema
                            FROM ${database}.${admin_schema}.migration_share_objects
                            WHERE migration_id = ${migration_id}
                              AND object_type = 'TABLE'
                        `;
                        var migrated_tables_result = snowflake.execute({sqlText: get_migrated_tables_sql});
                        var migrated_tables = [];
                        while (migrated_tables_result.next()) {
                            var table_name = migrated_tables_result.getColumnValue('OBJECT_NAME');
                            var table_schema = migrated_tables_result.getColumnValue('SOURCE_SCHEMA');
                            migrated_tables.push({
                                name: table_name,
                                upper: table_name.toUpperCase(),
                                schema: table_schema,
                                fqn: `${target_database}.${table_schema}.${table_name}`
                            });
                        }
                        
                        // Replace unqualified table references with fully qualified names
                        // Pattern: FROM <table_name> or JOIN <table_name>
                        for (var t = 0; t < migrated_tables.length; t++) {
                            var table = migrated_tables[t];
                            // Skip the target table itself (already fixed above)
                            if (table.upper === obj_name.toUpperCase()) continue;
                            
                            // Replace FROM <table_name> with FROM <fully_qualified_name>
                            var from_pattern = new RegExp(`(FROM|JOIN)\\s+${table.name}\\s+`, 'gi');
                            adapted_sql = adapted_sql.replace(from_pattern, `$1 ${table.fqn} `);
                            
                            // Also handle table aliases: FROM <table_name> AS alias
                            var from_as_pattern = new RegExp(`(FROM|JOIN)\\s+${table.name}\\s+AS\\s+`, 'gi');
                            adapted_sql = adapted_sql.replace(from_as_pattern, `$1 ${table.fqn} AS `);
                        }
                        
                        migration_script += adapted_sql;
                    }

                    insert_count++;

                } else {
                    // No transformation SQL available - fallback to CTAS
                    migration_strategy = 'CTAS_FROM_SHARED';
                    migration_script = `-- WARNING: No transformation SQL found - using CTAS (lineage will NOT be preserved)\\n`;
                    migration_script += `CREATE OR REPLACE TABLE ${target_database}.${src_schema}.${obj_name} AS\\n`;
                    migration_script += `SELECT * FROM <SHARED_DB_NAME>.${src_schema}.${obj_name};`;
                    ctas_count++;
                }
            }

            // Insert into migration_hybrid_scripts
            var insert_script_sql = `
                INSERT INTO ${database}.${admin_schema}.migration_hybrid_scripts (
                    migration_id,
                    source_database,
                    source_schema,
                    object_name,
                    object_type,
                    object_classification,
                    migration_strategy,
                    migration_script,
                    execution_order
                ) VALUES (
                    ${migration_id},
                    '${src_database}',
                    '${src_schema}',
                    '${obj_name}',
                    '${obj_type}',
                    '${obj_class}',
                    '${migration_strategy}',
                    '${migration_script.replace(/'/g, "''")}',
                    ${execution_order}
                )
            `;
            snowflake.execute({sqlText: insert_script_sql});
        }

        var summary = "Hybrid Migration Script Generation Summary:\\n";
        summary += "  VIEW_ONLY: " + view_count + " objects\\n";
        summary += "  CTAS_FROM_SHARED: " + ctas_count + " objects\\n";
        summary += "  INSERT_WITH_TRANSFORMATION: " + insert_count + " objects (LINEAGE PRESERVED!)\\n";
        summary += "\\nTotal scripts generated: " + (view_count + ctas_count + insert_count) + "\\n";

        if (insert_count > 0) {
            summary += "\\n✓ SUCCESS: " + insert_count + " objects will have native Snowflake lineage preserved!";
        }

        if (ctas_count > view_count) {
            summary += "\\n⚠ WARNING: " + (ctas_count - view_count) + " derived tables using CTAS (lineage NOT preserved)";
        }

        return summary;

    } catch (err) {
        return "ERROR in sp_generate_hybrid_migration_scripts: " + err.message + "\\nSQL: " + err.stackTraceTxt;
    }
$$;

-- Example Usage:
-- CALL PROD_DB.ADMIN_SCHEMA.sp_generate_hybrid_migration_scripts(
--     2,                -- migration_id
--     'PROD_DB',        -- source database
--     'DEV_DB',         -- target database
--     'ADMIN_SCHEMA'    -- admin_schema
-- );
