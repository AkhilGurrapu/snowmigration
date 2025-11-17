-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Generate Migration Scripts
-- ============================================
-- Purpose: Extract DDLs and generate CTAS scripts for all objects
-- Replaces source database name with target database name

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;
USE SCHEMA ADMIN_SCHEMA;

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_generate_migration_scripts(
    p_migration_id FLOAT,
    p_target_database VARCHAR,
    p_target_schema VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Get all objects to migrate (dependencies + original objects)
    // Order by dependency_level DESC so deepest dependencies are created first
    var get_objects_sql = `
        SELECT DISTINCT
            source_database,
            source_schema,
            object_name,
            fully_qualified_name,
            object_type,
            dependency_level
        FROM migration_share_objects
        WHERE migration_id = ?
        ORDER BY dependency_level DESC, fully_qualified_name
    `;

    var stmt = snowflake.createStatement({
        sqlText: get_objects_sql,
        binds: [P_MIGRATION_ID]
    });
    var objects = stmt.execute();

    // Clear any existing records for this migration_id to ensure idempotency
    var delete_ddl = `DELETE FROM migration_ddl_scripts WHERE migration_id = ?`;
    stmt = snowflake.createStatement({sqlText: delete_ddl, binds: [P_MIGRATION_ID]});
    stmt.execute();

    var delete_ctas = `DELETE FROM migration_ctas_scripts WHERE migration_id = ?`;
    stmt = snowflake.createStatement({sqlText: delete_ctas, binds: [P_MIGRATION_ID]});
    stmt.execute();

    var ddl_count = 0;
    var ctas_count = 0;
    var table_count = 0;
    var view_count = 0;

    while (objects.next()) {
        var source_db = objects.getColumnValue('SOURCE_DATABASE');
        var source_schema = objects.getColumnValue('SOURCE_SCHEMA');
        var obj_name = objects.getColumnValue('OBJECT_NAME');
        var fqn = objects.getColumnValue('FULLY_QUALIFIED_NAME');
        var obj_type = objects.getColumnValue('OBJECT_TYPE');
        var dep_level = objects.getColumnValue('DEPENDENCY_LEVEL');

        // Get DDL based on object type
        var ddl_type = obj_type === 'VIEW' ? 'VIEW' : 'TABLE';
        var get_ddl_sql = `SELECT GET_DDL('${ddl_type}', '${fqn}') as ddl`;

        try {
            stmt = snowflake.createStatement({sqlText: get_ddl_sql});
            var ddl_result = stmt.execute();
            ddl_result.next();
            var source_ddl = ddl_result.getColumnValue('DDL');

            // GET_DDL returns DDL without database.schema prefix, so we need to inject it
            // Example: "create or replace TABLE TABLE_NAME (" → "create or replace TABLE DEV_DB.SCHEMA.TABLE_NAME ("
            var target_fqn = `${P_TARGET_DATABASE}.${source_schema}.${obj_name}`;

            // Replace the object name in DDL with fully qualified name
            // Pattern matches: CREATE OR REPLACE TABLE/VIEW <object_name> (
            var ddl_pattern = new RegExp(`(create\\s+or\\s+replace\\s+(?:table|view))\\s+(${obj_name})\\s*\\(`, 'gi');
            var target_ddl = source_ddl.replace(ddl_pattern, `$1 ${target_fqn} (`);

            // If replacement didn't work (edge case), prepend schema to object name at beginning
            if (target_ddl === source_ddl) {
                // Fallback: try to match without parenthesis (for views with AS clause immediately)
                var view_pattern = new RegExp(`(create\\s+or\\s+replace\\s+view)\\s+(${obj_name})\\s+`, 'gi');
                target_ddl = source_ddl.replace(view_pattern, `$1 ${target_fqn} `);
            }

            // This handles references inside view definitions like: FROM PROD_DB.SRC_INVESTMENTS_BOLT.table_name
            var db_pattern = new RegExp(source_db, 'gi');
            target_ddl = target_ddl.replace(db_pattern, P_TARGET_DATABASE);

            // GET_DDL() may return unqualified references for same-schema objects
            // We need to add database.schema prefix to these references

            // Get all objects in this migration to qualify unqualified references
            var get_all_objects_sql = `
                SELECT DISTINCT object_name, source_schema
                FROM migration_share_objects
                WHERE migration_id = ?
                  AND object_name != ?
            `;
            var obj_stmt = snowflake.createStatement({
                sqlText: get_all_objects_sql,
                binds: [P_MIGRATION_ID, obj_name]
            });
            var all_objects = obj_stmt.execute();

            // For each object, replace unqualified references with fully qualified ones
            while (all_objects.next()) {
                var ref_obj_name = all_objects.getColumnValue('OBJECT_NAME');
                var ref_obj_schema = all_objects.getColumnValue('SOURCE_SCHEMA');
                var qualified_name = `${P_TARGET_DATABASE}.${ref_obj_schema}.${ref_obj_name}`;

                // Escape object name for regex
                var escaped_obj_name = ref_obj_name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

                // Pattern to match unqualified references
                // Matches word boundary + object_name + word boundary
                var search_regex = new RegExp('\\b' + escaped_obj_name + '\\b', 'gi');

                // Find all matches and their positions
                var match;
                var replacements = [];

                while ((match = search_regex.exec(target_ddl)) !== null) {
                    var match_index = match.index;
                    var match_text = match[0];

                    // Get context before the match (up to 50 chars)
                    var before_start = Math.max(0, match_index - 50);
                    var before_text = target_ddl.substring(before_start, match_index);

                    // Check if already qualified (preceded by a dot)
                    if (before_text.match(/\.\s*$/)) {
                        continue; // Skip - already qualified as schema.object or db.schema.object
                    }

                    // Check if in FROM/JOIN/comma context
                    if (before_text.match(/(from|join|,)\s+$/i)) {
                        // This is an unqualified reference that needs qualification
                        replacements.push({
                            index: match_index,
                            length: match_text.length,
                            replacement: qualified_name
                        });
                    }
                }

                // Apply replacements in reverse order to preserve indices
                for (var r = replacements.length - 1; r >= 0; r--) {
                    var repl = replacements[r];
                    target_ddl = target_ddl.substring(0, repl.index) +
                                repl.replacement +
                                target_ddl.substring(repl.index + repl.length);
                }
            }

            if (obj_type === 'VIEW') {
                var insert_ddl = `
                    INSERT INTO migration_ddl_scripts
                    (migration_id, source_database, source_schema, object_name, object_type, dependency_level, source_ddl, target_ddl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                `;
                stmt = snowflake.createStatement({
                    sqlText: insert_ddl,
                    binds: [P_MIGRATION_ID, source_db, source_schema, obj_name, obj_type, dep_level, source_ddl, target_ddl]
                });
                stmt.execute();
                ddl_count++;
                view_count++;
            } else {
                table_count++;
            }

            // Generate CTAS script for tables (not views)
            if (obj_type === 'TABLE') {
                var ctas_script = `
-- CTAS for ${obj_name}
CREATE OR REPLACE TABLE ${P_TARGET_DATABASE}.${source_schema}.${obj_name} AS
SELECT * FROM <SHARED_DB_NAME>.${source_schema}.${obj_name};
                `;

                var insert_ctas = `
                    INSERT INTO migration_ctas_scripts
                    (migration_id, source_database, source_schema, object_name, ctas_script, execution_order)
                    VALUES (?, ?, ?, ?, ?, ?)
                `;
                stmt = snowflake.createStatement({
                    sqlText: insert_ctas,
                    binds: [P_MIGRATION_ID, source_db, source_schema, obj_name, ctas_script, dep_level]
                });
                stmt.execute();
                ctas_count++;
            }

        } catch (err) {
            // Log error but continue
            continue;
        }
    }

    // Simplified output message with counts only
    var result_msg = `
📝 SCRIPTS GENERATED:
   • View DDL Scripts: ${ddl_count} (views only - tables use CTAS)
   • CTAS Scripts: ${ctas_count} (data migration)
   • Total Objects: ${table_count + view_count} (${table_count} tables, ${view_count} views)
    `;

    return result_msg;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_generate_migration_scripts';
