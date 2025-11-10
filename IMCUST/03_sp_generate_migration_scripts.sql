-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Generate Migration Scripts
-- ============================================
-- Purpose: Extract DDLs and generate CTAS scripts for all objects
-- Replaces source database name with target database name

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_generate_migration_scripts(
    p_migration_id FLOAT,
    p_target_database VARCHAR,
    p_target_schema VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // Get all objects to migrate (dependencies + original objects)
    var get_objects_sql = `
        SELECT DISTINCT
            fully_qualified_name,
            object_type,
            ROW_NUMBER() OVER (ORDER BY fully_qualified_name) as exec_order
        FROM migration_share_objects
        WHERE migration_id = ?
        ORDER BY fully_qualified_name
    `;

    var stmt = snowflake.createStatement({
        sqlText: get_objects_sql,
        binds: [P_MIGRATION_ID]
    });
    var objects = stmt.execute();

    var ddl_count = 0;
    var ctas_count = 0;

    while (objects.next()) {
        var fqn = objects.getColumnValue('FULLY_QUALIFIED_NAME');
        var obj_type = objects.getColumnValue('OBJECT_TYPE');
        var exec_order = objects.getColumnValue('EXEC_ORDER');

        // Extract object name
        var parts = fqn.split('.');
        var obj_name = parts[parts.length - 1];
        var source_schema = parts.length > 1 ? parts[parts.length - 2] : P_TARGET_SCHEMA;
        var source_db = parts.length > 2 ? parts[parts.length - 3] : '';

        // Get DDL based on object type
        var ddl_type = obj_type === 'VIEW' ? 'VIEW' : 'TABLE';
        var get_ddl_sql = `SELECT GET_DDL('${ddl_type}', '${fqn}') as ddl`;

        try {
            stmt = snowflake.createStatement({sqlText: get_ddl_sql});
            var ddl_result = stmt.execute();
            ddl_result.next();
            var source_ddl = ddl_result.getColumnValue('DDL');

            // Replace source database with target database
            var target_ddl = source_ddl;
            if (source_db) {
                // Use regex to replace database name while preserving structure
                var db_pattern = new RegExp(source_db.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
                target_ddl = target_ddl.replace(db_pattern, P_TARGET_DATABASE);
            }

            // Store DDL scripts
            var insert_ddl = `
                INSERT INTO migration_ddl_scripts
                (migration_id, object_name, object_type, dependency_level, source_ddl, target_ddl)
                VALUES (?, ?, ?, ?, ?, ?)
            `;
            stmt = snowflake.createStatement({
                sqlText: insert_ddl,
                binds: [P_MIGRATION_ID, obj_name, obj_type, exec_order, source_ddl, target_ddl]
            });
            stmt.execute();
            ddl_count++;

            // Generate CTAS script for tables (not views)
            if (obj_type === 'TABLE') {
                var ctas_script = `
-- CTAS for ${obj_name}
CREATE OR REPLACE TABLE ${P_TARGET_DATABASE}.${P_TARGET_SCHEMA}.${obj_name} AS
SELECT * FROM <SHARED_DB_NAME>.${source_schema}.${obj_name};
                `;

                var insert_ctas = `
                    INSERT INTO migration_ctas_scripts
                    (migration_id, object_name, ctas_script, execution_order)
                    VALUES (?, ?, ?, ?)
                `;
                stmt = snowflake.createStatement({
                    sqlText: insert_ctas,
                    binds: [P_MIGRATION_ID, obj_name, ctas_script, exec_order]
                });
                stmt.execute();
                ctas_count++;
            }

        } catch (err) {
            // Log error but continue
            continue;
        }
    }

    return `Generated ${ddl_count} DDL scripts and ${ctas_count} CTAS scripts`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_generate_migration_scripts';
