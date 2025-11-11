-- ============================================
-- IMSDLC (TARGET) - Stored Procedure: Execute Target DDL
-- ============================================
-- Purpose: Execute DDL scripts from shared database on target account
-- This procedure reads DDL scripts and executes them in dependency order

USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA admin_schema;

CREATE OR REPLACE PROCEDURE dev_db.admin_schema.sp_execute_target_ddl(
    p_migration_id FLOAT,
    p_shared_database VARCHAR,
    p_shared_schema VARCHAR,
    p_target_database VARCHAR,
    p_admin_schema VARCHAR  -- Target admin schema where execution log is stored
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var success_count = 0;
    var error_count = 0;

    // Build the query to get DDL scripts
    var query = `
        SELECT object_name, object_type, target_ddl, dependency_level
        FROM IDENTIFIER('${P_SHARED_DATABASE}.${P_SHARED_SCHEMA}.migration_ddl_scripts')
        WHERE migration_id = ?
        ORDER BY dependency_level DESC, object_name
    `;

    var stmt = snowflake.createStatement({
        sqlText: query,
        binds: [P_MIGRATION_ID]
    });
    var resultSet = stmt.execute();

    // Execute each DDL script
    while (resultSet.next()) {
        var object_name = resultSet.getColumnValue('OBJECT_NAME');
        var object_type = resultSet.getColumnValue('OBJECT_TYPE');
        var ddl_script = resultSet.getColumnValue('TARGET_DDL');
        var dep_level = resultSet.getColumnValue('DEPENDENCY_LEVEL');
        var start_time = Date.now();

        try {
            // Execute the DDL
            var ddl_stmt = snowflake.createStatement({sqlText: ddl_script});
            ddl_stmt.execute();
            var end_time = Date.now();

            // Log success
            var log_sql = `
                INSERT INTO ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.migration_execution_log
                (migration_id, execution_phase, object_name, script_type, sql_statement, status, execution_time_ms)
                VALUES (?, 'DDL_EXECUTION', ?, ?, ?, 'SUCCESS', ?)
            `;
            var log_stmt = snowflake.createStatement({
                sqlText: log_sql,
                binds: [P_MIGRATION_ID, object_name, object_type, ddl_script, (end_time - start_time)]
            });
            log_stmt.execute();
            success_count++;

        } catch (err) {
            var end_time = Date.now();

            // Log error
            var log_sql = `
                INSERT INTO ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.migration_execution_log
                (migration_id, execution_phase, object_name, script_type, sql_statement, status, error_message, execution_time_ms)
                VALUES (?, 'DDL_EXECUTION', ?, ?, ?, 'FAILED', ?, ?)
            `;
            var log_stmt = snowflake.createStatement({
                sqlText: log_sql,
                binds: [P_MIGRATION_ID, object_name, object_type, ddl_script, err.message, (end_time - start_time)]
            });
            log_stmt.execute();
            error_count++;
        }
    }

    return `DDL Execution Complete: ${success_count} succeeded, ${error_count} failed. Check ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.migration_execution_log for details.`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_target_ddl';
