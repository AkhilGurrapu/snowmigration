-- ============================================
-- IMSDLC (TARGET) - Stored Procedure: Execute Target CTAS
-- ============================================
-- Purpose: Execute CTAS scripts to copy data from shared database

USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA admin_schema;

CREATE OR REPLACE PROCEDURE dev_db.admin_schema.sp_execute_target_ctas(
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

    // Build the query to get CTAS scripts
    var query = `
        SELECT object_name, ctas_script, execution_order
        FROM IDENTIFIER('${P_SHARED_DATABASE}.${P_SHARED_SCHEMA}.migration_ctas_scripts')
        WHERE migration_id = ?
        ORDER BY execution_order
    `;

    var stmt = snowflake.createStatement({
        sqlText: query,
        binds: [P_MIGRATION_ID]
    });
    var resultSet = stmt.execute();

    // Execute each CTAS script
    while (resultSet.next()) {
        var object_name = resultSet.getColumnValue('OBJECT_NAME');
        var ctas_script = resultSet.getColumnValue('CTAS_SCRIPT');
        var exec_order = resultSet.getColumnValue('EXECUTION_ORDER');
        var start_time = Date.now();

        // Replace placeholder with actual shared database name
        var final_script = ctas_script.replace(/<SHARED_DB_NAME>/g, P_SHARED_DATABASE);

        try {
            // Execute the CTAS
            var ctas_stmt = snowflake.createStatement({sqlText: final_script});
            ctas_stmt.execute();
            var end_time = Date.now();

            // Log success
            var log_sql = `
                INSERT INTO ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.migration_execution_log
                (migration_id, execution_phase, object_name, script_type, sql_statement, status, execution_time_ms)
                VALUES (?, 'CTAS_EXECUTION', ?, 'CTAS', ?, 'SUCCESS', ?)
            `;
            var log_stmt = snowflake.createStatement({
                sqlText: log_sql,
                binds: [P_MIGRATION_ID, object_name, final_script, (end_time - start_time)]
            });
            log_stmt.execute();
            success_count++;

        } catch (err) {
            var end_time = Date.now();

            // Log error
            var log_sql = `
                INSERT INTO ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.migration_execution_log
                (migration_id, execution_phase, object_name, script_type, sql_statement, status, error_message, execution_time_ms)
                VALUES (?, 'CTAS_EXECUTION', ?, 'CTAS', ?, 'FAILED', ?, ?)
            `;
            var log_stmt = snowflake.createStatement({
                sqlText: log_sql,
                binds: [P_MIGRATION_ID, object_name, final_script, err.message, (end_time - start_time)]
            });
            log_stmt.execute();
            error_count++;
        }
    }

    return `CTAS Execution Complete: ${success_count} succeeded, ${error_count} failed. Check ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.migration_execution_log for details.`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_target_ctas';
