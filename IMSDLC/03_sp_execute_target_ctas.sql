-- ============================================
-- IMSDLC (TARGET) - Stored Procedure: Execute CTAS Scripts
-- ============================================
-- Purpose: Execute all CTAS scripts to copy data from shared database
-- Replaces placeholder with actual shared database name

USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_execute_target_ctas(
    p_migration_id NUMBER,
    p_shared_database VARCHAR,  -- e.g., 'shared_prod_db'
    p_shared_schema VARCHAR     -- e.g., 'mart_investments_bolt'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_query VARCHAR;
    v_table_name VARCHAR;
    ctas_resultset RESULTSET;
    ctas_cursor CURSOR FOR ctas_resultset;
    v_object_name VARCHAR;
    v_ctas_script VARCHAR;
    v_exec_order NUMBER;
    v_final_script VARCHAR;
    v_success_count NUMBER DEFAULT 0;
    v_error_count NUMBER DEFAULT 0;
    v_start_time TIMESTAMP_LTZ;
    v_end_time TIMESTAMP_LTZ;
    v_error_msg VARCHAR;
BEGIN
    -- Build table name dynamically using provided schema
    v_table_name := p_shared_database || '.' || p_shared_schema || '.migration_ctas_scripts';

    -- Build dynamic query
    v_query := 'SELECT object_name, ctas_script, execution_order ' ||
               'FROM IDENTIFIER(?) ' ||
               'WHERE migration_id = ? ' ||
               'ORDER BY execution_order';

    -- Execute query and get resultset
    ctas_resultset := (EXECUTE IMMEDIATE :v_query USING (v_table_name, p_migration_id));

    -- Open cursor on resultset
    OPEN ctas_cursor;

    -- Iterate through CTAS scripts
    FOR record IN ctas_cursor DO
        v_object_name := record.object_name;
        v_ctas_script := record.ctas_script;
        v_exec_order := record.execution_order;
        v_start_time := CURRENT_TIMESTAMP();

        -- Replace placeholder with actual shared database name
        v_final_script := REPLACE(:v_ctas_script, '<SHARED_DB_NAME>', :p_shared_database);

        BEGIN
            -- Execute the CTAS statement
            EXECUTE IMMEDIATE :v_final_script;
            v_end_time := CURRENT_TIMESTAMP();

            -- Log success
            INSERT INTO migration_execution_log
                (migration_id, execution_phase, object_name, script_type,
                 sql_statement, status, execution_time_ms)
            VALUES
                (:p_migration_id, 'CTAS_EXECUTION', :v_object_name, 'CTAS',
                 :v_final_script, 'SUCCESS',
                 DATEDIFF(millisecond, :v_start_time, :v_end_time));

            v_success_count := v_success_count + 1;

        EXCEPTION
            WHEN OTHER THEN
                v_error_msg := SQLERRM;
                v_end_time := CURRENT_TIMESTAMP();

                -- Log error
                INSERT INTO migration_execution_log
                    (migration_id, execution_phase, object_name, script_type,
                     sql_statement, status, error_message, execution_time_ms)
                VALUES
                    (:p_migration_id, 'CTAS_EXECUTION', :v_object_name, 'CTAS',
                     :v_final_script, 'FAILED', :v_error_msg,
                     DATEDIFF(millisecond, :v_start_time, :v_end_time));

                v_error_count := v_error_count + 1;
        END;
    END FOR;

    CLOSE ctas_cursor;

    RETURN 'CTAS Execution Complete: ' || v_success_count || ' succeeded, ' ||
           v_error_count || ' failed. Check migration_execution_log for details.';
END;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_target_ctas';
