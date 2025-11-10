-- ============================================
-- IMSDLC (TARGET) - Stored Procedure: Execute DDL Scripts
-- ============================================
-- Purpose: Execute all DDL scripts in dependency order
-- Reads DDL scripts from shared database and executes them

USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_execute_target_ddl(
    p_migration_id NUMBER,
    p_shared_database VARCHAR  -- e.g., 'shared_prod_db'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_query VARCHAR;
    v_table_name VARCHAR;
    ddl_resultset RESULTSET;
    ddl_cursor CURSOR FOR ddl_resultset;
    v_object_name VARCHAR;
    v_object_type VARCHAR;
    v_ddl_script VARCHAR;
    v_dep_level NUMBER;
    v_success_count NUMBER DEFAULT 0;
    v_error_count NUMBER DEFAULT 0;
    v_start_time TIMESTAMP_LTZ;
    v_end_time TIMESTAMP_LTZ;
    v_error_msg VARCHAR;
BEGIN
    -- Build table name
    v_table_name := p_shared_database || '.mart_investments_bolt.migration_ddl_scripts';

    -- Build dynamic query
    v_query := 'SELECT object_name, object_type, target_ddl, dependency_level ' ||
               'FROM IDENTIFIER(?) ' ||
               'WHERE migration_id = ? ' ||
               'ORDER BY dependency_level, object_name';

    -- Execute query and get resultset
    ddl_resultset := (EXECUTE IMMEDIATE :v_query USING (v_table_name, p_migration_id));

    -- Open cursor on resultset
    OPEN ddl_cursor;

    -- Iterate through DDL scripts
    FOR record IN ddl_cursor DO
        v_object_name := record.object_name;
        v_object_type := record.object_type;
        v_ddl_script := record.target_ddl;
        v_dep_level := record.dependency_level;
        v_start_time := CURRENT_TIMESTAMP();

        BEGIN
            -- Execute the DDL statement
            EXECUTE IMMEDIATE :v_ddl_script;
            v_end_time := CURRENT_TIMESTAMP();

            -- Log success
            INSERT INTO migration_execution_log
                (migration_id, execution_phase, object_name, script_type,
                 sql_statement, status, execution_time_ms)
            VALUES
                (:p_migration_id, 'DDL_EXECUTION', :v_object_name, :v_object_type,
                 :v_ddl_script, 'SUCCESS',
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
                    (:p_migration_id, 'DDL_EXECUTION', :v_object_name, :v_object_type,
                     :v_ddl_script, 'FAILED', :v_error_msg,
                     DATEDIFF(millisecond, :v_start_time, :v_end_time));

                v_error_count := v_error_count + 1;
        END;
    END FOR;

    CLOSE ddl_cursor;

    RETURN 'DDL Execution Complete: ' || v_success_count || ' succeeded, ' ||
           v_error_count || ' failed. Check migration_execution_log for details.';
END;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_target_ddl';
