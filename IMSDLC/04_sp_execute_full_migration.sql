-- ============================================
-- IMSDLC (TARGET) - Stored Procedure: Full Migration Orchestrator
-- ============================================
-- Purpose: Master procedure to orchestrate complete target-side migration
-- This is the main entry point for target-side execution

USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_execute_full_migration(
    p_migration_id NUMBER,
    p_shared_database VARCHAR,
    p_validate_before_ctas BOOLEAN DEFAULT TRUE
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    ddl_result VARCHAR;
    ctas_result VARCHAR;
    validation_msg VARCHAR DEFAULT '';
    v_ddl_count NUMBER;
    v_ctas_count NUMBER;
    v_query VARCHAR;
    v_table_name VARCHAR;
    v_count_result RESULTSET;
    v_count_cursor CURSOR FOR v_count_result;
BEGIN
    -- Step 1: Validate shared database exists
    BEGIN
        EXECUTE IMMEDIATE 'USE DATABASE ' || :p_shared_database;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: Shared database ' || :p_shared_database ||
                   ' does not exist. Create it first from the share.';
    END;

    -- Step 2: Get counts from shared metadata - DDL scripts
    v_query := 'SELECT COUNT(*) as cnt FROM IDENTIFIER(?) WHERE migration_id = ?';
    v_table_name := p_shared_database || '.mart_investments_bolt.migration_ddl_scripts';
    v_count_result := (EXECUTE IMMEDIATE :v_query USING (v_table_name, p_migration_id));
    OPEN v_count_cursor;
    FETCH v_count_cursor INTO v_ddl_count;
    CLOSE v_count_cursor;

    -- Get counts from shared metadata - CTAS scripts
    v_table_name := p_shared_database || '.mart_investments_bolt.migration_ctas_scripts';
    v_count_result := (EXECUTE IMMEDIATE :v_query USING (v_table_name, p_migration_id));
    OPEN v_count_cursor;
    FETCH v_count_cursor INTO v_ctas_count;
    CLOSE v_count_cursor;

    validation_msg := 'Found ' || :v_ddl_count || ' DDL scripts and ' ||
                      :v_ctas_count || ' CTAS scripts for migration ' || :p_migration_id || '.' || CHR(10);

    -- Step 3: Execute DDL scripts
    CALL sp_execute_target_ddl(:p_migration_id, :p_shared_database)
        INTO :ddl_result;

    -- Step 4: Execute CTAS scripts
    validation_msg := validation_msg || 'Proceeding with CTAS data migration.' || CHR(10);

    CALL sp_execute_target_ctas(:p_migration_id, :p_shared_database)
        INTO :ctas_result;

    RETURN :validation_msg || :ddl_result || CHR(10) || :ctas_result;
END;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_full_migration';
