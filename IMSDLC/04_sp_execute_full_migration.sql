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
    p_shared_schema VARCHAR,    -- e.g., 'mart_investments_bolt'
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
BEGIN
    -- Step 1: Get counts from shared metadata - DDL scripts
    v_table_name := p_shared_database || '.' || p_shared_schema || '.migration_ddl_scripts';
    v_query := 'SELECT COUNT(*) as cnt FROM ' || v_table_name || ' WHERE migration_id = ' || p_migration_id;
    LET count_rs RESULTSET := (EXECUTE IMMEDIATE :v_query);
    LET count_cur CURSOR FOR count_rs;
    OPEN count_cur;
    FETCH count_cur INTO v_ddl_count;
    CLOSE count_cur;

    -- Get counts from shared metadata - CTAS scripts
    v_table_name := p_shared_database || '.' || p_shared_schema || '.migration_ctas_scripts';
    v_query := 'SELECT COUNT(*) as cnt FROM ' || v_table_name || ' WHERE migration_id = ' || p_migration_id;
    count_rs := (EXECUTE IMMEDIATE :v_query);
    OPEN count_cur FOR count_rs;
    FETCH count_cur INTO v_ctas_count;
    CLOSE count_cur;

    validation_msg := 'Found ' || :v_ddl_count || ' DDL scripts and ' ||
                      :v_ctas_count || ' CTAS scripts for migration ' || :p_migration_id || '.' || CHR(10);

    -- Step 2: Execute DDL scripts
    CALL sp_execute_target_ddl(:p_migration_id, :p_shared_database, :p_shared_schema)
        INTO :ddl_result;

    -- Step 3: Execute CTAS scripts
    validation_msg := validation_msg || 'Proceeding with CTAS data migration.' || CHR(10);

    CALL sp_execute_target_ctas(:p_migration_id, :p_shared_database, :p_shared_schema)
        INTO :ctas_result;

    RETURN :validation_msg || :ddl_result || CHR(10) || :ctas_result;
END;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_full_migration';
