-- ============================================
-- IMSDLC (TARGET) - Stored Procedure: Full Migration Orchestrator
-- ============================================
-- Purpose: Master procedure to orchestrate complete target-side migration
-- This is the main entry point for target-side execution

USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA admin_schema;

CREATE OR REPLACE PROCEDURE dev_db.admin_schema.sp_execute_full_migration(
    p_migration_id FLOAT,
    p_shared_database VARCHAR,
    p_shared_schema VARCHAR,         -- Admin schema in shared DB with metadata
    p_target_database VARCHAR,
    p_admin_schema VARCHAR,          -- Target admin schema for execution log
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
BEGIN
    -- Step 1: Simple validation message
    validation_msg := 'Starting migration ' || :p_migration_id || ' from shared database ' ||
                      :p_shared_database || CHR(10);

    -- Step 3: Execute DDL scripts
    CALL sp_execute_target_ddl(
        :p_migration_id,
        :p_shared_database,
        :p_shared_schema,
        :p_target_database,
        :p_admin_schema
    ) INTO :ddl_result;

    -- Step 4: Execute CTAS scripts
    validation_msg := validation_msg || 'Proceeding with CTAS data migration.' || CHR(10);

    CALL sp_execute_target_ctas(
        :p_migration_id,
        :p_shared_database,
        :p_shared_schema,
        :p_target_database,
        :p_admin_schema
    ) INTO :ctas_result;

    RETURN :validation_msg || :ddl_result || CHR(10) || :ctas_result;
END;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_full_migration';
