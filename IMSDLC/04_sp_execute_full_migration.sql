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
    header_msg VARCHAR;
    final_result VARCHAR;
BEGIN
    -- Build header message
    header_msg := '
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   TARGET-SIDE MIGRATION EXECUTION                             ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🆔 MIGRATION ID: ' || :p_migration_id || '
📦 SHARED DATABASE: ' || :p_shared_database || '
🎯 TARGET DATABASE: ' || :p_target_database || '

🔄 EXECUTION PLAN:
   Step 1: Execute CTAS scripts (create tables with data)
   Step 2: Execute DDL scripts (create views only)

' || CHR(10);

    -- FIX #3: Execute CTAS FIRST (creates tables with data)
    CALL sp_execute_target_ctas(
        :p_migration_id,
        :p_shared_database,
        :p_shared_schema,
        :p_target_database,
        :p_admin_schema
    ) INTO :ctas_result;

    -- Then execute DDL for VIEWS ONLY (after tables exist)
    CALL sp_execute_target_ddl(
        :p_migration_id,
        :p_shared_database,
        :p_shared_schema,
        :p_target_database,
        :p_admin_schema
    ) INTO :ddl_result;

    -- Build final result message
    final_result := :header_msg || :ctas_result || CHR(10) || CHR(10) || :ddl_result || CHR(10) ||
        '
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         MIGRATION COMPLETED                                   ║
╚═══════════════════════════════════════════════════════════════════════════════╝

✅ Check ' || :p_target_database || '.' || :p_admin_schema || '.migration_execution_log for detailed logs
    ';

    RETURN :final_result;
END;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_full_migration';
