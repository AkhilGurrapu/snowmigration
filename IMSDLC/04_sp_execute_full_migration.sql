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
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var validation_msg = 'Starting migration ' + P_MIGRATION_ID + ' from shared database ' + P_SHARED_DATABASE + '\n';
    var ddl_result = '';
    var data_result = '';

    // Step 2: Execute DDL scripts (create all object structures)
    var ddl_sql = `
        CALL ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.sp_execute_target_ddl(
            ${P_MIGRATION_ID},
            '${P_SHARED_DATABASE}',
            '${P_SHARED_SCHEMA}',
            '${P_TARGET_DATABASE}',
            '${P_ADMIN_SCHEMA}'
        )
    `;
    var ddl_stmt = snowflake.createStatement({sqlText: ddl_sql});
    var ddl_result_set = ddl_stmt.execute();
    if (ddl_result_set.next()) {
        ddl_result = ddl_result_set.getColumnValue(1);
    }

    // Step 3: Check if hybrid migration scripts exist
    var check_hybrid_sql = `
        SELECT COUNT(*) as count
        FROM ${P_SHARED_DATABASE}.${P_SHARED_SCHEMA}.migration_hybrid_scripts
        WHERE migration_id = ${P_MIGRATION_ID}
    `;
    var check_stmt = snowflake.createStatement({sqlText: check_hybrid_sql});
    var check_result = check_stmt.execute();
    var hybrid_scripts_exist = false;
    if (check_result.next()) {
        hybrid_scripts_exist = check_result.getColumnValue('COUNT') > 0;
    }

    // Step 4: Execute data migration (prefer hybrid approach for lineage preservation)
    if (hybrid_scripts_exist) {
        validation_msg += '✓ Using HYBRID migration (preserves native lineage)\n';

        var hybrid_sql = `
            CALL ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.sp_execute_hybrid_migration(
                ${P_MIGRATION_ID},
                '${P_SHARED_DATABASE}',
                '${P_SHARED_SCHEMA}',
                '${P_TARGET_DATABASE}',
                '${P_ADMIN_SCHEMA}'
            )
        `;
        var hybrid_stmt = snowflake.createStatement({sqlText: hybrid_sql});
        var hybrid_result_set = hybrid_stmt.execute();
        if (hybrid_result_set.next()) {
            data_result = hybrid_result_set.getColumnValue(1);
        }
    } else {
        validation_msg += '⚠ Using legacy CTAS migration (lineage NOT preserved)\n';

        var ctas_sql = `
            CALL ${P_TARGET_DATABASE}.${P_ADMIN_SCHEMA}.sp_execute_target_ctas(
                ${P_MIGRATION_ID},
                '${P_SHARED_DATABASE}',
                '${P_SHARED_SCHEMA}',
                '${P_TARGET_DATABASE}',
                '${P_ADMIN_SCHEMA}'
            )
        `;
        var ctas_stmt = snowflake.createStatement({sqlText: ctas_sql});
        var ctas_result_set = ctas_stmt.execute();
        if (ctas_result_set.next()) {
            data_result = ctas_result_set.getColumnValue(1);
        }
    }

    return validation_msg + ddl_result + '\n' + data_result;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_execute_full_migration';
