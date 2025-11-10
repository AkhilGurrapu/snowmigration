-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Main Orchestration
-- ============================================
-- Purpose: Single entry point that orchestrates the entire migration process
-- This is the main procedure you call to start a migration

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_orchestrate_migration(
    p_source_database VARCHAR,
    p_source_schema VARCHAR,
    p_target_database VARCHAR,
    p_target_schema VARCHAR,
    p_object_list ARRAY,
    p_share_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // Insert migration request
    var insert_config = `
        INSERT INTO migration_config
        (source_database, source_schema, target_database, target_schema, object_list, status)
        VALUES (?, ?, ?, ?, PARSE_JSON(?), 'IN_PROGRESS')
    `;

    var stmt = snowflake.createStatement({
        sqlText: insert_config,
        binds: [P_SOURCE_DATABASE, P_SOURCE_SCHEMA, P_TARGET_DATABASE, P_TARGET_SCHEMA, JSON.stringify(P_OBJECT_LIST)]
    });
    stmt.execute();

    // Get migration_id
    var get_id = `SELECT MAX(migration_id) as mid FROM migration_config`;
    stmt = snowflake.createStatement({sqlText: get_id});
    var result = stmt.execute();
    result.next();
    var migration_id = result.getColumnValue('MID');

    // Step 1: Get all upstream dependencies
    var call_deps = `CALL sp_get_upstream_dependencies(?, ?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_deps,
        binds: [migration_id, P_SOURCE_DATABASE, P_SOURCE_SCHEMA, JSON.stringify(P_OBJECT_LIST)]
    });
    var deps_result = stmt.execute();
    deps_result.next();
    var deps_message = deps_result.getColumnValue(1);

    // Step 2: Generate migration scripts (DDL + CTAS)
    var call_scripts = `CALL sp_generate_migration_scripts(?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_scripts,
        binds: [migration_id, P_TARGET_DATABASE, P_TARGET_SCHEMA]
    });
    var scripts_result = stmt.execute();
    scripts_result.next();
    var scripts_message = scripts_result.getColumnValue(1);

    // Step 3: Setup data share with database role
    var call_share = `CALL sp_setup_data_share(?, ?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_share,
        binds: [migration_id, P_SOURCE_DATABASE, P_SHARE_NAME, 'IMSDLC']  // Target account
    });
    var share_result = stmt.execute();
    share_result.next();
    var share_message = share_result.getColumnValue(1);

    // Update status
    var update_status = `UPDATE migration_config SET status = 'COMPLETED' WHERE migration_id = ?`;
    stmt = snowflake.createStatement({
        sqlText: update_status,
        binds: [migration_id]
    });
    stmt.execute();

    return `Migration ID: ${migration_id}\n${deps_message}\n${scripts_message}\n${share_message}`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_orchestrate_migration';
