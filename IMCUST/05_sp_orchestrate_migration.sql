-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Main Orchestration
-- ============================================
-- Purpose: Single entry point that orchestrates the entire migration process
-- This is the main procedure you call to start a migration

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;
USE SCHEMA ADMIN_SCHEMA;

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
    p_source_database VARCHAR,
    p_source_schema VARCHAR,        -- Initial schema for object lookup only
    p_admin_schema VARCHAR,         -- Schema where metadata tables are stored
    p_target_database VARCHAR,
    -- p_target_schema REMOVED: Schema mapping is AUTOMATIC based on SOURCE_OBJECT_SCHEMA from GET_LINEAGE
    p_object_list ARRAY,
    p_share_name VARCHAR,
    p_target_account VARCHAR  -- Target Snowflake account identifier (e.g., 'IMSDLC', 'ORG123.ACCT456')
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Insert migration request using INSERT ... SELECT to support PARSE_JSON
    var jsonStr = JSON.stringify(P_OBJECT_LIST).replace(/'/g, "''");  // Escape single quotes
    var insert_config = `
        INSERT INTO ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.migration_config
        (source_database, source_schema, target_database, target_schema, object_list, status)
        SELECT ?, ?, ?, null, PARSE_JSON('${jsonStr}'), 'IN_PROGRESS'
    `;

    var stmt = snowflake.createStatement({
        sqlText: insert_config,
        binds: [P_SOURCE_DATABASE, P_SOURCE_SCHEMA, P_TARGET_DATABASE]
    });
    stmt.execute();

    // Get migration_id
    var get_id = `SELECT MAX(migration_id) as mid FROM ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.migration_config`;
    stmt = snowflake.createStatement({sqlText: get_id});
    var result = stmt.execute();
    result.next();
    var migration_id = result.getColumnValue('MID');

    // Step 1: Get all upstream dependencies
    var call_deps = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_get_upstream_dependencies(?, ?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_deps,
        binds: [migration_id, P_SOURCE_DATABASE, P_SOURCE_SCHEMA, JSON.stringify(P_OBJECT_LIST)]
    });
    var deps_result = stmt.execute();
    deps_result.next();
    var deps_message = deps_result.getColumnValue(1);

    // Step 2: Classify objects (BASE_TABLE, DERIVED_TABLE, VIEW)
    var call_classify = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_classify_migration_objects(?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_classify,
        binds: [migration_id, P_SOURCE_DATABASE, P_ADMIN_SCHEMA]
    });
    var classify_result = stmt.execute();
    classify_result.next();
    var classify_message = classify_result.getColumnValue(1);

    // Step 3: Capture transformation SQL from query history (365-day retention)
    var call_capture = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_capture_transformation_sql_enhanced(?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_capture,
        binds: [migration_id, P_SOURCE_DATABASE, P_ADMIN_SCHEMA]
    });
    var capture_result = stmt.execute();
    capture_result.next();
    var capture_message = capture_result.getColumnValue(1);

    // Step 4: Extract lineage from metadata (fallback for objects without query history)
    var call_extract = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_extract_lineage_from_metadata(?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_extract,
        binds: [migration_id, P_SOURCE_DATABASE, P_ADMIN_SCHEMA]
    });
    var extract_result = stmt.execute();
    extract_result.next();
    var extract_message = extract_result.getColumnValue(1);

    // Step 5: Generate hybrid migration scripts (LINEAGE PRESERVING!)
    var call_hybrid = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_generate_hybrid_migration_scripts(?, ?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_hybrid,
        binds: [migration_id, P_SOURCE_DATABASE, P_TARGET_DATABASE, P_ADMIN_SCHEMA]
    });
    var hybrid_result = stmt.execute();
    hybrid_result.next();
    var hybrid_message = hybrid_result.getColumnValue(1);

    // Step 6: Generate legacy migration scripts (DDL + CTAS) - for backward compatibility
    // Note: p_target_schema parameter exists but is NOT used for schema mapping
    // Schema mapping is automatic based on source_schema from migration_share_objects
    var call_scripts = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_generate_migration_scripts(?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_scripts,
        binds: [migration_id, P_TARGET_DATABASE, null]  // null for unused target_schema
    });
    var scripts_result = stmt.execute();
    scripts_result.next();
    var scripts_message = scripts_result.getColumnValue(1);

    // Step 7: Setup data share with database role
    var call_share = `CALL ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.sp_setup_data_share(?, ?, ?, ?, ?, ?)`;
    stmt = snowflake.createStatement({
        sqlText: call_share,
        binds: [migration_id, P_SOURCE_DATABASE, P_SOURCE_SCHEMA, P_ADMIN_SCHEMA, P_SHARE_NAME, P_TARGET_ACCOUNT]
    });
    var share_result = stmt.execute();
    share_result.next();
    var share_message = share_result.getColumnValue(1);

    // Update status
    var update_status = `UPDATE ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.migration_config SET status = 'COMPLETED' WHERE migration_id = ?`;
    stmt = snowflake.createStatement({
        sqlText: update_status,
        binds: [migration_id]
    });
    stmt.execute();

    return `Migration ID: ${migration_id}\n\n${deps_message}\n\n${classify_message}\n\n${capture_message}\n\n${extract_message}\n\n${hybrid_message}\n\n${scripts_message}\n\n${share_message}`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_orchestrate_migration';
