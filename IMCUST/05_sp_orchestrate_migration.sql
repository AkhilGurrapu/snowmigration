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

    // Step 2: Generate migration scripts (DDL + CTAS)
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

    // Step 3: Setup data share with database role
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

    // Get object counts by type and schema
    var get_stats = `
        SELECT
            source_schema,
            object_type,
            COUNT(*) as obj_count,
            MIN(dependency_level) as min_level,
            MAX(dependency_level) as max_level
        FROM ${P_SOURCE_DATABASE}.${P_ADMIN_SCHEMA}.migration_share_objects
        WHERE migration_id = ?
        GROUP BY source_schema, object_type
        ORDER BY source_schema, object_type
    `;
    stmt = snowflake.createStatement({
        sqlText: get_stats,
        binds: [migration_id]
    });
    var stats = stmt.execute();

    var schema_breakdown = '';
    while (stats.next()) {
        var schema = stats.getColumnValue('SOURCE_SCHEMA');
        var type = stats.getColumnValue('OBJECT_TYPE');
        var count = stats.getColumnValue('OBJ_COUNT');
        schema_breakdown += `   • ${schema}.${type}: ${count}\n`;
    }

    // Build detailed output message
    var result_msg = `
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   SOURCE-SIDE MIGRATION ORCHESTRATION                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🆔 MIGRATION ID: ${migration_id}

📦 SOURCE CONFIGURATION:
   • Database: ${P_SOURCE_DATABASE}
   • Initial Schema: ${P_SOURCE_SCHEMA}
   • Admin Schema: ${P_ADMIN_SCHEMA}
   • Requested Objects: ${P_OBJECT_LIST.length}

🎯 TARGET CONFIGURATION:
   • Database: ${P_TARGET_DATABASE}
   • Account: ${P_TARGET_ACCOUNT}
   • Share Name: ${P_SHARE_NAME}

${deps_message}

📂 OBJECT BREAKDOWN BY SCHEMA:
${schema_breakdown}
${scripts_message}

${share_message}

✅ STATUS: Migration preparation completed successfully
📋 Next Step: On target account, create shared database and run sp_execute_full_migration(${migration_id}, ...)
    `;

    return result_msg;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_orchestrate_migration';
