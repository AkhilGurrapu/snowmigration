-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Setup Data Share
-- ============================================
-- Purpose: Create database role, grant privileges, create share
-- This follows Snowflake best practices for data sharing

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_setup_data_share(
    p_migration_id FLOAT,
    p_database VARCHAR,
    p_share_name VARCHAR,
    p_target_account VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var db_role_name = 'MIGRATION_' + P_MIGRATION_ID + '_ROLE';

    // Step 1: Create database role
    var create_role_sql = `
        USE DATABASE ${P_DATABASE};
        CREATE DATABASE ROLE IF NOT EXISTS ${db_role_name};
    `;
    var stmt = snowflake.createStatement({sqlText: create_role_sql});
    stmt.execute();

    // Step 2: Grant SELECT on all dependency objects to database role
    var get_objects = `
        SELECT DISTINCT fully_qualified_name, object_type
        FROM migration_share_objects
        WHERE migration_id = ?
    `;
    stmt = snowflake.createStatement({
        sqlText: get_objects,
        binds: [P_MIGRATION_ID]
    });
    var objects = stmt.execute();

    var grant_count = 0;
    var schema_set = new Set();

    while (objects.next()) {
        var fqn = objects.getColumnValue('FULLY_QUALIFIED_NAME');
        var obj_type = objects.getColumnValue('OBJECT_TYPE');

        // Extract schema for USAGE grant
        var parts = fqn.split('.');
        if (parts.length >= 2) {
            schema_set.add(parts[0] + '.' + parts[1]);
        }

        // Grant SELECT on object
        var grant_sql = `GRANT SELECT ON ${fqn} TO DATABASE ROLE ${db_role_name}`;
        try {
            stmt = snowflake.createStatement({sqlText: grant_sql});
            stmt.execute();
            grant_count++;
        } catch (err) {
            continue;
        }
    }

    // Grant USAGE on schemas
    schema_set.forEach(function(schema_fqn) {
        var grant_usage = `GRANT USAGE ON SCHEMA ${schema_fqn} TO DATABASE ROLE ${db_role_name}`;
        stmt = snowflake.createStatement({sqlText: grant_usage});
        stmt.execute();
    });

    // Grant SELECT on migration metadata tables to the database role
    var metadata_tables = [
        'migration_config',
        'migration_ddl_scripts',
        'migration_ctas_scripts',
        'migration_share_objects'
    ];

    for (var i = 0; i < metadata_tables.length; i++) {
        var grant_meta = `GRANT SELECT ON TABLE ${P_DATABASE}.mart_investments_bolt.${metadata_tables[i]} TO DATABASE ROLE ${db_role_name}`;
        stmt = snowflake.createStatement({sqlText: grant_meta});
        stmt.execute();
    }

    // Step 3: Create share
    var create_share_sql = `CREATE SHARE IF NOT EXISTS ${P_SHARE_NAME}`;
    stmt = snowflake.createStatement({sqlText: create_share_sql});
    stmt.execute();

    // Step 4: Grant database usage to share
    var grant_db = `GRANT USAGE ON DATABASE ${P_DATABASE} TO SHARE ${P_SHARE_NAME}`;
    stmt = snowflake.createStatement({sqlText: grant_db});
    stmt.execute();

    // Step 5: Grant database role to share
    var grant_role_to_share = `GRANT DATABASE ROLE ${db_role_name} TO SHARE ${P_SHARE_NAME}`;
    stmt = snowflake.createStatement({sqlText: grant_role_to_share});
    stmt.execute();

    // Step 6: Add target account to share
    var add_account = `ALTER SHARE ${P_SHARE_NAME} ADD ACCOUNTS = ${P_TARGET_ACCOUNT}`;
    try {
        stmt = snowflake.createStatement({sqlText: add_account});
        stmt.execute();
    } catch (err) {
        // Account might already be added
    }

    return `Created share '${P_SHARE_NAME}' with database role '${db_role_name}' and granted ${grant_count} objects. Target account: ${P_TARGET_ACCOUNT}`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_setup_data_share';
