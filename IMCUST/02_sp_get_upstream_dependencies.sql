-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Get Upstream Dependencies
-- ============================================
-- Purpose: Find all upstream dependencies using SNOWFLAKE.CORE.GET_LINEAGE
-- This procedure recursively discovers all objects that the target objects depend on

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

CREATE OR REPLACE PROCEDURE sp_get_upstream_dependencies(
    p_migration_id FLOAT,
    p_database VARCHAR,
    p_schema VARCHAR,
    p_object_list_json VARCHAR  -- Changed to VARCHAR to accept JSON string
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var all_dependencies = new Set();
    var processed_objects = new Set();
    var objects_to_process = [];

    // Parse the JSON string to get the array
    var object_list = JSON.parse(P_OBJECT_LIST_JSON);

    // Initialize with input objects
    for (var i = 0; i < object_list.length; i++) {
        objects_to_process.push({
            name: object_list[i],
            level: 0
        });
    }

    var max_level = 0;

    // Breadth-first search for dependencies
    while (objects_to_process.length > 0) {
        var current = objects_to_process.shift();
        var full_name = P_DATABASE + '.' + P_SCHEMA + '.' + current.name;

        if (processed_objects.has(full_name)) {
            continue;
        }

        processed_objects.add(full_name);

        // Get upstream dependencies using GET_LINEAGE
        // Note: Using 'TABLE' works for both tables and views in GET_LINEAGE
        var get_lineage_sql = `
            SELECT
                SOURCE_OBJECT_DATABASE,
                SOURCE_OBJECT_SCHEMA,
                SOURCE_OBJECT_NAME,
                SOURCE_OBJECT_DOMAIN,
                DISTANCE
            FROM TABLE(
                SNOWFLAKE.CORE.GET_LINEAGE(
                    '${full_name}',
                    'TABLE',
                    'UPSTREAM',
                    5
                )
            )
            WHERE SOURCE_OBJECT_NAME IS NOT NULL
        `;

        try {
            var stmt = snowflake.createStatement({sqlText: get_lineage_sql});
            var result = stmt.execute();

            while (result.next()) {
                var dep_database = result.getColumnValue('SOURCE_OBJECT_DATABASE');
                var dep_schema = result.getColumnValue('SOURCE_OBJECT_SCHEMA');
                var dep_name = result.getColumnValue('SOURCE_OBJECT_NAME');
                var dep_type = result.getColumnValue('SOURCE_OBJECT_DOMAIN');
                var distance = result.getColumnValue('DISTANCE');

                var dep_level = current.level + distance;
                if (dep_level > max_level) max_level = dep_level;

                var dep_full_name = dep_database + '.' + dep_schema + '.' + dep_name;

                all_dependencies.add(JSON.stringify({
                    database: dep_database,
                    schema: dep_schema,
                    name: dep_name,
                    full_name: dep_full_name,
                    type: dep_type,
                    level: dep_level
                }));

                // Add to processing queue
                objects_to_process.push({
                    name: dep_name,
                    level: dep_level
                });
            }
        } catch (err) {
            // Object might not support lineage (views, etc), skip
            continue;
        }
    }

    // Clear any existing records for this migration_id to ensure idempotency
    var delete_sql = `DELETE FROM migration_share_objects WHERE migration_id = ?`;
    var stmt = snowflake.createStatement({
        sqlText: delete_sql,
        binds: [P_MIGRATION_ID]
    });
    stmt.execute();

    // Store all dependencies with dependency level from GET_LINEAGE distance
    var insert_count = 0;
    all_dependencies.forEach(function(dep_json) {
        var dep = JSON.parse(dep_json);
        var insert_sql = `
            INSERT INTO migration_share_objects
            (migration_id, source_database, source_schema, object_name, object_type, fully_qualified_name, dependency_level)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `;
        var stmt = snowflake.createStatement({
            sqlText: insert_sql,
            binds: [P_MIGRATION_ID, dep.database, dep.schema, dep.name, dep.type, dep.full_name, dep.level]
        });
        stmt.execute();
        insert_count++;
    });

    return `Found ${insert_count} upstream dependencies across ${max_level} levels`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_get_upstream_dependencies';
