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
    p_object_list ARRAY
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var all_dependencies = new Set();
    var processed_objects = new Set();
    var objects_to_process = [];

    // Initialize with input objects
    for (var i = 0; i < P_OBJECT_LIST.length; i++) {
        objects_to_process.push({
            name: P_OBJECT_LIST[i],
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
        var get_lineage_sql = `
            SELECT
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
                var dep_name = result.getColumnValue('SOURCE_OBJECT_NAME');
                var dep_type = result.getColumnValue('SOURCE_OBJECT_DOMAIN');
                var distance = result.getColumnValue('DISTANCE');

                var dep_level = current.level + distance;
                if (dep_level > max_level) max_level = dep_level;

                all_dependencies.add(JSON.stringify({
                    name: dep_name,
                    type: dep_type,
                    level: dep_level
                }));

                // Extract object name from fully qualified name
                var obj_parts = dep_name.split('.');
                var obj_name = obj_parts[obj_parts.length - 1];

                objects_to_process.push({
                    name: obj_name,
                    level: dep_level
                });
            }
        } catch (err) {
            // Object might not support lineage (views, etc), skip
            continue;
        }
    }

    // Store all dependencies
    var insert_count = 0;
    all_dependencies.forEach(function(dep_json) {
        var dep = JSON.parse(dep_json);
        var insert_sql = `
            INSERT INTO migration_share_objects
            (migration_id, object_name, object_type, fully_qualified_name)
            VALUES (?, ?, ?, ?)
        `;
        var stmt = snowflake.createStatement({
            sqlText: insert_sql,
            binds: [P_MIGRATION_ID, dep.name, dep.type, dep.name]
        });
        stmt.execute();
        insert_count++;
    });

    return `Found ${insert_count} upstream dependencies across ${max_level} levels`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_get_upstream_dependencies';
