-- ============================================
-- IMCUST (SOURCE) - Stored Procedure: Get Upstream Dependencies (FIXED)
-- ============================================
-- Purpose: Discover all upstream dependencies for requested objects using GET_LINEAGE
-- GET_LINEAGE returns ALL transitive dependencies in ONE call - no recursion needed!

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;
USE SCHEMA ADMIN_SCHEMA;

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_get_upstream_dependencies(
    p_migration_id FLOAT,
    p_database VARCHAR,
    p_schema VARCHAR,        -- Initial schema hint for user-provided objects
    p_object_list_json VARCHAR  -- JSON array of object names to migrate
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var dep_map = new Map();  // Use Map to store unique objects (key = fqn, value = {database, schema, name, type, level})

    // Parse the JSON string to get the array of object names
    var object_list = JSON.parse(P_OBJECT_LIST_JSON);

    var max_level = 0;

    // Process each requested object
    for (var i = 0; i < object_list.length; i++) {
        var obj_name = object_list[i];
        var full_name = P_DATABASE + '.' + P_SCHEMA + '.' + obj_name;

        // Call GET_LINEAGE - it returns ALL transitive dependencies in ONE call!
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
                    5  -- Max depth to traverse
                )
            )
            WHERE SOURCE_OBJECT_NAME IS NOT NULL
              AND SOURCE_STATUS = 'ACTIVE'  -- Only include active objects, not deleted
        `;

        try {
            var stmt = snowflake.createStatement({sqlText: get_lineage_sql});
            var result = stmt.execute();

            // Store all dependencies returned (GET_LINEAGE gives us ALL levels at once)
            while (result.next()) {
                var dep_database = result.getColumnValue('SOURCE_OBJECT_DATABASE');
                var dep_schema = result.getColumnValue('SOURCE_OBJECT_SCHEMA');
                var dep_name = result.getColumnValue('SOURCE_OBJECT_NAME');
                var dep_type = result.getColumnValue('SOURCE_OBJECT_DOMAIN');
                var distance = result.getColumnValue('DISTANCE');

                if (distance > max_level) max_level = distance;

                var dep_full_name = dep_database + '.' + dep_schema + '.' + dep_name;
                var dep_key = dep_full_name;

                // Only add if not already present, or if this has a lower level (closer dependency)
                if (!dep_map.has(dep_key)) {
                    dep_map.set(dep_key, {
                        database: dep_database,
                        schema: dep_schema,
                        name: dep_name,
                        full_name: dep_full_name,
                        type: dep_type,
                        level: distance
                    });
                } else {
                    // If object already exists, keep the minimum level (closest dependency)
                    var existing = dep_map.get(dep_key);
                    if (distance < existing.level) {
                        existing.level = distance;
                    }
                }
            }
        } catch (err) {
            // Object might not support lineage, skip
            continue;
        }

        // Add the requested object itself with level 0
        // Detect object type
        var obj_type = 'TABLE';  // Default
        try {
            var type_check_sql = `
                SELECT CASE
                    WHEN COUNT(*) > 0 THEN 'VIEW'
                    ELSE 'TABLE'
                END as obj_type
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_CATALOG = UPPER('${P_DATABASE}')
                AND TABLE_SCHEMA = UPPER('${P_SCHEMA}')
                AND TABLE_NAME = UPPER('${obj_name}')
            `;
            var type_stmt = snowflake.createStatement({sqlText: type_check_sql});
            var type_result = type_stmt.execute();
            if (type_result.next()) {
                obj_type = type_result.getColumnValue('OBJ_TYPE');
            }
        } catch (err) {
            // Keep default 'TABLE'
        }

        // Add requested object with level 0
        var obj_key = `${P_DATABASE}.${P_SCHEMA}.${obj_name}`;
        if (!dep_map.has(obj_key)) {
            dep_map.set(obj_key, {
                database: P_DATABASE,
                schema: P_SCHEMA,
                name: obj_name,
                full_name: full_name,
                type: obj_type,
                level: 0
            });
        }
    }

    // Clear existing records for idempotency
    var delete_sql = `DELETE FROM migration_share_objects WHERE migration_id = ?`;
    var stmt = snowflake.createStatement({
        sqlText: delete_sql,
        binds: [P_MIGRATION_ID]
    });
    stmt.execute();

    // Insert all discovered unique dependencies
    var insert_count = 0;
    dep_map.forEach(function(dep, key) {
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

    return `Found ${insert_count} total objects (including ${object_list.length} requested objects and ${insert_count - object_list.length} dependencies) across ${max_level} levels`;
$$;

-- Test that procedure was created
SHOW PROCEDURES LIKE 'sp_get_upstream_dependencies';
