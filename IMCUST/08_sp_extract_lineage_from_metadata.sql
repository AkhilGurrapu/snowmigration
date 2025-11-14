-- ============================================
-- IMCUST (SOURCE) - Extract Lineage from Metadata (Fallback)
-- ============================================
-- Purpose: Extract transformation logic from table metadata when query history is unavailable
-- Uses ACCESS_HISTORY to reconstruct SQL from column lineage
--
-- Fallback Strategy (for objects without captured SQL):
--   1. ACCESS_HISTORY: Reconstruct SQL from column lineage (confidence: 0.5)
--   2. OBJECT_DEPENDENCIES: Build simple JOIN statement from dependencies (confidence: 0.3)
--   3. COMMENT: Extract SQL from table comment if documented (confidence: 0.6)
--   4. TAG: Extract SQL from custom 'TRANSFORMATION_SQL' tag (confidence: 0.8)
--
-- Author: Enhanced Migration Framework v2.0
-- ============================================

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_extract_lineage_from_metadata(
    p_migration_id FLOAT,
    p_database VARCHAR,
    p_admin_schema VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        var migration_id = P_MIGRATION_ID;
        var database = P_DATABASE;
        var admin_schema = P_ADMIN_SCHEMA;

        // Get DERIVED_TABLEs that still have no transformation SQL
        var get_missing_sql_sql = `
            SELECT
                mso.source_database,
                mso.source_schema,
                mso.object_name,
                mso.fully_qualified_name,
                mso.dependency_level
            FROM ${database}.${admin_schema}.migration_share_objects mso
            LEFT JOIN ${database}.${admin_schema}.migration_transformation_sql mts
                ON mso.migration_id = mts.migration_id
                AND mso.object_name = mts.object_name
            WHERE mso.migration_id = ${migration_id}
              AND mso.object_classification = 'DERIVED_TABLE'
              AND (mts.transformation_sql IS NULL OR mts.capture_method = 'NONE')
        `;

        var missing_sql_objects = snowflake.execute({sqlText: get_missing_sql_sql});
        var fallback_count = 0;
        var failed_count = 0;

        while (missing_sql_objects.next()) {
            var src_database = missing_sql_objects.getColumnValue('SOURCE_DATABASE');
            var src_schema = missing_sql_objects.getColumnValue('SOURCE_SCHEMA');
            var obj_name = missing_sql_objects.getColumnValue('OBJECT_NAME');
            var fqn = missing_sql_objects.getColumnValue('FULLY_QUALIFIED_NAME');

            var transformation_sql = null;
            var capture_method = 'NONE';
            var confidence_score = 0.0;

            // Strategy 1: Check for TRANSFORMATION_SQL tag
            try {
                var tag_sql = `
                    SELECT TAG_VALUE
                    FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(
                        '${fqn}',
                        'TABLE'
                    ))
                    WHERE TAG_NAME = 'TRANSFORMATION_SQL'
                `;
                var tag_result = snowflake.execute({sqlText: tag_sql});
                if (tag_result.next()) {
                    transformation_sql = tag_result.getColumnValue('TAG_VALUE');
                    capture_method = 'TAG';
                    confidence_score = 0.8;
                }
            } catch (e) {
                // Tag doesn't exist, continue
            }

            // Strategy 2: Check table COMMENT for SQL
            if (transformation_sql == null) {
                try {
                    var comment_sql = `
                        SELECT COMMENT
                        FROM ${src_database}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '${src_schema}'
                          AND TABLE_NAME = '${obj_name}'
                          AND COMMENT IS NOT NULL
                          AND COMMENT ILIKE '%SELECT%'
                    `;
                    var comment_result = snowflake.execute({sqlText: comment_sql});
                    if (comment_result.next()) {
                        var comment = comment_result.getColumnValue('COMMENT');
                        // Check if comment contains SQL (has SELECT keyword)
                        if (comment && comment.toUpperCase().includes('SELECT')) {
                            transformation_sql = comment;
                            capture_method = 'COMMENT';
                            confidence_score = 0.6;
                        }
                    }
                } catch (e) {
                    // Continue
                }
            }

            // Strategy 3: Use ACCESS_HISTORY to reconstruct SQL from column lineage
            if (transformation_sql == null) {
                try {
                    var access_history_sql = `
                        SELECT
                            query_id,
                            base_objects_accessed,
                            objects_modified
                        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
                        WHERE ARRAY_SIZE(objects_modified) > 0
                          AND objects_modified[0]:objectName::STRING ILIKE '%${obj_name}%'
                          AND ARRAY_SIZE(base_objects_accessed) > 0
                        ORDER BY query_start_time DESC
                        LIMIT 1
                    `;
                    var access_result = snowflake.execute({sqlText: access_history_sql});
                    if (access_result.next()) {
                        var base_objects = access_result.getColumnValue('BASE_OBJECTS_ACCESSED');
                        // Reconstruct simple SQL from lineage
                        // Note: This is a simplified reconstruction - won't capture complex logic
                        var source_tables = [];
                        if (base_objects && base_objects.length > 0) {
                            for (var i = 0; i < base_objects.length; i++) {
                                var obj = base_objects[i];
                                if (obj.objectName) {
                                    source_tables.push(obj.objectName);
                                }
                            }
                        }
                        if (source_tables.length > 0) {
                            transformation_sql = "-- Reconstructed from ACCESS_HISTORY\\n";
                            transformation_sql += "-- NOTE: This is a simplified reconstruction. ";
                            transformation_sql += "Review and adjust as needed.\\n";
                            transformation_sql += "INSERT INTO " + fqn + "\\n";
                            transformation_sql += "SELECT * FROM " + source_tables.join("\\n  JOIN ");
                            capture_method = 'ACCESS_HISTORY';
                            confidence_score = 0.5;
                        }
                    }
                } catch (e) {
                    // ACCESS_HISTORY may not be available
                }
            }

            // Strategy 4: Build simple SQL from OBJECT_DEPENDENCIES (lowest confidence)
            if (transformation_sql == null) {
                try {
                    // Get upstream dependencies from migration_share_objects
                    var deps_sql = `
                        SELECT object_name, fully_qualified_name
                        FROM ${database}.${admin_schema}.migration_share_objects
                        WHERE migration_id = ${migration_id}
                          AND dependency_level > (
                              SELECT dependency_level
                              FROM ${database}.${admin_schema}.migration_share_objects
                              WHERE migration_id = ${migration_id}
                                AND object_name = '${obj_name}'
                          )
                        ORDER BY dependency_level DESC
                        LIMIT 5
                    `;
                    var deps_result = snowflake.execute({sqlText: deps_sql});
                    var dep_tables = [];
                    while (deps_result.next()) {
                        dep_tables.push(deps_result.getColumnValue('FULLY_QUALIFIED_NAME'));
                    }

                    if (dep_tables.length > 0) {
                        transformation_sql = "-- Generated from object dependencies\\n";
                        transformation_sql += "-- WARNING: This is a generic template. ";
                        transformation_sql += "YOU MUST customize with actual business logic!\\n";
                        transformation_sql += "INSERT INTO " + fqn + "\\n";
                        transformation_sql += "SELECT * FROM " + dep_tables[0];
                        capture_method = 'OBJECT_DEPENDENCIES';
                        confidence_score = 0.3;
                    }
                } catch (e) {
                    // Continue
                }
            }

            // Update or insert the transformation SQL
            if (transformation_sql != null) {
                var upsert_sql = `
                    MERGE INTO ${database}.${admin_schema}.migration_transformation_sql tgt
                    USING (
                        SELECT
                            ${migration_id} as migration_id,
                            '${src_database}' as source_database,
                            '${src_schema}' as source_schema,
                            '${obj_name}' as object_name
                    ) src
                    ON tgt.migration_id = src.migration_id
                       AND tgt.object_name = src.object_name
                    WHEN MATCHED THEN
                        UPDATE SET
                            transformation_sql = '${transformation_sql.replace(/'/g, "''")}',
                            capture_method = '${capture_method}',
                            confidence_score = ${confidence_score}
                    WHEN NOT MATCHED THEN
                        INSERT (
                            migration_id,
                            source_database,
                            source_schema,
                            object_name,
                            object_type,
                            transformation_sql,
                            capture_method,
                            query_id,
                            confidence_score
                        ) VALUES (
                            ${migration_id},
                            '${src_database}',
                            '${src_schema}',
                            '${obj_name}',
                            'TABLE',
                            '${transformation_sql.replace(/'/g, "''")}',
                            '${capture_method}',
                            NULL,
                            ${confidence_score}
                        )
                `;
                snowflake.execute({sqlText: upsert_sql});
                fallback_count++;
            } else {
                failed_count++;
            }
        }

        var summary = "Metadata Extraction Summary (Fallback):\n";
        summary += "  Extracted from metadata: " + fallback_count + " objects\n";
        summary += "  No metadata found: " + failed_count + " objects\n";
        if (failed_count > 0) {
            summary += "  (Objects with no metadata will use simple CTAS as last resort)\n";
        }

        return summary;

    } catch (err) {
        return "ERROR in sp_extract_lineage_from_metadata: " + err.message + "\nSQL: " + err.stackTraceTxt;
    }
$$;

-- Example Usage:
-- CALL PROD_DB.ADMIN_SCHEMA.sp_extract_lineage_from_metadata(
--     2,                -- migration_id
--     'PROD_DB',        -- database
--     'ADMIN_SCHEMA'    -- admin_schema
-- );
