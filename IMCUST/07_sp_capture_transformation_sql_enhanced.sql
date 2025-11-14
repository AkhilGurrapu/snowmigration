-- ============================================
-- IMCUST (SOURCE) - Capture Transformation SQL (Enhanced)
-- ============================================
-- Purpose: Capture the original transformation SQL used to populate each DERIVED_TABLE
-- Uses ACCOUNT_USAGE.QUERY_HISTORY (365-day retention) to find INSERT/MERGE/CTAS statements
--
-- Capture Strategy (in priority order):
--   1. QUERY_HISTORY: Look for INSERT INTO, MERGE, or CTAS statements (confidence: 1.0)
--   2. CREATE TABLE: Look for initial CTAS during table creation (confidence: 0.9)
--   3. Most recent INSERT: Use most recent data load statement (confidence: 0.7)
--
-- Author: Enhanced Migration Framework v2.0
-- ============================================

CREATE OR REPLACE PROCEDURE PROD_DB.ADMIN_SCHEMA.sp_capture_transformation_sql_enhanced(
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

        // Get all DERIVED_TABLEs that need transformation SQL
        var get_derived_tables_sql = `
            SELECT
                source_database,
                source_schema,
                object_name,
                fully_qualified_name
            FROM ${database}.${admin_schema}.migration_share_objects
            WHERE migration_id = ${migration_id}
              AND object_classification = 'DERIVED_TABLE'
        `;

        var derived_tables = snowflake.execute({sqlText: get_derived_tables_sql});
        var captured_count = 0;
        var skipped_count = 0;

        while (derived_tables.next()) {
            var src_database = derived_tables.getColumnValue('SOURCE_DATABASE');
            var src_schema = derived_tables.getColumnValue('SOURCE_SCHEMA');
            var obj_name = derived_tables.getColumnValue('OBJECT_NAME');
            var fqn = derived_tables.getColumnValue('FULLY_QUALIFIED_NAME');

            var transformation_sql = null;
            var capture_method = 'NONE';
            var query_id = null;
            var confidence_score = 0.0;

            // Strategy 1: Look for INSERT INTO statements (highest confidence)
            var insert_query_sql = `
                SELECT
                    query_id,
                    query_text,
                    start_time
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE query_text ILIKE '%INSERT%INTO%${obj_name}%'
                  AND query_text ILIKE '%SELECT%'
                  AND database_name = '${src_database}'
                  AND schema_name = '${src_schema}'
                  AND execution_status = 'SUCCESS'
                  AND query_type IN ('INSERT', 'MERGE')
                ORDER BY start_time DESC
                LIMIT 1
            `;

            try {
                var insert_result = snowflake.execute({sqlText: insert_query_sql});
                if (insert_result.next()) {
                    transformation_sql = insert_result.getColumnValue('QUERY_TEXT');
                    query_id = insert_result.getColumnValue('QUERY_ID');
                    capture_method = 'QUERY_HISTORY_INSERT';
                    confidence_score = 1.0;
                }
            } catch (e) {
                // Query history may not be accessible, continue
            }

            // Strategy 2: Look for CREATE TABLE AS SELECT (if INSERT not found)
            if (transformation_sql == null) {
                var ctas_query_sql = `
                    SELECT
                        query_id,
                        query_text,
                        start_time
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE query_text ILIKE '%CREATE%TABLE%${obj_name}%AS%SELECT%'
                      AND database_name = '${src_database}'
                      AND schema_name = '${src_schema}'
                      AND execution_status = 'SUCCESS'
                      AND query_type = 'CREATE_TABLE_AS_SELECT'
                    ORDER BY start_time DESC
                    LIMIT 1
                `;

                try {
                    var ctas_result = snowflake.execute({sqlText: ctas_query_sql});
                    if (ctas_result.next()) {
                        transformation_sql = ctas_result.getColumnValue('QUERY_TEXT');
                        query_id = ctas_result.getColumnValue('QUERY_ID');
                        capture_method = 'QUERY_HISTORY_CTAS';
                        confidence_score = 0.9;
                    }
                } catch (e) {
                    // Continue
                }
            }

            // Strategy 3: Look for MERGE statements
            if (transformation_sql == null) {
                var merge_query_sql = `
                    SELECT
                        query_id,
                        query_text,
                        start_time
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE query_text ILIKE '%MERGE%INTO%${obj_name}%'
                      AND database_name = '${src_database}'
                      AND schema_name = '${src_schema}'
                      AND execution_status = 'SUCCESS'
                      AND query_type = 'MERGE'
                    ORDER BY start_time DESC
                    LIMIT 1
                `;

                try {
                    var merge_result = snowflake.execute({sqlText: merge_query_sql});
                    if (merge_result.next()) {
                        transformation_sql = merge_result.getColumnValue('QUERY_TEXT');
                        query_id = merge_result.getColumnValue('QUERY_ID');
                        capture_method = 'QUERY_HISTORY_MERGE';
                        confidence_score = 1.0;
                    }
                } catch (e) {
                    // Continue
                }
            }

            // Insert captured SQL into migration_transformation_sql table
            var insert_sql = `
                INSERT INTO ${database}.${admin_schema}.migration_transformation_sql (
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
                    ${transformation_sql ? "'" + transformation_sql.replace(/'/g, "''") + "'" : "NULL"},
                    '${capture_method}',
                    ${query_id ? "'" + query_id + "'" : "NULL"},
                    ${confidence_score}
                )
            `;

            snowflake.execute({sqlText: insert_sql});

            if (transformation_sql != null) {
                captured_count++;
            } else {
                skipped_count++;
            }
        }

        var summary = "Transformation SQL Capture Summary:\n";
        summary += "  Successfully captured: " + captured_count + " objects\n";
        summary += "  No SQL found: " + skipped_count + " objects\n";
        summary += "  (Objects with no SQL will use fallback strategies)\n";

        return summary;

    } catch (err) {
        return "ERROR in sp_capture_transformation_sql_enhanced: " + err.message + "\nSQL: " + err.stackTraceTxt;
    }
$$;

-- Example Usage:
-- CALL PROD_DB.ADMIN_SCHEMA.sp_capture_transformation_sql_enhanced(
--     2,                -- migration_id
--     'PROD_DB',        -- database
--     'ADMIN_SCHEMA'    -- admin_schema
-- );
