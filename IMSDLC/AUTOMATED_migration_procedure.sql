-- ============================================================================
-- IMSDLC - AUTOMATED MIGRATION STORED PROCEDURES
-- Description: Transform DDL, create objects, populate data, validate
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;
USE SCHEMA PUBLIC;

-- ============================================================================
-- PROCEDURE 1: Transform DDL (PROD_DB → DEV_DB)
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEV_DB.PUBLIC.SP_TRANSFORM_DDL(
    SOURCE_DDL VARCHAR,
    SOURCE_DB VARCHAR,
    TARGET_DB VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    transformed_ddl VARCHAR;
BEGIN
    -- Simple string replacement (case-sensitive)
    transformed_ddl := REPLACE(:SOURCE_DDL, :SOURCE_DB, :TARGET_DB);

    -- Handle lowercase
    transformed_ddl := REPLACE(transformed_ddl, LOWER(:SOURCE_DB), LOWER(:TARGET_DB));

    -- Handle uppercase
    transformed_ddl := REPLACE(transformed_ddl, UPPER(:SOURCE_DB), UPPER(:TARGET_DB));

    RETURN transformed_ddl;
END;
$$;

-- ============================================================================
-- PROCEDURE 2: Automated Table Creation from Share
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    SHARED_DATABASE VARCHAR,              -- Source shared database
    TARGET_DATABASE VARCHAR,              -- Target database (DEV_DB)
    SCHEMAS_TO_MIGRATE VARCHAR,           -- Comma-separated schemas
    CREATE_DATA BOOLEAN                   -- TRUE = populate data, FALSE = structure only
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    table_count INTEGER DEFAULT 0;
    result_msg VARCHAR DEFAULT '';
    sql_cmd VARCHAR;
    where_clause VARCHAR;

    table_cursor CURSOR FOR
        SELECT
            table_schema,
            table_name
        FROM IDENTIFIER(:SHARED_DATABASE || '.INFORMATION_SCHEMA.TABLES')
        WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_MIGRATE, ',')))
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;

    table_rec RECORD;

BEGIN
    result_msg := 'Starting table creation from share...\n';

    where_clause := CASE WHEN :CREATE_DATA THEN '' ELSE 'WHERE 1=0' END;

    OPEN table_cursor;
    FOR table_rec IN table_cursor DO
        BEGIN
            sql_cmd := 'CREATE OR REPLACE TABLE ' || :TARGET_DATABASE || '.' ||
                       table_rec.table_schema || '.' || table_rec.table_name ||
                       ' AS SELECT * FROM ' || :SHARED_DATABASE || '.' ||
                       table_rec.table_schema || '.' || table_rec.table_name ||
                       ' ' || where_clause;

            EXECUTE IMMEDIATE :sql_cmd;
            table_count := table_count + 1;
            result_msg := result_msg || 'Created: ' ||
                          table_rec.table_schema || '.' || table_rec.table_name || '\n';
        EXCEPTION
            WHEN OTHER THEN
                result_msg := result_msg || 'ERROR creating ' ||
                              table_rec.table_schema || '.' || table_rec.table_name ||
                              ': ' || SQLERRM || '\n';
        END;
    END FOR;
    CLOSE table_cursor;

    result_msg := result_msg || '\nTotal tables created: ' || table_count || '\n';

    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'PROCEDURE ERROR: ' || SQLERRM || '\n' || result_msg;
END;
$$;

-- ============================================================================
-- PROCEDURE 3: Automated Data Population from Share
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
    SHARED_DATABASE VARCHAR,
    TARGET_DATABASE VARCHAR,
    SCHEMAS_TO_MIGRATE VARCHAR,
    TRUNCATE_BEFORE_LOAD BOOLEAN
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    table_count INTEGER DEFAULT 0;
    total_rows_inserted INTEGER DEFAULT 0;
    result_msg VARCHAR DEFAULT '';
    sql_cmd VARCHAR;
    row_count_result RESULTSET;
    rows_inserted INTEGER;

    table_cursor CURSOR FOR
        SELECT
            table_schema,
            table_name
        FROM IDENTIFIER(:TARGET_DATABASE || '.INFORMATION_SCHEMA.TABLES')
        WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_MIGRATE, ',')))
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;

    table_rec RECORD;

BEGIN
    result_msg := 'Starting data population from share...\n';

    OPEN table_cursor;
    FOR table_rec IN table_cursor DO
        BEGIN
            -- Truncate if requested
            IF (:TRUNCATE_BEFORE_LOAD) THEN
                sql_cmd := 'TRUNCATE TABLE ' || :TARGET_DATABASE || '.' ||
                           table_rec.table_schema || '.' || table_rec.table_name;
                EXECUTE IMMEDIATE :sql_cmd;
            END IF;

            -- Insert data
            sql_cmd := 'INSERT INTO ' || :TARGET_DATABASE || '.' ||
                       table_rec.table_schema || '.' || table_rec.table_name ||
                       ' SELECT * FROM ' || :SHARED_DATABASE || '.' ||
                       table_rec.table_schema || '.' || table_rec.table_name;

            EXECUTE IMMEDIATE :sql_cmd;

            -- Get row count
            sql_cmd := 'SELECT COUNT(*) AS row_count FROM ' || :TARGET_DATABASE || '.' ||
                       table_rec.table_schema || '.' || table_rec.table_name;
            row_count_result := (EXECUTE IMMEDIATE :sql_cmd);

            LET c1 CURSOR FOR row_count_result;
            OPEN c1;
            FETCH c1 INTO rows_inserted;
            CLOSE c1;

            table_count := table_count + 1;
            total_rows_inserted := total_rows_inserted + rows_inserted;

            result_msg := result_msg || 'Populated: ' ||
                          table_rec.table_schema || '.' || table_rec.table_name ||
                          ' (' || rows_inserted || ' rows)\n';

        EXCEPTION
            WHEN OTHER THEN
                result_msg := result_msg || 'ERROR populating ' ||
                              table_rec.table_schema || '.' || table_rec.table_name ||
                              ': ' || SQLERRM || '\n';
        END;
    END FOR;
    CLOSE table_cursor;

    result_msg := result_msg || '\n=== DATA POPULATION COMPLETE ===\n';
    result_msg := result_msg || 'Tables populated: ' || table_count || '\n';
    result_msg := result_msg || 'Total rows inserted: ' || total_rows_inserted || '\n';

    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'PROCEDURE ERROR: ' || SQLERRM || '\n' || result_msg;
END;
$$;

-- ============================================================================
-- PROCEDURE 4: Automated Validation
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    SHARED_DATABASE VARCHAR,
    TARGET_DATABASE VARCHAR,
    SCHEMAS_TO_VALIDATE VARCHAR
)
RETURNS TABLE (
    validation_type VARCHAR,
    table_name VARCHAR,
    source_count INTEGER,
    target_count INTEGER,
    status VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH target_tables AS (
            SELECT table_schema, table_name
            FROM IDENTIFIER(:TARGET_DATABASE || '.INFORMATION_SCHEMA.TABLES')
            WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_VALIDATE, ',')))
              AND table_type = 'BASE TABLE'
        ),
        validation_results AS (
            SELECT
                'ROW_COUNT' AS validation_type,
                t.table_schema || '.' || t.table_name AS table_name,
                (SELECT COUNT(*) FROM IDENTIFIER(:SHARED_DATABASE || '.' || t.table_schema || '.' || t.table_name)) AS source_count,
                (SELECT COUNT(*) FROM IDENTIFIER(:TARGET_DATABASE || '.' || t.table_schema || '.' || t.table_name)) AS target_count
            FROM target_tables t
        )
        SELECT
            validation_type,
            table_name,
            source_count,
            target_count,
            CASE
                WHEN source_count = target_count THEN 'PASS'
                ELSE 'FAIL'
            END AS status
        FROM validation_results
        ORDER BY table_name
    );

    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- PROCEDURE 5: Complete Automated Migration Workflow
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION(
    SHARED_DATABASE VARCHAR,
    TARGET_DATABASE VARCHAR,
    SCHEMAS_TO_MIGRATE VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    result_msg VARCHAR DEFAULT '';
    step_result VARCHAR;
BEGIN
    result_msg := '=== AUTOMATED MIGRATION WORKFLOW ===\n\n';

    -- Step 1: Create tables (structure only)
    result_msg := result_msg || 'STEP 1: Creating table structures...\n';
    CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
        :SHARED_DATABASE,
        :TARGET_DATABASE,
        :SCHEMAS_TO_MIGRATE,
        FALSE
    ) INTO :step_result;
    result_msg := result_msg || step_result || '\n';

    -- Step 2: Populate data
    result_msg := result_msg || 'STEP 2: Populating data...\n';
    CALL DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
        :SHARED_DATABASE,
        :TARGET_DATABASE,
        :SCHEMAS_TO_MIGRATE,
        FALSE
    ) INTO :step_result;
    result_msg := result_msg || step_result || '\n';

    -- Step 3: Validate
    result_msg := result_msg || 'STEP 3: Validating migration...\n';
    result_msg := result_msg || 'Run: CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(...)\n\n';

    result_msg := result_msg || '=== MIGRATION WORKFLOW COMPLETE ===\n';

    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'WORKFLOW ERROR: ' || SQLERRM || '\n' || result_msg;
END;
$$;

-- ============================================================================
-- PROCEDURE 6: Generate Transformed DDL for Views and Procedures
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEV_DB.PUBLIC.SP_GENERATE_VIEW_PROCEDURE_DDL(
    SHARED_DATABASE VARCHAR,
    SOURCE_DATABASE_NAME VARCHAR,        -- 'PROD_DB'
    TARGET_DATABASE_NAME VARCHAR,        -- 'DEV_DB'
    SCHEMAS VARCHAR                      -- Comma-separated
)
RETURNS TABLE (
    object_schema VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    original_ddl VARCHAR,
    transformed_ddl VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    -- Note: This returns DDL that needs manual execution
    -- Views and procedures from source need to be recreated with transformed references

    res := (
        SELECT
            'MANUAL_EXECUTION_REQUIRED' AS object_schema,
            'DDL transformation for views/procedures requires manual extraction from source' AS object_name,
            'INFO' AS object_type,
            'Use IMCUST SP_EXTRACT_ALL_DDL procedure to get source DDL' AS original_ddl,
            'Then use SP_TRANSFORM_DDL to transform PROD_DB to DEV_DB' AS transformed_ddl
    );

    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- Example 1: Complete automated migration (tables only)
CALL DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- Example 2: Create table structures only
CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE  -- Structure only, no data
);

-- Example 3: Populate data separately
CALL DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE  -- Don't truncate before load
);

-- Example 4: Validate migration
CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- Example 5: Transform a single DDL statement
CALL DEV_DB.PUBLIC.SP_TRANSFORM_DDL(
    'CREATE TABLE PROD_DB.SCHEMA1.TABLE1 (COL1 INT)',
    'PROD_DB',
    'DEV_DB'
);
-- Returns: 'CREATE TABLE DEV_DB.SCHEMA1.TABLE1 (COL1 INT)'

-- Example 6: Create tables WITH data (single step)
CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    TRUE  -- Create and populate in one step
);
*/
