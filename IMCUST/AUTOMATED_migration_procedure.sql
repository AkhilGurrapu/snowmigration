-- ============================================================================
-- IMCUST - AUTOMATED MIGRATION STORED PROCEDURE
-- Description: Discover dependencies, extract DDL, create/update share
-- Usage: CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
--            'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
--            'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS,VW_CURRENT_HOLDINGS,SP_LOAD_DIM_STOCKS,SP_CALCULATE_DAILY_POSITIONS',
--            'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
--            'nfmyizv.imsdlc'
--        );
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    SCHEMAS_TO_MIGRATE VARCHAR,           -- Comma-separated list: 'SCHEMA1,SCHEMA2'
    BASE_OBJECTS VARCHAR,                 -- Comma-separated list: 'TABLE1,TABLE2,VIEW1'
    SHARE_NAME VARCHAR,                   -- Name of share to create
    TARGET_ACCOUNT VARCHAR                -- Format: 'org_name.account_name'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    share_exists BOOLEAN DEFAULT FALSE;
    table_count INTEGER DEFAULT 0;
    dependency_count INTEGER DEFAULT 0;
    result_msg VARCHAR DEFAULT '';

    -- Cursor variables
    table_cursor CURSOR FOR
        WITH split_schemas AS (
            SELECT TRIM(VALUE) AS schema_name
            FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_MIGRATE, ','))
        ),
        split_objects AS (
            SELECT TRIM(VALUE) AS object_name
            FROM TABLE(SPLIT_TO_TABLE(:BASE_OBJECTS, ','))
        ),
        all_dependencies AS (
            SELECT DISTINCT
                od.referenced_database_name,
                od.referenced_schema_name,
                od.referenced_object_name,
                od.referenced_object_domain
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            WHERE od.referencing_database_name = 'PROD_DB'
              AND od.referencing_schema_name IN (SELECT schema_name FROM split_schemas)
              AND od.referencing_object_name IN (SELECT object_name FROM split_objects)
              AND od.referenced_object_domain = 'TABLE'
              AND od.referenced_database_name = 'PROD_DB'
              AND od.referenced_schema_name IN (SELECT schema_name FROM split_schemas)

            UNION

            SELECT DISTINCT
                t.table_catalog,
                t.table_schema,
                t.table_name,
                'TABLE' AS object_domain
            FROM PROD_DB.INFORMATION_SCHEMA.TABLES t
            CROSS JOIN split_schemas ss
            WHERE t.table_schema = ss.schema_name
              AND t.table_type = 'BASE TABLE'
              AND t.table_name IN (SELECT object_name FROM split_objects)
        )
        SELECT
            referenced_database_name AS db_name,
            referenced_schema_name AS schema_name,
            referenced_object_name AS table_name
        FROM all_dependencies
        WHERE referenced_object_domain = 'TABLE';

    table_rec RECORD;
    sql_cmd VARCHAR;

BEGIN
    result_msg := 'Starting migration preparation...\n';

    -- -------------------------------------------------------------------------
    -- STEP 1: Create or replace share
    -- -------------------------------------------------------------------------
    sql_cmd := 'CREATE SHARE IF NOT EXISTS ' || :SHARE_NAME ||
               ' COMMENT = ''Automated migration share: PROD_DB to target''';
    EXECUTE IMMEDIATE :sql_cmd;
    result_msg := result_msg || 'Share created: ' || :SHARE_NAME || '\n';

    -- -------------------------------------------------------------------------
    -- STEP 2: Grant database usage
    -- -------------------------------------------------------------------------
    sql_cmd := 'GRANT USAGE ON DATABASE PROD_DB TO SHARE ' || :SHARE_NAME;
    EXECUTE IMMEDIATE :sql_cmd;
    result_msg := result_msg || 'Granted database usage\n';

    -- -------------------------------------------------------------------------
    -- STEP 3: Grant schema usage for each schema
    -- -------------------------------------------------------------------------
    FOR schema_rec IN (SELECT TRIM(VALUE) AS schema_name
                       FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_MIGRATE, ','))) DO
        sql_cmd := 'GRANT USAGE ON SCHEMA PROD_DB.' || schema_rec.schema_name ||
                   ' TO SHARE ' || :SHARE_NAME;
        EXECUTE IMMEDIATE :sql_cmd;
        result_msg := result_msg || 'Granted schema usage: ' || schema_rec.schema_name || '\n';
    END FOR;

    -- -------------------------------------------------------------------------
    -- STEP 4: Grant SELECT on all discovered tables
    -- -------------------------------------------------------------------------
    table_count := 0;

    OPEN table_cursor;
    FOR table_rec IN table_cursor DO
        BEGIN
            sql_cmd := 'GRANT SELECT ON TABLE PROD_DB.' ||
                       table_rec.schema_name || '.' || table_rec.table_name ||
                       ' TO SHARE ' || :SHARE_NAME;
            EXECUTE IMMEDIATE :sql_cmd;
            table_count := table_count + 1;
            result_msg := result_msg || 'Added to share: ' ||
                          table_rec.schema_name || '.' || table_rec.table_name || '\n';
        EXCEPTION
            WHEN OTHER THEN
                result_msg := result_msg || 'WARNING: Could not add ' ||
                              table_rec.schema_name || '.' || table_rec.table_name ||
                              ' - ' || SQLERRM || '\n';
        END;
    END FOR;
    CLOSE table_cursor;

    result_msg := result_msg || 'Total tables added to share: ' || table_count || '\n';

    -- -------------------------------------------------------------------------
    -- STEP 5: Add target account to share
    -- -------------------------------------------------------------------------
    sql_cmd := 'ALTER SHARE ' || :SHARE_NAME ||
               ' ADD ACCOUNTS = ' || :TARGET_ACCOUNT;
    EXECUTE IMMEDIATE :sql_cmd;
    result_msg := result_msg || 'Target account added: ' || :TARGET_ACCOUNT || '\n';

    -- -------------------------------------------------------------------------
    -- STEP 6: Return summary
    -- -------------------------------------------------------------------------
    result_msg := result_msg || '\n=== MIGRATION SHARE PREPARATION COMPLETE ===\n';
    result_msg := result_msg || 'Share Name: ' || :SHARE_NAME || '\n';
    result_msg := result_msg || 'Tables Shared: ' || table_count || '\n';
    result_msg := result_msg || 'Target Account: ' || :TARGET_ACCOUNT || '\n';

    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM || '\n' || result_msg;
END;
$$;

-- ============================================================================
-- AUTOMATED DDL EXTRACTION PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE PROD_DB.PUBLIC.SP_EXTRACT_ALL_DDL(
    SCHEMAS_TO_EXTRACT VARCHAR,          -- Comma-separated list: 'SCHEMA1,SCHEMA2'
    OBJECT_TYPES VARCHAR                 -- Comma-separated list: 'TABLE,VIEW,PROCEDURE'
)
RETURNS TABLE (
    object_schema VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    ddl_statement VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        -- Extract Tables
        SELECT
            table_schema AS object_schema,
            table_name AS object_name,
            'TABLE' AS object_type,
            GET_DDL('TABLE', 'PROD_DB.' || table_schema || '.' || table_name, TRUE) AS ddl_statement
        FROM PROD_DB.INFORMATION_SCHEMA.TABLES
        WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_EXTRACT, ',')))
          AND table_type = 'BASE TABLE'
          AND :OBJECT_TYPES ILIKE '%TABLE%'

        UNION ALL

        -- Extract Views
        SELECT
            table_schema AS object_schema,
            table_name AS object_name,
            'VIEW' AS object_type,
            GET_DDL('VIEW', 'PROD_DB.' || table_schema || '.' || table_name, TRUE) AS ddl_statement
        FROM PROD_DB.INFORMATION_SCHEMA.VIEWS
        WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_EXTRACT, ',')))
          AND :OBJECT_TYPES ILIKE '%VIEW%'

        UNION ALL

        -- Extract Procedures
        SELECT
            procedure_schema AS object_schema,
            procedure_name AS object_name,
            'PROCEDURE' AS object_type,
            GET_DDL('PROCEDURE',
                    'PROD_DB.' || procedure_schema || '.' || procedure_name ||
                    '(' || COALESCE(argument_signature, '') || ')',
                    TRUE) AS ddl_statement
        FROM PROD_DB.INFORMATION_SCHEMA.PROCEDURES
        WHERE procedure_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_EXTRACT, ',')))
          AND :OBJECT_TYPES ILIKE '%PROCEDURE%'

        ORDER BY object_type, object_schema, object_name
    );

    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- AUTOMATED DEPENDENCY DISCOVERY PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE PROD_DB.PUBLIC.SP_DISCOVER_DEPENDENCIES(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMAS VARCHAR,              -- Comma-separated
    BASE_OBJECTS VARCHAR,                -- Comma-separated
    MAX_DEPTH INTEGER                    -- Maximum recursion depth
)
RETURNS TABLE (
    dependency_type VARCHAR,
    database_name VARCHAR,
    schema_name VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    dependency_level INTEGER
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH RECURSIVE
        split_schemas AS (
            SELECT TRIM(VALUE) AS schema_name
            FROM TABLE(SPLIT_TO_TABLE(:SOURCE_SCHEMAS, ','))
        ),
        split_objects AS (
            SELECT TRIM(VALUE) AS object_name
            FROM TABLE(SPLIT_TO_TABLE(:BASE_OBJECTS, ','))
        ),
        -- Upstream dependencies (what base objects depend ON)
        upstream_deps AS (
            SELECT DISTINCT
                od.referenced_database_name AS database_name,
                od.referenced_schema_name AS schema_name,
                od.referenced_object_name AS object_name,
                od.referenced_object_domain AS object_type,
                1 AS dependency_level
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            WHERE od.referencing_database_name = :SOURCE_DATABASE
              AND od.referencing_schema_name IN (SELECT schema_name FROM split_schemas)
              AND od.referencing_object_name IN (SELECT object_name FROM split_objects)
              AND od.referenced_database_name = :SOURCE_DATABASE

            UNION ALL

            SELECT DISTINCT
                od.referenced_database_name,
                od.referenced_schema_name,
                od.referenced_object_name,
                od.referenced_object_domain,
                ud.dependency_level + 1
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            INNER JOIN upstream_deps ud
                ON od.referencing_database_name = ud.database_name
                AND od.referencing_schema_name = ud.schema_name
                AND od.referencing_object_name = ud.object_name
            WHERE ud.dependency_level < :MAX_DEPTH
              AND od.referenced_database_name = :SOURCE_DATABASE
        ),
        -- Downstream dependencies (what depends ON base objects)
        downstream_deps AS (
            SELECT DISTINCT
                od.referencing_database_name AS database_name,
                od.referencing_schema_name AS schema_name,
                od.referencing_object_name AS object_name,
                od.referencing_object_domain AS object_type,
                1 AS dependency_level
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            WHERE od.referenced_database_name = :SOURCE_DATABASE
              AND od.referenced_schema_name IN (SELECT schema_name FROM split_schemas)
              AND od.referenced_object_name IN (SELECT object_name FROM split_objects)
              AND od.referencing_database_name = :SOURCE_DATABASE

            UNION ALL

            SELECT DISTINCT
                od.referencing_database_name,
                od.referencing_schema_name,
                od.referencing_object_name,
                od.referencing_object_domain,
                dd.dependency_level + 1
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            INNER JOIN downstream_deps dd
                ON od.referenced_database_name = dd.database_name
                AND od.referenced_schema_name = dd.schema_name
                AND od.referenced_object_name = dd.object_name
            WHERE dd.dependency_level < :MAX_DEPTH
              AND od.referencing_database_name = :SOURCE_DATABASE
        )
        SELECT
            'UPSTREAM' AS dependency_type,
            database_name,
            schema_name,
            object_name,
            object_type,
            dependency_level
        FROM upstream_deps

        UNION ALL

        SELECT
            'DOWNSTREAM' AS dependency_type,
            database_name,
            schema_name,
            object_name,
            object_type,
            dependency_level
        FROM downstream_deps

        UNION ALL

        -- Base objects
        SELECT
            'BASE' AS dependency_type,
            :SOURCE_DATABASE AS database_name,
            ss.schema_name,
            so.object_name,
            'OBJECT' AS object_type,
            0 AS dependency_level
        FROM split_schemas ss
        CROSS JOIN split_objects so

        ORDER BY dependency_type, dependency_level, schema_name, object_name
    );

    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- Example 1: Prepare migration share
CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS',
    'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'nfmyizv.imsdlc'
);

-- Example 2: Extract all DDL
CALL PROD_DB.PUBLIC.SP_EXTRACT_ALL_DDL(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'TABLE,VIEW,PROCEDURE'
);

-- Example 3: Discover dependencies
CALL PROD_DB.PUBLIC.SP_DISCOVER_DEPENDENCIES(
    'PROD_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS,VW_CURRENT_HOLDINGS',
    10
);
*/
