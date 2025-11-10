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
    sql_cmd VARCHAR;
    table_rs RESULTSET;

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
    LET schema_rs RESULTSET := (SELECT TRIM(VALUE) AS schema_name
                                 FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_MIGRATE, ',')));
    FOR schema_rec IN schema_rs DO
        sql_cmd := 'GRANT USAGE ON SCHEMA PROD_DB.' || schema_rec.schema_name ||
                   ' TO SHARE ' || :SHARE_NAME;
        EXECUTE IMMEDIATE :sql_cmd;
        result_msg := result_msg || 'Granted schema usage: ' || schema_rec.schema_name || '\n';
    END FOR;

    -- -------------------------------------------------------------------------
    -- STEP 4: Grant SELECT on all discovered tables
    -- -------------------------------------------------------------------------
    table_count := 0;

    -- Build resultset for tables to share
    table_rs := (
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
                od.REFERENCED_DATABASE,
                od.REFERENCED_SCHEMA,
                od.REFERENCED_OBJECT_NAME,
                od.REFERENCED_OBJECT_DOMAIN
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            WHERE od.REFERENCING_DATABASE = 'PROD_DB'
              AND od.REFERENCING_SCHEMA IN (SELECT schema_name FROM split_schemas)
              AND od.REFERENCING_OBJECT_NAME IN (SELECT object_name FROM split_objects)
              AND od.REFERENCED_OBJECT_DOMAIN = 'TABLE'
              AND od.REFERENCED_DATABASE = 'PROD_DB'
              AND od.REFERENCED_SCHEMA IN (SELECT schema_name FROM split_schemas)

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
            REFERENCED_DATABASE AS db_name,
            REFERENCED_SCHEMA AS schema_name,
            REFERENCED_OBJECT_NAME AS table_name
        FROM all_dependencies
        WHERE REFERENCED_OBJECT_DOMAIN = 'TABLE'
    );

    FOR table_rec IN table_rs DO
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
    query_string VARCHAR;
    res RESULTSET;
    table_query VARCHAR;
    view_query VARCHAR;
    proc_list RESULTSET;
    proc_query VARCHAR DEFAULT '';
    ddl_text VARCHAR;
BEGIN
    -- Build dynamic query for tables
    IF (:OBJECT_TYPES ILIKE '%TABLE%') THEN
        SELECT LISTAGG(
            'SELECT ''' || table_schema || ''' AS object_schema, ' ||
            '''' || table_name || ''' AS object_name, ' ||
            '''TABLE'' AS object_type, ' ||
            'GET_DDL(''TABLE'', ''PROD_DB.' || table_schema || '.' || table_name || ''', TRUE) AS ddl_statement',
            ' UNION ALL ')
        INTO :table_query
        FROM PROD_DB.INFORMATION_SCHEMA.TABLES
        WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_EXTRACT, ',')))
          AND table_type = 'BASE TABLE';
    ELSE
        table_query := '';
    END IF;

    -- Build dynamic query for views
    IF (:OBJECT_TYPES ILIKE '%VIEW%') THEN
        SELECT LISTAGG(
            'SELECT ''' || table_schema || ''' AS object_schema, ' ||
            '''' || table_name || ''' AS object_name, ' ||
            '''VIEW'' AS object_type, ' ||
            'GET_DDL(''VIEW'', ''PROD_DB.' || table_schema || '.' || table_name || ''', TRUE) AS ddl_statement',
            ' UNION ALL ')
        INTO :view_query
        FROM PROD_DB.INFORMATION_SCHEMA.VIEWS
        WHERE table_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_EXTRACT, ',')));
    ELSE
        view_query := '';
    END IF;

    -- Build dynamic query for procedures with error handling
    IF (:OBJECT_TYPES ILIKE '%PROCEDURE%') THEN
        proc_list := (
            SELECT
                procedure_schema,
                procedure_name,
                COALESCE(argument_signature, '()') AS arg_sig
            FROM PROD_DB.INFORMATION_SCHEMA.PROCEDURES
            WHERE procedure_schema IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:SCHEMAS_TO_EXTRACT, ',')))
              AND procedure_name NOT LIKE '%$%'  -- Exclude system procedures
        );

        FOR proc_rec IN proc_list DO
            BEGIN
                -- Try to get DDL for this procedure
                LET full_proc_name VARCHAR := 'PROD_DB.' || proc_rec.procedure_schema || '.' || proc_rec.procedure_name || proc_rec.arg_sig;
                ddl_text := GET_DDL('PROCEDURE', full_proc_name, TRUE);

                -- Build SELECT for this procedure
                IF (proc_query != '') THEN
                    proc_query := proc_query || ' UNION ALL ';
                END IF;
                proc_query := proc_query ||
                    'SELECT ''' || proc_rec.procedure_schema || ''' AS object_schema, ' ||
                    '''' || proc_rec.procedure_name || ''' AS object_name, ' ||
                    '''PROCEDURE'' AS object_type, ' ||
                    '''' || REPLACE(ddl_text, '''', '''''') || ''' AS ddl_statement';
            EXCEPTION
                WHEN OTHER THEN
                    -- Skip procedures that can't be accessed
                    CONTINUE;
            END;
        END FOR;
    END IF;

    -- Combine queries
    query_string := '';
    IF (table_query IS NOT NULL AND table_query != '') THEN
        query_string := table_query;
    END IF;
    IF (view_query IS NOT NULL AND view_query != '') THEN
        IF (query_string != '') THEN
            query_string := query_string || ' UNION ALL ' || view_query;
        ELSE
            query_string := view_query;
        END IF;
    END IF;
    IF (proc_query IS NOT NULL AND proc_query != '') THEN
        IF (query_string != '') THEN
            query_string := query_string || ' UNION ALL ' || proc_query;
        ELSE
            query_string := proc_query;
        END IF;
    END IF;

    -- Execute dynamic query
    IF (query_string != '') THEN
        query_string := query_string || ' ORDER BY object_type, object_schema, object_name';
        res := (EXECUTE IMMEDIATE :query_string);
    ELSE
        res := (SELECT NULL AS object_schema, NULL AS object_name, NULL AS object_type, NULL AS ddl_statement WHERE 1=0);
    END IF;

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
                od.REFERENCED_DATABASE AS database_name,
                od.REFERENCED_SCHEMA AS schema_name,
                od.REFERENCED_OBJECT_NAME AS object_name,
                od.REFERENCED_OBJECT_DOMAIN AS object_type,
                1 AS dependency_level
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            WHERE od.REFERENCING_DATABASE = :SOURCE_DATABASE
              AND od.REFERENCING_SCHEMA IN (SELECT schema_name FROM split_schemas)
              AND od.REFERENCING_OBJECT_NAME IN (SELECT object_name FROM split_objects)
              AND od.REFERENCED_DATABASE = :SOURCE_DATABASE

            UNION ALL

            SELECT
                od.REFERENCED_DATABASE,
                od.REFERENCED_SCHEMA,
                od.REFERENCED_OBJECT_NAME,
                od.REFERENCED_OBJECT_DOMAIN,
                ud.dependency_level + 1
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            INNER JOIN upstream_deps ud
                ON od.REFERENCING_DATABASE = ud.database_name
                AND od.REFERENCING_SCHEMA = ud.schema_name
                AND od.REFERENCING_OBJECT_NAME = ud.object_name
            WHERE ud.dependency_level < :MAX_DEPTH
              AND od.REFERENCED_DATABASE = :SOURCE_DATABASE
        ),
        -- Downstream dependencies (what depends ON base objects)
        downstream_deps AS (
            SELECT DISTINCT
                od.REFERENCING_DATABASE AS database_name,
                od.REFERENCING_SCHEMA AS schema_name,
                od.REFERENCING_OBJECT_NAME AS object_name,
                od.REFERENCING_OBJECT_DOMAIN AS object_type,
                1 AS dependency_level
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            WHERE od.REFERENCED_DATABASE = :SOURCE_DATABASE
              AND od.REFERENCED_SCHEMA IN (SELECT schema_name FROM split_schemas)
              AND od.REFERENCED_OBJECT_NAME IN (SELECT object_name FROM split_objects)
              AND od.REFERENCING_DATABASE = :SOURCE_DATABASE

            UNION ALL

            SELECT
                od.REFERENCING_DATABASE,
                od.REFERENCING_SCHEMA,
                od.REFERENCING_OBJECT_NAME,
                od.REFERENCING_OBJECT_DOMAIN,
                dd.dependency_level + 1
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
            INNER JOIN downstream_deps dd
                ON od.REFERENCED_DATABASE = dd.database_name
                AND od.REFERENCED_SCHEMA = dd.schema_name
                AND od.REFERENCED_OBJECT_NAME = dd.object_name
            WHERE dd.dependency_level < :MAX_DEPTH
              AND od.REFERENCING_DATABASE = :SOURCE_DATABASE
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

